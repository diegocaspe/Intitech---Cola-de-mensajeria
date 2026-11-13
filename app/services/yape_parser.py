"""Lector de las notificaciones de Yape que reenvía MacroDroid. TODO lo que depende del formato está en este archivo.

Aún no se conoce el texto exacto de la notificación, así que es tolerante. Para ajustarlo al texto real:
  1. Mira el texto crudo en el panel (sección "Pagos Yape en vivo").
  2. Si un pago recibido sale como "ignorada", agrega su frase a FRASES_PAGO_RECIBIDO.
  3. Si algo que NO es un pago sale como pago, agrega su frase a FRASES_IGNORAR (se revisan primero).
  4. Si el nombre de quien paga no sale bien, ajusta PATRONES_PAGADOR.
  5. Agrega el texto real a tests/test_yape.py (test_parser_formatos) y corre pytest.
Las frases se comparan sin tildes y en minúsculas.

Es una función pura (sin BD ni red): el worker la usa para procesar y el webhook solo para mostrar un adelanto.
"""
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# Frases de una notificación que NO es un pago recibido. Se revisan antes que las de pago.
FRASES_IGNORAR = (
    "yapeaste",            # "Yapeaste S/ 10 a ..." (pago enviado por el dueño)
    "enviaste",
    "pagaste",
    "transferiste",
    "recarga",
    "promo",               # promociones
    "sorteo",
    "descuento",
    "cashback",
    "recuerda",            # recordatorios
    "recordatorio",
    "no olvides",
    "actualiza",
    "codigo de verificacion",
)

# Frases de un pago RECIBIDO (basta una)
FRASES_PAGO_RECIBIDO = (
    "te envio un pago",    # "Yape! JUAN PEREZ te envió un pago por S/ 1.37"
    "te envio",
    "te yapeo",            # "JUAN te yapeó S/ 1.37"
    "te yapearon",
    "te pago",
    "recibiste",           # "Recibiste S/ 1.37 de JUAN"
    "has recibido",
    "pago recibido",
    "confirmacion de pago",
)

# Monto: "S/ 1.37", "S/1,37", "S/. 1.37", "S/ 1.37" con espacios raros, "S/ 5", "S/ 1,250.50"
MONTO = re.compile(r"S\s*/\s*\.?\s*(\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)(?!\d)", re.IGNORECASE)

# Nombre de quien paga (sobre el texto ya normalizado, con tildes). El grupo "n" es el nombre.
_NOMBRE = r"(?P<n>[A-Za-zÁÉÍÓÚÜÑáéíóúüñ][A-Za-zÁÉÍÓÚÜÑáéíóúüñ.' -]{1,60}?)"
PATRONES_PAGADOR = (
    re.compile(_NOMBRE + r"\s+te\s+(?:envi[oó]|yape[oó]|pag[oó])", re.IGNORECASE),   # "JUAN PEREZ te envió..."
    re.compile(r"\bde\s+" + _NOMBRE + r"(?:\s*[.,;]|\s*$|\s+(?:el|con|por)\b)", re.IGNORECASE),   # "Recibiste S/ 5 de JUAN"
)
_PREFIJOS_PAGADOR = re.compile(r"^(?:yape!?|yape:|confirmaci[oó]n de pago:?)\s*", re.IGNORECASE)

# Espacios "raros" que algunos celulares ponen en las notificaciones
_ESPACIOS = dict.fromkeys(map(ord, "        　\t\r\n"), " ")
_INVISIBLES = dict.fromkeys(map(ord, "​‌‍⁠﻿"), None)


@dataclass(frozen=True)
class LecturaYape:
    es_pago: bool
    monto: Decimal | None
    pagador: str | None
    motivo: str          # por qué se decidió así (se guarda como detalle para el panel)


def normalizar(texto: str) -> str:
    texto = (texto or "").translate(_INVISIBLES).translate(_ESPACIOS)
    return re.sub(r" {2,}", " ", unicodedata.normalize("NFC", texto)).strip()


def _sin_tildes(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn").lower()


def leer_monto(texto: str) -> Decimal | None:
    m = MONTO.search(normalizar(texto))
    if not m:
        return None
    raw = m.group(1).replace(" ", "")
    seps = [i for i, c in enumerate(raw) if c in ".,"]
    if seps and len(raw) - seps[-1] - 1 <= 2:          # el último separador con 1-2 dígitos detrás es el decimal
        entero, dec = raw[:seps[-1]], raw[seps[-1] + 1:]
        raw = re.sub(r"[.,]", "", entero) + "." + dec
    else:                                              # "1,250" o "1.250": separador de miles
        raw = re.sub(r"[.,]", "", raw)
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def leer_pagador(texto: str) -> str | None:
    limpio = normalizar(texto)
    for patron in PATRONES_PAGADOR:
        m = patron.search(limpio)
        if m:
            nombre = _PREFIJOS_PAGADOR.sub("", m.group("n").strip(" .,'-"))
            if nombre and not re.match(r"(?i)^(te|tu|un|una|yape)$", nombre):
                return nombre[:60]
    return None


def leer(titulo: str, texto: str) -> LecturaYape:
    completo = normalizar(f"{titulo or ''} {texto or ''}")
    plano = _sin_tildes(completo)
    for frase in FRASES_IGNORAR:
        if frase in plano:
            return LecturaYape(False, leer_monto(completo), None, f"No es un pago recibido (contiene «{frase.strip()}»)")
    if not any(frase in plano for frase in FRASES_PAGO_RECIBIDO):
        return LecturaYape(False, leer_monto(completo), None, "No parece un pago recibido (frase desconocida)")
    monto = leer_monto(texto) or leer_monto(completo)
    if monto is None or monto <= 0:
        return LecturaYape(False, None, None, "Parece un pago, pero no se encontró el monto «S/ ...»")
    return LecturaYape(True, monto, leer_pagador(texto) or leer_pagador(completo), "Pago recibido")
