"""Pago con tarjeta SIMULADO. No hay pasarela real: FakePaymentProvider hace de proveedor de pagos dentro del código.

Seguridad (aunque sea una demo, se trata como si fuera real):
- Solo se aceptan las 3 tarjetas de prueba de TEST_CARDS. Cualquier otro número se rechaza en la API (400), así nadie
  del público escribe su tarjeta real.
- tokenize() es el ÚNICO lugar que ve el número completo y el CVV. Devuelve un token de prueba, la marca y los últimos 4
  dígitos: eso es lo único que se guarda en orders. El número y el CVV nunca se guardan, nunca se escriben en los logs
  y nunca viajan en el mensaje de RabbitMQ (que sigue llevando solo el order_id).
- Los mensajes de error nunca incluyen lo que escribió el cliente."""
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import date
from typing import Callable

from app.models.order import PaymentStatus

TOKEN_APPROVED = "tok_test_approved"
TOKEN_DECLINED = "tok_test_declined"
TOKEN_FLAKY = "tok_test_flaky"

# número de la tarjeta de prueba → token. Las tres empiezan con 4: Visa
TEST_CARDS = {
    "4111111111111111": TOKEN_APPROVED,   # aprobada
    "4000000000000002": TOKEN_DECLINED,   # rechazada: fondos insuficientes
    "4000000000000119": TOKEN_FLAKY,      # error temporal del proveedor: falla el 1.er intento, se aprueba en el reintento
}
DECLINE_REASON = "Fondos insuficientes"


class CardError(ValueError):
    """Datos de tarjeta inválidos. El mensaje es apto para mostrarlo al cliente: nunca repite lo que escribió."""


@dataclass(frozen=True)
class CardToken:
    token: str
    brand: str
    last4: str


def tokenize(card: dict | None, today: date | None = None) -> CardToken:
    """Valida la tarjeta y la cambia por un token de prueba. Lanza CardError (sin repetir los datos)."""
    if not isinstance(card, dict):
        raise CardError("Faltan los datos de la tarjeta")
    holder = str(card.get("holder") or "").strip()
    number = re.sub(r"[\s-]", "", str(card.get("number") or ""))
    exp = str(card.get("exp") or "").strip()
    cvv = str(card.get("cvv") or "").strip()
    if not 2 <= len(holder) <= 60:
        raise CardError("Escribe el nombre del titular")
    if number not in TEST_CARDS:
        raise CardError("Usa una tarjeta de prueba")
    m = re.fullmatch(r"(\d{2})\s*/\s*(\d{2})", exp)
    if not m or not 1 <= int(m.group(1)) <= 12:
        raise CardError("Vencimiento inválido: usa MM/AA")
    today = today or date.today()
    month, year = int(m.group(1)), 2000 + int(m.group(2))
    if (year, month) < (today.year, today.month):   # vale hasta el último día de su mes
        raise CardError("La tarjeta está vencida")
    if not re.fullmatch(r"\d{3}", cvv):
        raise CardError("El CVV debe tener 3 dígitos")
    return CardToken(TEST_CARDS[number], "Visa", number[-4:])


class PaymentUnavailable(Exception):
    """Error temporal del proveedor (timeout, 5xx...): el worker lo reintenta con espera creciente."""


@dataclass(frozen=True)
class ChargeResult:
    status: PaymentStatus           # APPROVED o DECLINED
    reason: str | None = None


class FakePaymentProvider:
    """Proveedor de pagos falso con clave de idempotencia, como las pasarelas reales: si el mismo order_id se cobra dos
    veces (p. ej. dos workers con el mismo mensaje duplicado a la vez), el segundo recibe el resultado del primero y no
    se cobra de nuevo. Los errores temporales no se recuerdan: el reintento sí vuelve a intentar el cobro.

    La memoria del proveedor vive en el proceso del worker; la otra mitad de la idempotencia está en la BD
    (payment_status = APPROVED: el worker ni siquiera llama al proveedor)."""

    def __init__(self, latency: Callable[[], float] | None = None, sleep: Callable[[float], None] = time.sleep):
        self._latency = latency or (lambda: random.uniform(0.3, 0.8))
        self._sleep = sleep
        self._results: dict[str, ChargeResult] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self.charges: list[str] = []   # order_id de cada cobro aprobado de verdad (para las pruebas y el log)

    def _lock_for(self, key: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def charge(self, idempotency_key: str, token: str, attempt: int, latency_s: float | None = None) -> ChargeResult:
        """attempt: número de intento de cobro de este pedido (1 = el primero). Lanza PaymentUnavailable."""
        with self._lock_for(idempotency_key):
            if idempotency_key in self._results:
                return self._results[idempotency_key]
            self._sleep(self._latency() if latency_s is None else latency_s)   # la "llamada" al proveedor
            if token == TOKEN_FLAKY and attempt <= 1:
                raise PaymentUnavailable("Error temporal del proveedor de pagos")
            if token == TOKEN_DECLINED:
                result = ChargeResult(PaymentStatus.DECLINED, DECLINE_REASON)
            elif token in (TOKEN_APPROVED, TOKEN_FLAKY):
                result = ChargeResult(PaymentStatus.APPROVED)
                self.charges.append(idempotency_key)
            else:
                result = ChargeResult(PaymentStatus.DECLINED, "Token de pago desconocido")
            self._results[idempotency_key] = result
            return result
