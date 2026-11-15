"""Plantillas de los correos: asunto, HTML y texto plano (para clientes de correo que no muestran HTML).

HTML de correo: tablas y estilos en línea (los clientes de correo ignoran casi todo el CSS moderno). Fredoka y Nunito
Sans se piden a Google Fonts; Gmail no carga fuentes web y usa el respaldo del sistema, así que el diseño no depende
de ellas. Todo dato del usuario pasa por escape() antes de entrar al HTML."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html import escape
from typing import Callable

from app.models.email import EmailEvent, EmailKind

# Perú no tiene horario de verano: UTC-5 todo el año (evita depender de la base de zonas horarias del sistema)
LIMA = timezone(timedelta(hours=-5), "America/Lima")

BLUE, YELLOW, NIGHT, CLOUD, MUTED, LINE = "#2357F5", "#FFC93C", "#0E1525", "#F4F6FB", "#68728A", "#E9EDF5"
DISPLAY = "'Fredoka', ui-rounded, 'Trebuchet MS', Arial, sans-serif"
BODY = "'Nunito Sans', 'Segoe UI', Arial, sans-serif"


# estado del pago con tarjeta (simulado) tal como se muestra en el comprobante
PAYMENT_LABELS = {"APPROVED": "Pago aprobado", "DECLINED": "Pago rechazado", "REFUNDED": "Pago reembolsado",
                  "PENDING": "Pago pendiente"}


def lima_time(iso: str) -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LIMA).strftime("%d/%m/%Y, %H:%M") + " (hora de Lima)"


def soles(value) -> str:
    return f"S/ {Decimal(str(value)):,.2f}"


def _button(url: str, label: str) -> str:
    return (f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:28px 0 8px"><tr>'
            f'<td style="background:{BLUE};border-radius:12px">'
            f'<a href="{escape(url)}" style="display:inline-block;padding:14px 28px;font-family:{DISPLAY};font-size:17px;'
            f'font-weight:600;color:#ffffff;text-decoration:none">{escape(label)}</a></td></tr></table>'
            f'<p style="margin:0;font-size:13px;color:{MUTED}">Si el botón no funciona, copia este enlace en tu navegador:'
            f'<br><a href="{escape(url)}" style="color:{BLUE};word-break:break-all">{escape(url)}</a></p>')


def _layout(store: str, preheader: str, title: str, body_html: str, footer_note: str = "") -> str:
    note = f'<p style="margin:0 0 8px">{footer_note}</p>' if footer_note else ""
    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><title>{escape(title)}</title>
<link href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;600&family=Nunito+Sans:wght@400;700&display=swap" rel="stylesheet">
</head>
<body style="margin:0;padding:0;background:{CLOUD}">
<div style="display:none;max-height:0;overflow:hidden;opacity:0">{escape(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{CLOUD}"><tr><td align="center" style="padding:32px 16px">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px">
    <tr><td style="padding:0 8px 16px;font-family:{DISPLAY};font-size:26px;font-weight:600;color:{BLUE}">
      {escape(store)}<span style="color:{YELLOW}">.</span></td></tr>
    <tr><td style="background:#ffffff;border-radius:20px;padding:36px 32px;border:1px solid {LINE};
                   font-family:{BODY};font-size:16px;line-height:1.6;color:{NIGHT}">
      <h1 style="margin:0 0 16px;font-family:{DISPLAY};font-size:26px;line-height:1.2;font-weight:600;color:{NIGHT}">{escape(title)}</h1>
      {body_html}
    </td></tr>
    <tr><td style="padding:20px 8px 0;font-family:{BODY};font-size:12px;line-height:1.5;color:{MUTED}">
      {note}<p style="margin:0">Este es un correo automático de {escape(store)}. No respondas a este mensaje.</p>
    </td></tr>
  </table>
</td></tr></table>
</body></html>"""


def _p(text: str) -> str:
    return f'<p style="margin:0 0 14px">{text}</p>'


def render(event: EmailEvent, store: str, base_url: str,
           product_name: Callable[[str], str] = lambda sku: sku) -> tuple[str, str, str]:
    """Devuelve (asunto, html, texto)."""
    name = escape(event.name.split()[0] if event.name.strip() else "")
    hi = f"Hola, {name}" if name else "Hola"
    data = event.data

    if event.kind == EmailKind.REGISTRO:
        url = f"{base_url}/api/auth/verify?token={data['token']}"
        hours = data.get("expires_hours", 24)
        subject = f"Confirma tu correo en {store}" if data.get("resend") else f"¡Bienvenido a {store}! Confirma tu correo"
        body = (_p(f"{hi}. Tu cuenta en <b>{escape(store)}</b> ya está lista.")
                + _p("Confirma que este correo es tuyo para recibir tus comprobantes y avisos de seguridad.")
                + _button(url, "Verificar mi correo")
                + f'<p style="margin:20px 0 0;font-size:14px;color:{MUTED}">El enlace vence en {hours} horas y solo se puede usar una vez.</p>')
        text = (f"{hi}. Tu cuenta en {store} ya está lista.\n\nConfirma tu correo abriendo este enlace "
                f"(vence en {hours} horas, un solo uso):\n{url}\n")
        return subject, _layout(store, "Confirma tu correo para activar tu cuenta", "Te damos la bienvenida", body,
                                "Si no creaste esta cuenta, ignora este correo."), text

    if event.kind == EmailKind.LOGIN:
        when = lima_time(data["at"])
        subject = f"Nuevo inicio de sesión en {store}"
        body = (_p(f"{hi}. Se inició sesión en tu cuenta de <b>{escape(store)}</b>.")
                + f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:8px 0 20px;background:{CLOUD};border-radius:12px;width:100%"><tr>'
                  f'<td style="padding:16px 20px;font-size:15px"><span style="color:{MUTED}">Fecha y hora</span><br><b>{escape(when)}</b></td></tr></table>'
                + _p("Si fuiste tú, no tienes que hacer nada.")
                + _p(f'Si <b>no</b> fuiste tú, cambia tu contraseña ahora desde "¿Olvidaste tu contraseña?" en '
                     f'<a href="{escape(base_url)}/login" style="color:{BLUE}">{escape(base_url)}/login</a>.'))
        text = (f"{hi}. Se inició sesión en tu cuenta de {store} el {when}.\n\nSi no fuiste tú, cambia tu contraseña "
                f"desde \"¿Olvidaste tu contraseña?\" en {base_url}/login\n")
        return subject, _layout(store, f"Inicio de sesión el {when}", "Nuevo inicio de sesión", body), text

    if event.kind == EmailKind.RECUPERAR:
        url = f"{base_url}/login?reset={data['token']}"
        minutes = data.get("expires_minutes", 60)
        subject = f"Restablece tu contraseña de {store}"
        body = (_p(f"{hi}. Recibimos una solicitud para restablecer la contraseña de tu cuenta.")
                + _button(url, "Crear una contraseña nueva")
                + f'<p style="margin:20px 0 0;font-size:14px;color:{MUTED}">El enlace vence en {minutes} minutos y solo se puede usar una vez.</p>')
        text = (f"{hi}. Para crear una contraseña nueva abre este enlace (vence en {minutes} minutos, un solo uso):\n"
                f"{url}\n\nSi no lo pediste, ignora este correo: tu contraseña no cambia.\n")
        return subject, _layout(store, "Enlace para crear una contraseña nueva", "Restablece tu contraseña", body,
                                "Si no lo pediste, ignora este correo: tu contraseña sigue igual."), text

    if event.kind == EmailKind.COMPRA_CONFIRMADA:
        code = data["order_id"].split("-")[0].upper()
        when = lima_time(data["created_at"])
        rows, lines = "", []
        for it in data["items"]:
            qty, unit = int(it["quantity"]), Decimal(str(it["unit_price"]))
            subtotal = unit * qty
            pname = product_name(it["sku"])
            rows += (f'<tr><td style="padding:12px 0;border-bottom:1px solid {LINE}">{escape(pname)}'
                     f'<br><span style="font-size:13px;color:{MUTED}">{qty} × {soles(unit)}</span></td>'
                     f'<td align="right" style="padding:12px 0;border-bottom:1px solid {LINE};white-space:nowrap"><b>{soles(subtotal)}</b></td></tr>')
            lines.append(f"- {pname}: {qty} x {soles(unit)} = {soles(subtotal)}")
        total = soles(data["total"])
        # pago con tarjeta (simulado): solo marca y últimos 4 dígitos. Los correos anteriores a los pagos no lo traen
        card = data.get("card")
        pay = PAYMENT_LABELS.get(data.get("payment_status") or "", "")
        pay_html = (f'<p style="margin:16px 0 0;font-size:14px"><span style="color:{MUTED}">Pagado con</span> '
                    f'<b>{escape(card)}</b> · {escape(pay)}</p>') if card else ""
        pay_text = f"Pagado con: {card} · {pay}\n" if card else ""
        subject = f"Comprobante de tu compra {code} en {store}"
        body = (_p(f"{hi}. ¡Gracias por tu compra! Tu pedido fue <b>confirmado</b>.")
                + f'<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;margin:8px 0 16px;background:{CLOUD};border-radius:12px"><tr>'
                  f'<td style="padding:14px 20px;font-size:14px"><span style="color:{MUTED}">Pedido</span><br>'
                  f'<b style="font-family:{DISPLAY};font-size:18px">{code}</b></td>'
                  f'<td style="padding:14px 20px;font-size:14px" align="right"><span style="color:{MUTED}">Fecha y hora</span><br><b>{escape(when)}</b></td></tr></table>'
                + f'<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;font-size:15px">{rows}'
                  f'<tr><td style="padding:16px 0 0;font-family:{DISPLAY};font-size:18px">Total</td>'
                  f'<td align="right" style="padding:16px 0 0;font-family:{DISPLAY};font-size:22px;color:{BLUE}"><b>{total}</b></td></tr></table>'
                + pay_html
                + f'<p style="margin:24px 0 0;padding:12px 16px;background:#FFF9E6;border-radius:10px;font-size:13px;color:{NIGHT}">'
                  f'Este correo es un comprobante de compra, no una boleta electrónica.</p>')
        text = (f"{hi}. ¡Gracias por tu compra! Tu pedido fue confirmado.\n\nPedido: {code}\nFecha y hora: {when}\n\n"
                + "\n".join(lines) + f"\n\nTotal: {total}\n" + pay_text + "\n"
                "Este correo es un comprobante de compra, no una boleta electrónica.\n")
        return subject, _layout(store, f"Pedido {code} confirmado · Total {total}", "Compra confirmada", body), text

    raise ValueError(f"Tipo de correo desconocido: {event.kind}")
