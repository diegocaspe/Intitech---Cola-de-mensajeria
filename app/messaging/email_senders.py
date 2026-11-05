"""Implementaciones de EmailSender: Gmail por SMTP, solo log (EMAIL_ENABLED=false) y falsa (pruebas)."""
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr, make_msgid

from app.repositories.interfaces import EmailSender, EmailSendError

log = logging.getLogger(__name__)


class SmtpEmailSender(EmailSender):
    """Envía por SMTP con TLS desde el primer byte (SMTP_SSL, puerto 465), con la contraseña de aplicación de Gmail.
    Una conexión por correo: el volumen es bajo y así una conexión rota nunca afecta al siguiente envío."""

    def __init__(self, user: str, password: str, from_name: str, host: str = "smtp.gmail.com", port: int = 465,
                 timeout: float = 20):
        self.user = user
        self._password = password   # nunca se escribe en logs ni en mensajes de error
        self.from_name = from_name
        self.host, self.port, self.timeout = host, port, timeout

    def send(self, to: str, subject: str, html: str, text: str) -> None:
        msg = EmailMessage()
        msg["From"] = formataddr((self.from_name, self.user))
        msg["To"] = to
        msg["Subject"] = subject
        msg["Message-ID"] = make_msgid(domain=self.user.split("@")[-1])
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")
        try:
            with smtplib.SMTP_SSL(self.host, self.port, timeout=self.timeout,
                                  context=ssl.create_default_context()) as smtp:
                smtp.login(self.user, self._password)
                smtp.send_message(msg)
        except smtplib.SMTPAuthenticationError as exc:
            # solo el código: la respuesta del servidor no se reenvía tal cual por si incluyera datos de la cuenta
            raise EmailSendError(f"Gmail rechazó el usuario o la contraseña de aplicación (SMTP {exc.smtp_code})") from None
        except smtplib.SMTPRecipientsRefused:
            raise EmailSendError("El servidor rechazó la dirección de destino") from None
        except (smtplib.SMTPException, OSError) as exc:
            raise EmailSendError(f"Fallo de SMTP o de red: {exc.__class__.__name__}") from None


class LogEmailSender(EmailSender):
    """EMAIL_ENABLED=false: no envía nada, solo deja constancia en el log (sin el contenido: lleva enlaces con token)."""

    def send(self, to: str, subject: str, html: str, text: str) -> None:
        log.info("Correo simulado a %s: %s", to, subject)


class FakeEmailSender(EmailSender):
    """Para las pruebas: guarda los correos en memoria y puede fallar a propósito las primeras `fail_times` veces."""

    def __init__(self, fail_times: int = 0):
        self.sent: list[dict] = []
        self.fail_times = fail_times

    def send(self, to: str, subject: str, html: str, text: str) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise EmailSendError("SMTP no disponible (simulado)")
        self.sent.append({"to": to, "subject": subject, "html": html, "text": text})
