"""Entrega de correos: lo usa el proceso mailer al consumir la cola "correos". Nunca lo llaman routes ni otros services:
ellos solo dejan un evento en el outbox."""
import logging
from typing import Callable

from app.repositories.interfaces import EmailSender, EmailSendError, SentEmailRepository
from app.models.email import EmailEvent
from app.services import email_templates

log = logging.getLogger(__name__)


class NotificationService:
    def __init__(self, sent: SentEmailRepository, sender: EmailSender, store_name: str, base_url: str,
                 product_name: Callable[[str], str] = lambda sku: sku):
        self.sent = sent
        self.sender = sender
        self.store_name = store_name
        self.base_url = base_url.rstrip("/")
        self.product_name = product_name

    def deliver(self, payload: dict) -> bool:
        """Envía el correo del evento. Devuelve False si ya se había enviado (mensaje duplicado).

        Idempotencia: RabbitMQ entrega "al menos una vez", así que el mismo evento puede llegar dos veces; el email_id
        queda registrado al enviarlo y un duplicado se ignora. Lanza EmailSendError si SMTP o la red fallan (el
        consumidor lo reintenta con espera creciente) y ValueError/KeyError si el evento es ilegible (va a la DLQ)."""
        event = EmailEvent.from_payload(payload)
        if self.sent.was_sent(event.email_id):
            log.info("Correo %s ya enviado: se ignora el duplicado", event.email_id[:8])
            return False
        subject, html, text = email_templates.render(event, self.store_name, self.base_url, self.product_name)
        self.sender.send(event.to, subject, html, text)   # EmailSendError -> reintento
        # Si el proceso muriera justo aquí (enviado pero sin registrar), RabbitMQ lo reentregaría y llegaría dos
        # veces: es el límite de "al menos una vez" con un servidor SMTP externo, que no participa de la transacción
        self.sent.mark_sent(event.email_id, event.kind.value, event.to)
        log.info("Correo %s (%s) enviado", event.email_id[:8], event.kind.value)
        return True


__all__ = ["NotificationService", "EmailSendError"]
