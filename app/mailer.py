"""Mailer: proceso aparte que consume la cola "correos" y envía cada correo por Gmail (SMTP).

Nadie más envía correos: la API y el worker solo dejan eventos en el outbox y el relay los publica. Si el mailer cae,
los correos esperan en la cola (en disco) y salen al volver; un correo que ya salió no se repite (sent_emails)."""
import logging
import threading
import time

from app import config
from app.database.connection import create_pool, init_schema
from app.messaging.email_senders import LogEmailSender, SmtpEmailSender
from app.messaging.rabbitmq import EmailConsumer
from app.repositories.catalog_repository import InMemoryProductRepository
from app.repositories.postgres_repository import PostgresSentEmails
from app.services.notification_service import NotificationService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [mailer] %(message)s")
logging.getLogger("psycopg.pool").setLevel(logging.ERROR)
logging.getLogger("pika").setLevel(logging.WARNING)   # pika registra cada intento de conexión: ruido en el log
log = logging.getLogger("mailer")


def main() -> None:
    config.require_env("mailer")
    pool = create_pool(config.DATABASE_URL, max_size=3)
    while True:
        try:
            init_schema(pool)
            break
        except Exception as exc:                          # BD aún no disponible (p. ej. se levantan a la vez)
            log.warning("Esperando la base de datos: %s", exc.__class__.__name__)
            time.sleep(1)

    if config.EMAIL_ENABLED:
        sender = SmtpEmailSender(config.GMAIL_USER, config.GMAIL_APP_PASSWORD, config.STORE_NAME,
                                 config.SMTP_HOST, config.SMTP_PORT)
        log.info("Envío real activado por %s:%d", config.SMTP_HOST, config.SMTP_PORT)
    else:
        sender = LogEmailSender()
        log.info("EMAIL_ENABLED=false: los correos solo se registran en este log")

    products = InMemoryProductRepository()
    def product_name(sku: str) -> str:
        p = products.get(sku)
        return p.name if p else sku

    base_url = config.PUBLIC_URL or "http://localhost:8000"
    service = NotificationService(PostgresSentEmails(pool), sender, config.STORE_NAME, base_url, product_name)
    EmailConsumer(service).run(threading.Event())     # bucle infinito con reconexión automática


if __name__ == "__main__":
    main()
