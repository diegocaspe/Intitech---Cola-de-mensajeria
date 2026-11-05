"""Relay del Transactional Outbox: proceso aparte que lee los eventos pendientes y los publica en RabbitMQ.

Es el único camino de un pedido hacia la cola: la API solo escribe en la base de datos. Cada evento se marca como
publicado recién cuando el broker lo confirma (publisher confirms); si algo falla, sigue pendiente y se reintenta."""
import logging
import time

from app import config
from app.database.connection import create_pool, init_schema
from app.messaging.rabbitmq import RabbitOrderPublisher
from app.repositories.postgres_repository import PostgresOrderRepository, PostgresOutbox
from app.services.order_service import OrderService
from app.utils.control import ControlStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [relay] %(message)s")
logging.getLogger("psycopg.pool").setLevel(logging.ERROR)
log = logging.getLogger("relay")

BATCH = 500


def main() -> None:
    config.require_env("relay")
    pool = create_pool(config.DATABASE_URL, max_size=3)   # una conexión para el lote y otra para marcar cada pedido
    while True:
        try:
            init_schema(pool)
            break
        except Exception as exc:                          # BD aún no disponible (p. ej. se levantan a la vez)
            log.warning("Esperando la base de datos: %s", exc.__class__.__name__)
            time.sleep(1)
    control = ControlStore()
    publisher = RabbitOrderPublisher(persistent_mode=lambda: control.get()["persistent"])
    service = OrderService(PostgresOrderRepository(pool), publisher, PostgresOutbox(pool))
    log.info("Revisando el outbox cada %.2f s", config.RELAY_POLL_S)
    while True:
        sent = 0
        try:
            sent = service.relay_outbox(limit=BATCH)
            if sent:
                log.info("%d eventos publicados en RabbitMQ", sent)
        except Exception as exc:                          # BD caída: se reintenta en la siguiente vuelta
            log.warning("No se pudo revisar el outbox: %s", exc.__class__.__name__)
        if sent < BATCH:                                  # lote lleno: hay más pendientes, sigue sin esperar
            time.sleep(config.RELAY_POLL_S)


if __name__ == "__main__":
    main()
