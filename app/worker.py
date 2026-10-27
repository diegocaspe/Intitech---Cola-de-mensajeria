import logging
import threading
import time

from app import config
from app.application.order_service import OrderService
from app.infrastructure.control import ControlStore
from app.infrastructure.messaging.rabbitmq import OrderConsumer, RabbitOrderPublisher
from app.infrastructure.persistence.postgres import PostgresOrderRepository, PostgresOutbox, create_pool, init_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s")
logging.getLogger("psycopg.pool").setLevel(logging.ERROR)
log = logging.getLogger("worker")


def main() -> None:
    pool = create_pool(config.DATABASE_URL, max_size=config.MAX_WORKERS + 2)
    init_schema(pool)
    control = ControlStore()
    service = OrderService(PostgresOrderRepository(pool), RabbitOrderPublisher(), PostgresOutbox(pool))
    consumer = OrderConsumer(service, control)
    running: list[tuple[threading.Thread, threading.Event]] = []

    # Supervisor: mantiene tantos consumidores como pida el panel (escalado horizontal en caliente)
    while True:
        running = [(t, e) for t, e in running if t.is_alive()]
        desired = max(0, min(config.MAX_WORKERS, int(control.get()["workers"])))
        while len(running) < desired:
            stop = threading.Event()
            t = threading.Thread(target=consumer.run, args=(stop,), name=f"worker-{len(running) + 1}", daemon=True)
            t.start()
            running.append((t, stop))
            log.info("Worker iniciado (%d activos)", len(running))
        while len(running) > desired:
            t, stop = running.pop()
            stop.set()
            log.info("Worker detenido (%d activos)", len(running))
        time.sleep(1)


if __name__ == "__main__":
    main()
