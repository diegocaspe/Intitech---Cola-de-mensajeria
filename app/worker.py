import logging
import threading
import time

from app import config
from app.services.order_service import OrderService
from app.utils.control import ControlStore
from app.messaging.rabbitmq import OrderConsumer, PagoConsumer, RabbitOrderPublisher
from app.database.connection import create_pool, init_schema
from app.repositories.postgres_repository import PostgresOrderRepository, PostgresOutbox, PostgresUserRepository
from app.repositories.yape_postgres import PostgresYapeRepository
from app.services.yape_service import YapeService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s")
logging.getLogger("psycopg.pool").setLevel(logging.ERROR)
log = logging.getLogger("worker")


def main() -> None:
    config.require_env("worker")
    pool = create_pool(config.DATABASE_URL, max_size=config.MAX_WORKERS + 3)   # +1 para el consumidor de pagos
    try:
        init_schema(pool, attempts=2)
    except Exception:   # ConnectionError tras los intentos, o PoolTimeout del pool sin conexiones
        # revivido con PostgreSQL caído: igual consume; cada mensaje vuelve a la cola (sin gastar reintentos ni ir a la
        # DLQ) hasta que la BD regrese. Las tablas ya existen: las creó la API al arrancar
        log.warning("PostgreSQL no responde al arrancar: se consume igual y los mensajes esperan en la cola")
    control = ControlStore()
    # con el repositorio de usuarios, al confirmar el pedido de un cliente real se pide su comprobante por correo
    # poison_mode: el veneno se decide al procesar, según el interruptor actual del panel (no queda marcado en el pedido)
    service = OrderService(PostgresOrderRepository(pool), RabbitOrderPublisher(), PostgresOutbox(pool),
                           PostgresUserRepository(pool), poison_mode=lambda: bool(control.get()["poison"]))
    consumer = OrderConsumer(service, control)
    running: list[tuple[threading.Thread, threading.Event]] = []
    # Pagos Yape: un consumidor de la cola "pagos" en este mismo contenedor ("Matar al worker" también lo detiene)
    pagos = PagoConsumer(YapeService(PostgresYapeRepository(pool), service.repo))
    pagos_thread: threading.Thread | None = None

    # Supervisor: mantiene tantos consumidores como pida el panel (escalado horizontal en caliente)
    while True:
        if pagos_thread is None or not pagos_thread.is_alive():
            pagos_thread = threading.Thread(target=pagos.run, args=(threading.Event(),), name="pagos", daemon=True)
            pagos_thread.start()
            log.info("Consumidor de pagos Yape iniciado")
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
