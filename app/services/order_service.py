import logging

from app.repositories.interfaces import Outbox, OrderPublisher, OrderRepository
from app.models.order import Order, OrderItem, OrderStatus

log = logging.getLogger(__name__)


class ProcessingError(Exception):
    """Error de negocio al procesar un pedido (provoca reintento)."""


class OrderService:
    def __init__(self, repo: OrderRepository, publisher: OrderPublisher, outbox: Outbox):
        self.repo = repo
        self.publisher = publisher
        self.outbox = outbox

    # --- lado API ---------------------------------------------------------
    def create_order(self, customer_email: str, items: list[dict]) -> Order:
        order = Order(customer_email, [OrderItem(**i) for i in items])
        # 1) Transactional Outbox: el pedido y su evento se guardan juntos en la BD (todo o nada).
        self.repo.save(order)
        # 2) Camino rápido: se intenta publicar ya. Si el broker no responde, el evento queda en el
        #    outbox y el relay lo enviará en cuanto vuelva. Nunca hay un pedido sin su mensaje.
        if self._publish_now(order.id):
            order.status = OrderStatus.PUBLISHED
        return order

    def _publish_now(self, order_id: str) -> bool:
        try:
            self.publisher.publish_order_created(order_id)
        except Exception as exc:
            log.warning("No se pudo publicar %s (queda en el outbox): %s", order_id, exc)
            return False
        self.outbox.mark_sent(order_id)
        self.repo.mark_published(order_id)
        return True

    def relay_outbox(self, limit: int = 500, min_age_s: float = 3) -> int:
        """Publica los eventos que quedaron en el outbox (p. ej. el broker estaba caído).
        min_age_s evita competir con el camino rápido de un pedido recién creado."""
        def publish(order_id: str) -> None:
            self.publisher.publish_order_created(order_id)
            self.repo.mark_published(order_id)
        return self.outbox.relay(publish, limit, min_age_s)

    def reconcile_lost(self, older_than_s: float) -> int:
        """Reconciliación: reenvía los pedidos cuyo mensaje se publicó pero nunca se procesó (se perdió en el
        broker). Es posible porque la BD los conserva; un duplicado no importa gracias a la idempotencia."""
        sent = 0
        for order_id in self.repo.stale_published(older_than_s):
            try:
                self.publisher.publish_order_created(order_id)
            except Exception:
                break
            sent += 1
        return sent

    def get_order(self, order_id: str) -> Order | None:
        return self.repo.get(order_id)

    def list_orders(self, status: OrderStatus | None = None) -> list[Order]:
        return self.repo.list(status)

    def orders_of(self, email: str, limit: int = 20) -> list[dict]:
        return [{**self.track(o.id), "created_at": o.created_at, "items": sum(i.quantity for i in o.items)}
                for o in self.repo.list_by_email(email, limit)]

    def track(self, order_id: str) -> dict | None:
        """Estado del pedido para el cliente, con su posición en la cola."""
        order = self.repo.get(order_id)
        if order is None:
            return None
        waiting = order.status in (OrderStatus.PENDING, OrderStatus.PUBLISHED)
        return {"id": order.id, "status": order.status.value, "total": order.total,
                "position": self.repo.count_ahead(order) + 1 if waiting else 0}

    # --- lado worker ------------------------------------------------------
    def process_order(self, order_id: str) -> None:
        order = self.repo.get(order_id)
        if order is None:
            raise ProcessingError(f"Pedido {order_id} no existe")
        # Idempotencia: RabbitMQ garantiza "al menos una vez", puede haber duplicados.
        if order.status == OrderStatus.PROCESSED:
            log.info("Pedido %s ya procesado, se ignora el duplicado", order_id)
            return
        # Simulación de lógica de negocio (stock, pago, correo...). SKU "FAIL" siempre falla.
        if any(i.sku == "FAIL" for i in order.items):
            raise ProcessingError("Producto sin stock (simulado)")
        self.repo.update_status(order_id, OrderStatus.PROCESSED)
        log.info("Pedido %s procesado. Total: %.2f", order_id, order.total)

    def mark_failed(self, order_id: str) -> None:
        self.repo.update_status(order_id, OrderStatus.FAILED)

    def mark_retrying(self, order_id: str) -> None:
        """Al reprocesar desde la DLQ el pedido vuelve a estar en camino."""
        self.repo.update_status(order_id, OrderStatus.PUBLISHED)
