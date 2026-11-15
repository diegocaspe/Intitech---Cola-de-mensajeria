import logging
import random
from typing import Callable

from app import config
from app.repositories.interfaces import ORDER_EVENT, Outbox, OrderPublisher, OrderRepository, UserRepository
from app.models.email import EmailEvent, EmailKind
from app.models.order import Order, OrderItem, OrderStatus, PaymentStatus
from app.services.payments import TOKEN_APPROVED, CardToken, FakePaymentProvider, PaymentUnavailable

log = logging.getLogger(__name__)

# SKU de prueba del modo "Pedidos venenosos": el simulador lo usa en 1 de cada 5 pedidos. Solo falla MIENTRAS el modo
# está activo; con el modo apagado es un producto más (así, al reprocesar la DLQ, esos pedidos se completan)
POISON_SKU = "FAIL"
# SKU de prueba sin stock: el pedido termina RECHAZADO (resultado de negocio), nunca en reintentos ni en la DLQ
OUT_OF_STOCK_SKU = "SIN-STOCK"


class ProcessingError(Exception):
    """Fallo al procesar que puede arreglarse solo (provoca reintento con espera y, al tercer fallo, DLQ)."""


class OrderRejected(Exception):
    """Resultado final de negocio (sin stock, pago rechazado): el pedido queda REJECTED, el mensaje se confirma y no
    se reintenta. Reintentarlo no cambiaría la respuesta y llenaría la DLQ de pedidos que no tienen ningún error."""


class OrderService:
    def __init__(self, repo: OrderRepository, publisher: OrderPublisher, outbox: Outbox,
                 users: UserRepository | None = None, poison_mode: Callable[[], bool] = lambda: False,
                 payments: FakePaymentProvider | None = None):
        self.repo = repo
        self.publisher = publisher
        self.outbox = outbox
        self.users = users   # opcional: con él, al confirmar un pedido de un cliente real se pide su comprobante
        self.poison_mode = poison_mode   # interruptor "Pedidos venenosos" del panel, leído en cada mensaje
        self.payments = payments or FakePaymentProvider()   # proveedor de pagos simulado (lo usa el worker)

    # --- lado API ---------------------------------------------------------
    def create_order(self, customer_email: str, items: list[dict], card: CardToken | None = None) -> Order:
        """card: la tarjeta ya convertida en token (checkout de la tienda). Sin tarjeta (simulador, escenarios,
        POST /orders) el pedido se paga solo con la tarjeta de prueba aprobada."""
        order = Order(customer_email, [OrderItem(**i) for i in items],
                      payment_token=card.token if card else TOKEN_APPROVED,
                      card_brand=card.brand if card else None, card_last4=card.last4 if card else None)
        # Transactional Outbox: el pedido y su evento se guardan juntos en la BD (todo o nada). La API no habla
        # con RabbitMQ: el relay (app/relay.py) lee el outbox y publica. Nunca hay un pedido sin su mensaje.
        self.repo.save(order)
        return order

    def relay_outbox(self, limit: int = 500, min_age_s: float = 0) -> int:
        """Publica los eventos pendientes del outbox, en orden. Lo llama el proceso relay. Los pedidos van a su cola y
        los correos (email.*) a la de correos, con las mismas garantías (persistentes y con publisher confirms)."""
        def publish(event_type: str, aggregate_id: str, payload: dict) -> None:
            if event_type == ORDER_EVENT:
                self.publisher.publish_order_created(aggregate_id)
                self.repo.mark_published(aggregate_id)
            else:
                self.publisher.publish_email(payload)
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
        reason = None
        if order.status == OrderStatus.REJECTED:   # la tienda distingue "pago rechazado" de "sin stock"
            reason = "pago_rechazado" if order.payment_status == PaymentStatus.DECLINED else "sin_stock"
        return {"id": order.id, "status": order.status.value, "total": order.total,
                "position": self.repo.count_ahead(order) + 1 if waiting else 0,
                "payment_status": order.payment_status.value if order.payment_status else None,
                "card": order.card_label, "reject_reason": reason,
                # el proveedor falló y el cobro espera su reintento (2 s, 5 s, 10 s)
                "payment_retry": (order.payment_status == PaymentStatus.PENDING and order.payment_failures > 0
                                  and order.status not in (OrderStatus.PROCESSED, OrderStatus.REJECTED))}

    # --- lado worker ------------------------------------------------------
    def process_order(self, order_id: str) -> None:
        order = self.repo.get(order_id)
        if order is None:
            raise ProcessingError(f"Pedido {order_id} no existe")
        # Idempotencia: RabbitMQ garantiza "al menos una vez", puede haber duplicados. Un pedido con resultado final
        # (confirmado o rechazado) no se vuelve a procesar.
        if order.status.final:
            log.info("Pedido %s ya %s, se ignora el duplicado", order_id, order.status.value)
            return
        skus = {i.sku for i in order.items}
        # Resultado de negocio: sin stock → RECHAZADO (final). No es un error del sistema: no gasta reintentos
        if OUT_OF_STOCK_SKU in skus:
            self.repo.reject_out_of_stock(order_id, "Sin stock: no se cobró")
            raise OrderRejected("Producto sin stock")
        # Fallo técnico simulado: solo mientras el modo venenoso está activo (la decisión no queda marcada para siempre)
        if POISON_SKU in skus and self.poison_mode():
            raise ProcessingError("Fallo simulado (modo «Pedidos venenosos» activo)")
        lines = self._stock_lines(order)
        # 1) ¿Hay stock? Solo se mira, no se descuenta. Si no alcanza, se rechaza SIN cobrar
        stock = self.repo.stock_of([sku for sku, _ in lines])
        short = next((sku for sku, qty in lines if stock.get(sku, 0) < qty), None)
        if short:
            self.repo.reject_out_of_stock(order_id, f"Sin stock de {short}: no se cobró")
            raise OrderRejected(f"Sin stock suficiente de {short} (no se cobró)")
        # 2) Cobro, con el order_id como clave de idempotencia. Si ya está APPROVED (RabbitMQ entregó el mensaje otra
        #    vez, o el worker murió tras cobrar) no se vuelve a cobrar
        if order.payment_status != PaymentStatus.APPROVED:
            self._charge(order)
        # 3) descuento del stock + confirmado + comprobante en el outbox, en UNA transacción: todo o nada. Si un ítem
        #    ya no alcanza (otro pedido se adelantó entre 1 y 3), nada se descuenta, el pedido queda RECHAZADO y el
        #    pago pasa a REEMBOLSADO en esa misma transacción
        result = self.repo.mark_processed(order_id, self._receipt(order), lines)
        if result.status == OrderStatus.REJECTED:
            raise OrderRejected(f"Sin stock suficiente de {result.out_of_stock} (pago reembolsado)")
        if result.status == OrderStatus.PROCESSED:
            log.info("Pedido %s procesado. Total: %.2f", order_id, order.total)

    def _charge(self, order: Order) -> None:
        """Cobra el pedido. Rechazado → REJECTED (resultado de negocio, sin reintentos ni DLQ). Error temporal del
        proveedor → ProcessingError (reintento a los 2 s, 5 s y 10 s). En los logs solo aparece el order_id."""
        attempt = order.payment_failures + 1   # 1 = primer intento; cada error temporal guardado suma uno
        # compras reales de la tienda: latencia de un proveedor de verdad; pedidos automáticos: casi nada
        lo, hi = config.PAYMENT_LATENCY if order.card_last4 else config.PAYMENT_LATENCY_TEST
        try:
            charge = self.payments.charge(order.id, order.payment_token or TOKEN_APPROVED, attempt,
                                          latency_s=random.uniform(lo, hi))
        except PaymentUnavailable as exc:
            self.repo.add_payment_failure(order.id)   # en la BD: el reintento sabe que ya falló aunque cambie el worker
            raise ProcessingError(f"{exc} (intento {attempt})") from exc
        if charge.status == PaymentStatus.DECLINED:
            self.repo.mark_payment_declined(order.id, charge.reason or "Pago rechazado")
            raise OrderRejected(f"Pago rechazado: {charge.reason} (no se descontó stock)")
        self.repo.mark_payment_approved(order.id)
        order.payment_status = PaymentStatus.APPROVED

    @staticmethod
    def _stock_lines(order: Order) -> list[tuple[str, int]]:
        """Unidades a descontar por SKU (sumando líneas repetidas). El SKU de prueba del modo venenoso no es un
        producto: no tiene stock que descontar."""
        qty: dict[str, int] = {}
        for i in order.items:
            if i.sku != POISON_SKU:
                qty[i.sku] = qty.get(i.sku, 0) + i.quantity
        return list(qty.items())

    def _receipt(self, order: Order) -> EmailEvent | None:
        """Comprobante de compra solo para clientes reales registrados desde la tienda: ni el administrador ni los
        compradores inventados por el simulador (no tienen cuenta) ni las cuentas de prueba anteriores."""
        if self.users is None:
            return None
        user = self.users.get_by_email(order.customer_email)
        if user is None or not user.receives_emails:
            return None
        return EmailEvent(EmailKind.COMPRA_CONFIRMADA, user.email, user.name, {
            "order_id": order.id,
            "created_at": order.created_at,
            "items": [i.to_dict() for i in order.items],   # precios como texto: exactos
            "total": str(order.total),
            # solo marca y últimos 4 dígitos ("Visa •••• 1111"); el comprobante se pide cuando el pago ya se aprobó
            "card": order.card_label,
            "payment_status": PaymentStatus.APPROVED.value,
        })

    def mark_failed(self, order_id: str) -> None:
        self.repo.update_status(order_id, OrderStatus.FAILED)

    def mark_retrying(self, order_id: str) -> bool:
        """Al reprocesar desde la DLQ el pedido FALLIDO vuelve a estar en camino. Uno con resultado final no se toca."""
        return self.repo.requeue_failed(order_id)
