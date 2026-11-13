from decimal import Decimal

import pytest

from app.models.order import OrderStatus, money
from app.models.product import Product
from app.repositories.catalog_repository import load_products
from app.repositories.interfaces import OrderPublisher
from app.repositories.sqlite_repository import SqliteOrderRepository, SqliteOutbox, SqliteProductRepository
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.order_service import (OUT_OF_STOCK_SKU, POISON_SKU, OrderRejected, OrderService,
                                        ProcessingError)


class FakePublisher(OrderPublisher):
    def __init__(self, fail=False):
        self.fail, self.sent = fail, []

    def publish_order_created(self, order_id):
        if self.fail:
            raise ConnectionError("broker caído")
        self.sent.append(order_id)


ITEMS = [{"sku": "A1", "quantity": 2, "unit_price": 10.0}]


def make_product(sku, stock=1000):
    return Product(sku, f"Producto {sku}", "Pruebas", 10, 20, 4.0, 1, stock, f"{sku}.jpg", "desc")


def make(fail=False, poison=True, seed=None):
    """poison: estado del interruptor «Pedidos venenosos» (por defecto activo, para probar el camino de fallo).
    seed: productos de la tabla products (por defecto A1 y B2 con mucho stock, más el catálogo real)."""
    pub = FakePublisher(fail)
    repo = SqliteOrderRepository(":memory:")
    products = SqliteProductRepository(repo, seed if seed is not None
                                       else [make_product("A1"), make_product("B2")] + load_products())
    mode = {"poison": poison}
    svc = OrderService(repo, pub, SqliteOutbox(repo), poison_mode=lambda: mode["poison"])
    svc.mode = mode   # las pruebas pueden apagar o encender el modo a mitad de camino
    svc.products = products
    return svc, pub


def test_create_only_writes_to_the_database_and_the_relay_publishes():
    svc, pub = make()
    order = svc.create_order("a@b.com", ITEMS)
    assert pub.sent == []                                         # la API nunca habla con RabbitMQ
    assert svc.get_order(order.id).status == OrderStatus.PENDING
    assert svc.outbox.pending_count() == 1
    assert order.total == 20.0
    assert svc.relay_outbox() == 1
    assert pub.sent == [order.id]
    assert svc.get_order(order.id).status == OrderStatus.PUBLISHED
    assert svc.outbox.pending_count() == 0


def test_queue_window_lists_both_ends_in_publication_order():
    svc, _ = make()
    orders = [svc.create_order("a@b.com", ITEMS) for _ in range(5)]
    assert svc.outbox.queue_window(2)["count"] == 0                # aún no publicados: no están en la fila
    svc.relay_outbox()
    svc.repo.update_status(orders[0].id, OrderStatus.PROCESSED)    # ya salió de la fila
    ids = [o.id for o in orders[1:]]
    window = svc.outbox.queue_window(2)
    assert (window["count"], window["head"], window["tail"]) == (4, ids[:2], ids[-2:])
    assert window["numbers"] == {ids[0]: 2, ids[1]: 3, ids[2]: 4, ids[3]: 5}    # cuenta desde el primer pedido, aunque ya salió


def test_broker_down_event_waits_in_outbox_then_relay_publishes():
    svc, pub = make(fail=True)
    order = svc.create_order("a@b.com", ITEMS)
    assert svc.get_order(order.id).status == OrderStatus.PENDING  # el pedido no se perdió...
    assert svc.outbox.pending_count() == 1                        # ...y su evento espera en el outbox
    assert svc.relay_outbox(min_age_s=0) == 0                     # broker aún caído: no se marca nada
    assert svc.outbox.pending_count() == 1
    pub.fail = False
    assert svc.relay_outbox(min_age_s=0) == 1
    assert svc.outbox.pending_count() == 0
    assert svc.get_order(order.id).status == OrderStatus.PUBLISHED
    assert pub.sent == [order.id]


class FlakyPublisher(FakePublisher):
    """Acepta los primeros `ok` mensajes y luego el broker se cae."""
    def __init__(self, ok):
        super().__init__()
        self.ok = ok

    def publish_order_created(self, order_id):
        if len(self.sent) >= self.ok:
            raise ConnectionError("broker caído")
        self.sent.append(order_id)


def test_relay_stops_when_broker_falls_midway_and_resumes_in_order():
    svc, _ = make(fail=True)
    orders = [svc.create_order("a@b.com", ITEMS) for _ in range(3)]
    svc.publisher = FlakyPublisher(ok=1)
    assert svc.relay_outbox(min_age_s=0) == 1                    # se cae tras el primero: no se salta ninguno
    assert svc.outbox.pending_count() == 2
    assert [svc.get_order(o.id).status for o in orders] == [OrderStatus.PUBLISHED, OrderStatus.PENDING,
                                                           OrderStatus.PENDING]
    svc.publisher = pub = FakePublisher()
    assert svc.relay_outbox(min_age_s=0) == 2
    assert pub.sent == [orders[1].id, orders[2].id]              # los pendientes salen en orden
    assert svc.outbox.pending_count() == 0


def test_outbox_recent_lists_newest_first_with_publish_time():
    svc, _ = make()
    second = svc.create_order("c@d.com", ITEMS)                   # publicado por el relay
    svc.relay_outbox()
    first = svc.create_order("a@b.com", ITEMS)                    # aún pendiente
    newest, oldest = svc.outbox.recent(6)
    assert (newest["aggregate_id"], oldest["aggregate_id"]) == (first.id, second.id)
    assert newest["event_type"] == "OrderCreated" and newest["published_at"] is None
    assert oldest["published_at"] is not None
    assert "+00:00" in newest["created_at"]                       # ISO con zona horaria


def test_order_and_event_are_saved_atomically():
    svc, pub = make()
    order = svc.create_order("a@b.com", ITEMS)
    assert svc.outbox.pending_count() == 1                        # el pedido y su evento se guardan juntos
    with pytest.raises(Exception):                                # mismo id: falla el INSERT del pedido...
        svc.repo.save(order)
    assert svc.repo.count_by_status()["PENDING"] == 1
    assert svc.outbox.pending_count() == 1                        # ...y tampoco queda un evento huérfano


def test_reconcile_resends_published_but_lost_messages():
    svc, pub = make()
    lost = svc.create_order("a@b.com", ITEMS)                     # el broker "lo olvidó": nunca se procesa
    done = svc.create_order("c@d.com", ITEMS)
    svc.relay_outbox()
    svc.process_order(done.id)
    assert svc.repo.stale_published(older_than_s=0) == [lost.id]
    pub.sent.clear()
    assert svc.reconcile_lost(older_than_s=0) == 1
    assert pub.sent == [lost.id]


def test_process_is_idempotent():
    svc, _ = make()
    order = svc.create_order("a@b.com", ITEMS)
    svc.process_order(order.id)
    svc.process_order(order.id)  # duplicado: no falla
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED


def test_poison_sku_fails_only_while_the_mode_is_active():
    svc, _ = make(poison=True)
    order = svc.create_order("a@b.com", [{"sku": POISON_SKU, "quantity": 1, "unit_price": 1}])
    with pytest.raises(ProcessingError):
        svc.process_order(order.id)
    svc.mode["poison"] = False                       # el modo se apagó: el mismo pedido ya no falla
    svc.process_order(order.id)
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED


def test_out_of_stock_is_a_final_rejection_not_an_error():
    svc, _ = make()
    order = svc.create_order("a@b.com", [{"sku": OUT_OF_STOCK_SKU, "quantity": 1, "unit_price": 1}])
    with pytest.raises(OrderRejected):
        svc.process_order(order.id)
    assert svc.get_order(order.id).status == OrderStatus.REJECTED


def test_rejected_order_is_never_processed_again():
    svc, _ = make()
    order = svc.create_order("a@b.com", ITEMS)
    svc.repo.update_status(order.id, OrderStatus.REJECTED)
    svc.process_order(order.id)                      # duplicado de un pedido rechazado: se salta
    assert svc.get_order(order.id).status == OrderStatus.REJECTED


def test_requeue_from_dlq_only_touches_failed_orders():
    svc, _ = make()
    failed, done = svc.create_order("a@b.com", ITEMS), svc.create_order("c@d.com", ITEMS)
    svc.mark_failed(failed.id)
    svc.process_order(done.id)
    assert svc.mark_retrying(failed.id) is True
    assert svc.get_order(failed.id).status == OrderStatus.PUBLISHED
    assert svc.mark_retrying(done.id) is False       # ya confirmado: no vuelve a "en cola" (no se procesa dos veces)
    assert svc.get_order(done.id).status == OrderStatus.PROCESSED


# --- checkout de la tienda --------------------------------------------------

def make_checkout():
    svc, _ = make()
    return CheckoutService(svc.products, svc), svc   # el catálogo y su stock salen de la tabla products


def test_checkout_uses_catalog_prices_and_tracks_position():
    co, svc = make_checkout()
    price = co.products.get("MOUSE-GAMER").price
    first = co.checkout("a@b.com", [{"sku": "MOUSE-GAMER", "quantity": 2}])
    second = co.checkout("c@d.com", [{"sku": "MOUSE-GAMER", "quantity": 1}, {"sku": "MOUSE-GAMER", "quantity": 1}])
    assert first.total == round(2 * price, 2)    # precio del catálogo, no del navegador
    assert len(second.items) == 1 and second.items[0].quantity == 2  # líneas repetidas se agrupan
    assert svc.track(second.id)["position"] == 2  # hay un pedido antes en la cola
    svc.process_order(first.id)
    assert svc.track(first.id)["position"] == 0
    assert svc.track(second.id)["position"] == 1


@pytest.mark.parametrize("email,lines", [
    ("sin-arroba", [{"sku": "MOUSE-GAMER", "quantity": 1}]),
    ("a@b.com", [{"sku": "NO-EXISTE", "quantity": 1}]),
    ("a@b.com", [{"sku": "MOUSE-GAMER", "quantity": 11}]),
    ("a@b.com", []),
])
def test_checkout_rejects_invalid(email, lines):
    co, _ = make_checkout()
    with pytest.raises(CheckoutError):
        co.checkout(email, lines)


# --- dinero exacto ------------------------------------------------------------

def test_money_is_exact_decimal_end_to_end():
    svc, _ = make()
    orders = [svc.create_order("a@b.com", [{"sku": "A1", "quantity": 1, "unit_price": 0.1}]) for _ in range(3)]
    for o in orders:
        svc.process_order(o.id)
    assert svc.repo.revenue(OrderStatus.PROCESSED) == Decimal("0.30")    # con float: 0.30000000000000004
    stored = svc.get_order(orders[0].id)                                   # ida y vuelta por la BD sin perder exactitud
    assert isinstance(stored.total, Decimal) and stored.items[0].unit_price == Decimal("0.10")


def test_money_rounds_half_up_like_a_cashier():
    assert money(2.675) == Decimal("2.68")                                 # round(2.675, 2) con float da 2.67
