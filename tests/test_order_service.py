import pytest

from app.models.order import OrderStatus
from app.repositories.catalog_repository import InMemoryProductRepository
from app.repositories.interfaces import OrderPublisher
from app.repositories.sqlite_repository import SqliteOrderRepository, SqliteOutbox
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.order_service import OrderService, ProcessingError


class FakePublisher(OrderPublisher):
    def __init__(self, fail=False):
        self.fail, self.sent = fail, []

    def publish_order_created(self, order_id):
        if self.fail:
            raise ConnectionError("broker caído")
        self.sent.append(order_id)


ITEMS = [{"sku": "A1", "quantity": 2, "unit_price": 10.0}]


def make(fail=False):
    pub = FakePublisher(fail)
    repo = SqliteOrderRepository(":memory:")
    return OrderService(repo, pub, SqliteOutbox(repo)), pub


def test_create_publishes_and_marks_published():
    svc, pub = make()
    order = svc.create_order("a@b.com", ITEMS)
    assert pub.sent == [order.id]
    assert svc.get_order(order.id).status == OrderStatus.PUBLISHED
    assert order.total == 20.0


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


def test_order_and_event_are_saved_atomically():
    svc, pub = make()
    order = svc.create_order("a@b.com", ITEMS)
    assert svc.outbox.pending_count() == 0                        # publicado en el camino rápido
    with pytest.raises(Exception):                                # mismo id: falla el INSERT del pedido...
        svc.repo.save(order)
    assert svc.repo.count_by_status()["PUBLISHED"] == 1
    assert svc.outbox.pending_count() == 0                        # ...y tampoco queda un evento huérfano


def test_reconcile_resends_published_but_lost_messages():
    svc, pub = make()
    lost = svc.create_order("a@b.com", ITEMS)                     # el broker "lo olvidó": nunca se procesa
    done = svc.create_order("c@d.com", ITEMS)
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


def test_failing_sku_raises():
    svc, _ = make()
    order = svc.create_order("a@b.com", [{"sku": "FAIL", "quantity": 1, "unit_price": 1}])
    with pytest.raises(ProcessingError):
        svc.process_order(order.id)


# --- checkout de la tienda --------------------------------------------------

def make_checkout():
    svc, _ = make()
    return CheckoutService(InMemoryProductRepository(), svc), svc


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
