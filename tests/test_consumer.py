"""Pruebas del consumidor (OrderConsumer) con un canal falso: no necesitan RabbitMQ."""
import json
from types import SimpleNamespace

import pika
import psycopg
import pytest

from app import config
from app.messaging import rabbitmq
from app.messaging.rabbitmq import DeadLetterInspector, OrderConsumer
from app.models.order import OrderStatus
from app.services.order_service import OUT_OF_STOCK_SKU, POISON_SKU
from tests.test_order_service import make

OK = [{"sku": "A1", "quantity": 1, "unit_price": 10}]
POISON = [{"sku": POISON_SKU, "quantity": 1, "unit_price": 1}]
OUT_OF_STOCK = [{"sku": OUT_OF_STOCK_SKU, "quantity": 1, "unit_price": 1}]


class FakeChannel:
    def __init__(self, publish_error=None):
        self.publish_error = publish_error
        self.acks, self.nacks, self.published = [], [], []

    def basic_ack(self, tag):
        self.acks.append(tag)

    def basic_nack(self, tag, requeue):
        self.nacks.append((tag, requeue))

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        if self.publish_error:
            raise self.publish_error
        self.published.append((routing_key, properties.headers))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(rabbitmq.time, "sleep", lambda _: None)


def deliver(svc, order_id, retries=0, ch=None, body=None):
    ch = ch or FakeChannel()
    consumer = OrderConsumer(svc, SimpleNamespace(get=lambda: {"worker_delay": 0}))
    consumer._on_message(ch, SimpleNamespace(delivery_tag=7), SimpleNamespace(headers={"retries": retries}),
                         body or json.dumps({"order_id": order_id}).encode())
    return ch


def test_ok_message_is_processed_then_acked():
    svc, _ = make()
    order = svc.create_order("a@b.com", OK)
    ch = deliver(svc, order.id)
    assert ch.acks == [7] and ch.nacks == []
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED


def test_business_error_goes_to_retry_queue_with_backoff():
    svc, _ = make()
    order = svc.create_order("a@b.com", POISON)
    ch = deliver(svc, order.id, retries=1)
    (queue, headers), = ch.published
    assert queue == config.RETRY_QUEUES[1] and headers["retries"] == 2
    assert ch.acks == [7]                                       # el original se confirma solo con la copia a salvo


def test_retry_publish_rejected_by_broker_leaves_original_unacked():
    svc, _ = make()
    order = svc.create_order("a@b.com", POISON)
    ch = FakeChannel(publish_error=pika.exceptions.NackError([]))
    with pytest.raises(pika.exceptions.AMQPError):              # run() reconecta y RabbitMQ lo reentrega
        deliver(svc, order.id, ch=ch)
    assert ch.acks == [] and ch.nacks == []


def test_exhausted_retries_marks_failed_and_dead_letters():
    svc, _ = make()
    order = svc.create_order("a@b.com", POISON)
    ch = deliver(svc, order.id, retries=config.MAX_RETRIES)
    assert ch.nacks == [(7, False)] and ch.published == []
    assert svc.get_order(order.id).status == OrderStatus.FAILED


def test_db_down_while_marking_failed_requeues_instead_of_crashing():
    svc, _ = make()
    order = svc.create_order("a@b.com", POISON)
    svc.relay_outbox()
    def db_down(_):
        raise psycopg.OperationalError("PostgreSQL caído")
    svc.mark_failed = db_down
    ch = deliver(svc, order.id, retries=config.MAX_RETRIES)    # antes la excepción mataba el hilo del worker
    assert ch.nacks == [(7, True)]                              # vuelve a la cola, no se pierde ni va a la DLQ
    assert svc.get_order(order.id).status == OrderStatus.PUBLISHED


def test_db_down_while_processing_requeues_without_spending_retries():
    svc, _ = make()
    order = svc.create_order("a@b.com", OK)
    def db_down(_):
        raise psycopg.OperationalError("PostgreSQL caído")
    svc.process_order = db_down
    ch = deliver(svc, order.id)
    assert ch.nacks == [(7, True)] and ch.published == []


def test_unreadable_message_goes_straight_to_dlq():
    svc, _ = make()
    ch = deliver(svc, None, body=b"no es json")
    assert ch.nacks == [(7, False)]


# --- rechazos de negocio, BD caída e idempotencia ---------------------------------------------------------------------
def test_out_of_stock_is_acked_as_rejected_and_never_reaches_the_dlq():
    svc, _ = make()
    order = svc.create_order("a@b.com", OUT_OF_STOCK)
    ch = deliver(svc, order.id, retries=config.MAX_RETRIES)    # aun con los reintentos agotados
    assert ch.acks == [7] and ch.nacks == [] and ch.published == []
    assert svc.get_order(order.id).status == OrderStatus.REJECTED


def test_db_down_never_sends_to_the_dlq_nor_spends_retries():
    svc, _ = make()
    order = svc.create_order("a@b.com", POISON)
    svc.relay_outbox()
    def db_down(_):
        raise psycopg.OperationalError("PostgreSQL caído")
    svc.process_order = db_down
    for retries in range(config.MAX_RETRIES + 1):               # en cualquier punto del ciclo de reintentos
        ch = deliver(svc, order.id, retries=retries)
        assert ch.nacks == [(7, True)] and ch.published == []   # vuelve a la cola, sin copia a reintento ni DLQ
    assert svc.get_order(order.id).status == OrderStatus.PUBLISHED


def test_duplicate_of_a_finished_order_is_acked_and_skipped():
    svc, _ = make()
    order = svc.create_order("a@b.com", OK)
    deliver(svc, order.id)
    calls = []
    svc.repo.mark_processed = lambda *a: calls.append(a)        # no debe volver a confirmarse
    ch = deliver(svc, order.id)
    assert ch.acks == [7] and calls == []
    rejected = svc.create_order("a@b.com", OUT_OF_STOCK)
    deliver(svc, rejected.id)
    ch = deliver(svc, rejected.id)                              # duplicado del rechazado
    assert ch.acks == [7] and svc.get_order(rejected.id).status == OrderStatus.REJECTED


# --- reprocesar la DLQ -----------------------------------------------------------------------------------------------
class FakeDlqChannel(FakeChannel):
    """Canal con una DLQ en memoria: basic_get entrega sus mensajes con los headers que dejó el último fallo."""
    def __init__(self, messages):
        super().__init__()
        self.queue = list(messages)

    def confirm_delivery(self):
        pass

    def basic_get(self, queue, auto_ack=False):
        if not self.queue:
            return None, None, None
        body, headers = self.queue.pop(0)
        return SimpleNamespace(delivery_tag=len(self.queue) + 1), SimpleNamespace(headers=headers), body

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        self.published.append((routing_key, properties.headers, body))


def test_reprocess_resets_retries_drops_x_death_and_succeeds_when_poison_is_off(monkeypatch):
    svc, _ = make(poison=True)
    order = svc.create_order("a@b.com", POISON)
    svc.relay_outbox()
    ch = deliver(svc, order.id, retries=config.MAX_RETRIES)     # agotó sus reintentos → DLQ
    assert ch.nacks == [(7, False)] and svc.get_order(order.id).status == OrderStatus.FAILED

    body = json.dumps({"order_id": order.id}).encode()
    dead = (body, {"retries": config.MAX_RETRIES, "x-last-error": "Fallo simulado",
                   "x-death": [{"count": 1, "reason": "rejected", "queue": config.QUEUE}]})
    dlq_ch = FakeDlqChannel([dead])
    monkeypatch.setattr(DeadLetterInspector, "_open", lambda self: (SimpleNamespace(close=lambda: None), dlq_ch))
    assert DeadLetterInspector().reprocess() == [order.id]
    (routing_key, headers, sent), = dlq_ch.published
    assert routing_key == config.ROUTING_KEY and sent == body
    assert headers["retries"] == 0 and "x-death" not in headers and "x-last-error" not in headers
    assert dlq_ch.acks == [1] and dlq_ch.queue == []            # salió de la DLQ

    svc.mark_retrying(order.id)
    svc.mode["poison"] = False                                  # el modo venenoso ya está apagado
    ch = deliver(svc, order.id, retries=headers["retries"])
    assert ch.acks == [7] and ch.nacks == [] and ch.published == []   # se procesa, no vuelve a la DLQ
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED


def test_reprocessed_message_while_poison_is_still_on_starts_its_retries_from_zero():
    svc, _ = make(poison=True)
    order = svc.create_order("a@b.com", POISON)
    ch = deliver(svc, order.id, retries=0)                      # contador en 0: primer reintento, no DLQ directa
    (queue, headers), = ch.published
    assert queue == config.RETRY_QUEUES[0] and headers["retries"] == 1 and ch.nacks == []


# --- cuadre final: aceptados = procesados + rechazados + DLQ -----------------------------------------------------------
def test_every_accepted_order_ends_processed_rejected_or_in_the_dlq():
    svc, _ = make(poison=True)
    orders = [svc.create_order("a@b.com", items) for items in [OK] * 6 + [POISON] * 3 + [OUT_OF_STOCK] * 2]
    svc.relay_outbox()
    pending = [(o.id, 0) for o in orders]                       # (pedido, contador de reintentos del mensaje)
    dlq = []
    while pending:
        order_id, retries = pending.pop(0)
        ch = deliver(svc, order_id, retries=retries)
        if ch.published:                                       # copia a la cola de espera: vuelve con retries + 1
            pending.append((order_id, ch.published[0][1]["retries"]))
        elif ch.nacks == [(7, False)]:
            dlq.append(order_id)
    counts = svc.repo.count_by_status()
    assert counts["PROCESSED"] == 6 and counts["REJECTED"] == 2 and counts["FAILED"] == len(dlq) == 3
    assert len(orders) == counts["PROCESSED"] + counts["REJECTED"] + len(dlq)
