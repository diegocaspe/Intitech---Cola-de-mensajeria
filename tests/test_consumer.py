"""Pruebas del consumidor (OrderConsumer) con un canal falso: no necesitan RabbitMQ."""
import json
from types import SimpleNamespace

import pika
import psycopg
import pytest

from app import config
from app.messaging import rabbitmq
from app.messaging.rabbitmq import OrderConsumer
from app.models.order import OrderStatus
from tests.test_order_service import make

OK = [{"sku": "A1", "quantity": 1, "unit_price": 10}]
POISON = [{"sku": "FAIL", "quantity": 1, "unit_price": 1}]


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
