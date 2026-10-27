import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable

import pika

from app import config
from app.application.order_service import ProcessingError
from app.application.ports import OrderPublisher

log = logging.getLogger(__name__)

_NET_ERRORS = (pika.exceptions.AMQPError, OSError)


def connect(retries: int = 30, delay: float = 2.0, heartbeat: int = 60) -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS),
        heartbeat=heartbeat,
        socket_timeout=3,
    )
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(params)
        except _NET_ERRORS:
            if attempt == retries:
                break
            log.warning("RabbitMQ no disponible (intento %d/%d)", attempt, retries)
            time.sleep(delay)
    raise ConnectionError("No se pudo conectar a RabbitMQ")


def declare_topology(ch) -> None:
    dead_letter = {"x-dead-letter-exchange": config.DLX, "x-dead-letter-routing-key": config.ROUTING_KEY}

    # DLQ: mensajes que agotaron sus reintentos
    ch.exchange_declare(config.DLX, "direct", durable=True)
    ch.queue_declare(config.DLQ, durable=True)
    ch.queue_bind(config.DLQ, config.DLX, config.ROUTING_KEY)

    # Camino normal: todo durable, sobrevive a un reinicio del broker
    ch.exchange_declare(config.EXCHANGE, "direct", durable=True)
    ch.queue_declare(config.QUEUE, durable=True, arguments=dead_letter)
    ch.queue_bind(config.QUEUE, config.EXCHANGE, config.ROUTING_KEY)

    # Camino SIN persistencia (solo para comparar): exchange y cola no durables, desaparecen si el broker cae
    ch.exchange_declare(config.VOLATILE_EXCHANGE, "direct", durable=False)
    ch.queue_declare(config.VOLATILE_QUEUE, durable=False, arguments=dead_letter)
    ch.queue_bind(config.VOLATILE_QUEUE, config.VOLATILE_EXCHANGE, config.ROUTING_KEY)

    # Colas de espera para reintentos: al vencer el TTL, RabbitMQ devuelve el mensaje a la cola principal
    for delay, queue in zip(config.RETRY_DELAYS, config.RETRY_QUEUES):
        ch.queue_declare(queue, durable=True, arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": config.EXCHANGE,
            "x-dead-letter-routing-key": config.ROUTING_KEY,
        })


def purge_queues() -> None:
    conn = connect(retries=1)
    try:
        ch = conn.channel()
        declare_topology(ch)
        for q in (config.QUEUE, config.VOLATILE_QUEUE, config.DLQ, *config.RETRY_QUEUES):
            ch.queue_purge(q)
    finally:
        conn.close()


class RabbitOrderPublisher(OrderPublisher):
    """Conexión reutilizable con publisher confirms. Si el broker cae falla rápido (circuit breaker)
    y el evento queda en el outbox de la base de datos hasta que el relay lo reenvíe.

    persistent_mode: función que indica si publicar en el camino durable (disco) o en el volátil (memoria)."""

    RETRY_AFTER = 1.0

    def __init__(self, persistent_mode: Callable[[], bool] = lambda: True):
        self.persistent_mode = persistent_mode
        self._lock = threading.Lock()
        self._conn = None
        self._ch = None
        self._down_until = 0.0
        self._healthy = True

    def is_healthy(self) -> bool:
        return self._healthy

    def _channel(self):
        if self._conn is None or self._conn.is_closed or self._ch is None or self._ch.is_closed:
            self._close()
            self._conn = connect(retries=1, heartbeat=0)
            self._ch = self._conn.channel()
            declare_topology(self._ch)
            self._ch.confirm_delivery()  # el broker avisa que recibió (y, si es persistente, guardó) el mensaje
        return self._ch

    def _close(self):
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        self._conn = self._ch = None

    def publish_order_created(self, order_id: str) -> None:
        durable = self.persistent_mode()
        with self._lock:
            if time.monotonic() < self._down_until:
                raise ConnectionError("RabbitMQ no disponible")
            last_exc = None
            for _ in range(2):  # la 2ª vez reabre una conexión nueva (la anterior pudo morir en reposo)
                try:
                    self._channel().basic_publish(
                        exchange=config.EXCHANGE if durable else config.VOLATILE_EXCHANGE,
                        routing_key=config.ROUTING_KEY,
                        body=json.dumps({"order_id": order_id}),
                        properties=pika.BasicProperties(
                            # Persistent: se escribe a disco. Transient: solo vive en memoria.
                            delivery_mode=pika.DeliveryMode.Persistent if durable else pika.DeliveryMode.Transient,
                            content_type="application/json",
                            headers={"retries": 0},
                        ),
                        mandatory=True,
                    )
                    self._healthy = True
                    return
                except (*_NET_ERRORS, ConnectionError) as exc:
                    last_exc = exc
                    self._close()
            self._healthy = False
            self._down_until = time.monotonic() + self.RETRY_AFTER
            raise ConnectionError(f"Publicación fallida: {last_exc}")


class OrderConsumer:
    """Consume con ack manual. Los errores de negocio se reintentan con espera creciente (2 s, 5 s, 10 s);
    al agotarlos el mensaje va a la DLQ. Se reconecta solo si RabbitMQ se cae y vuelve."""

    def __init__(self, service, control):
        self.service = service
        self.control = control

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            conn = None
            try:
                conn = connect(retries=1)
                ch = conn.channel()
                declare_topology(ch)
                ch.basic_qos(prefetch_count=1)  # un mensaje a la vez por worker
                for queue in (config.QUEUE, config.VOLATILE_QUEUE):
                    ch.basic_consume(queue, on_message_callback=self._on_message, auto_ack=False)
                log.info("Escuchando '%s' y '%s'", config.QUEUE, config.VOLATILE_QUEUE)
                while not stop.is_set():
                    conn.process_data_events(time_limit=0.5)
            except (*_NET_ERRORS, ConnectionError) as exc:
                log.warning("Conexión con RabbitMQ perdida (%s). Reintentando...", exc)
                stop.wait(2)
            finally:
                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass

    def _on_message(self, ch, method, props, body) -> None:
        order_id = json.loads(body)["order_id"]
        headers = props.headers or {}
        retries = headers.get("retries", 0)
        try:
            time.sleep(self.control.get()["worker_delay"])  # trabajo simulado (stock, pago, correo...)
            self.service.process_order(order_id)
            ch.basic_ack(method.delivery_tag)  # solo se confirma TRAS persistir el resultado
        except ProcessingError as exc:          # error de negocio: reintento diferido y luego DLQ
            if retries < config.MAX_RETRIES:
                wait_queue = config.RETRY_QUEUES[retries]
                log.error("Pedido %s falló (%s). Reintento %d en %d s", order_id[:8], exc, retries + 1,
                          config.RETRY_DELAYS[retries])
                ch.basic_publish("", wait_queue, body, pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                    headers={**headers, "retries": retries + 1, "x-last-error": str(exc),
                             "x-first-failure": headers.get("x-first-failure") or datetime.now(timezone.utc).isoformat()},
                ))
                ch.basic_ack(method.delivery_tag)
            else:
                log.error("Pedido %s agotó sus %d reintentos -> DLQ", order_id[:8], config.MAX_RETRIES)
                self.service.mark_failed(order_id)
                ch.basic_nack(method.delivery_tag, requeue=False)  # RabbitMQ lo envía a la DLQ (con cabecera x-death)
        except Exception as exc:
            # error de infraestructura (p. ej. la base de datos está caída): no es culpa del pedido,
            # así que no gasta reintentos; vuelve a la cola y espera a que el sistema se recupere
            log.warning("Infraestructura no disponible procesando %s: %s. Mensaje devuelto a la cola.",
                        order_id[:8], exc.__class__.__name__)
            time.sleep(2)
            ch.basic_nack(method.delivery_tag, requeue=True)


class DeadLetterInspector:
    """Revisa, reprocesa o descarta los mensajes de la DLQ sin perderlos en el camino."""

    def _open(self):
        conn = connect(retries=1, heartbeat=0)
        ch = conn.channel()
        declare_topology(ch)
        return conn, ch

    def peek(self, limit: int = 50) -> list[dict]:
        """Lee los mensajes sin confirmarlos; al cerrar el canal RabbitMQ los devuelve intactos a la DLQ."""
        conn, ch = self._open()
        items = []
        try:
            for _ in range(limit):
                method, props, body = ch.basic_get(config.DLQ, auto_ack=False)
                if method is None:
                    break
                headers = props.headers or {}
                death = (headers.get("x-death") or [{}])[0]
                died_at = death.get("time")
                items.append({
                    "order_id": json.loads(body).get("order_id"),
                    "retries": headers.get("retries", 0),
                    "error": _text(headers.get("x-last-error")) or "Rechazado por el worker",
                    "first_failure": _text(headers.get("x-first-failure")),
                    # RabbitMQ guarda la hora en UTC sin zona: se marca como UTC para que el navegador la convierta
                    "dead_at": died_at.replace(tzinfo=timezone.utc).isoformat() if hasattr(died_at, "isoformat") else None,
                    "reason": _text(death.get("reason")),
                    "queue": _text(death.get("queue")),
                })
        finally:
            conn.close()
        return items

    def _take(self, order_ids: set[str] | None, action: Callable) -> list[str]:
        """Recorre la DLQ: a los elegidos les aplica action(ch, body) y los confirma; al resto los deja como estaban."""
        conn, ch = self._open()
        done = []
        try:
            ch.confirm_delivery()
            while True:
                method, props, body = ch.basic_get(config.DLQ, auto_ack=False)
                if method is None:
                    break
                order_id = json.loads(body).get("order_id")
                if order_ids is None or order_id in order_ids:
                    action(ch, body)
                    ch.basic_ack(method.delivery_tag)
                    done.append(order_id)
        finally:
            conn.close()   # los no elegidos (sin ack) vuelven a la DLQ
        return done

    def reprocess(self, order_ids: set[str] | None = None) -> list[str]:
        def republish(ch, body):
            ch.basic_publish(config.EXCHANGE, config.ROUTING_KEY, body, pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent, content_type="application/json",
                headers={"retries": 0, "x-reprocessed": True}), mandatory=True)
        return self._take(order_ids, republish)

    def discard(self, order_ids: set[str] | None = None) -> list[str]:
        return self._take(order_ids, lambda ch, body: None)


def _text(value) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)
