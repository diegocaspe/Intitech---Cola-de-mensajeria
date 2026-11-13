import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable

import pika

from app import config
from app.services.order_service import OrderRejected, ProcessingError
from app.repositories.interfaces import EmailSendError, OrderPublisher

log = logging.getLogger(__name__)

_NET_ERRORS = (pika.exceptions.AMQPError, OSError)

# Con el contenedor del broker detenido, cada intento tarda varios segundos solo en resolver el nombre.
# Tras un fallo, los intentos rápidos (retries=1) fallan al instante mientras un hilo de fondo sondea
# el broker cada segundo: así el panel y la tienda no esperan ni acumulan peticiones bloqueadas.
PROBE_EVERY = 1.0
_broker_down = threading.Event()
_probe_lock = threading.Lock()
_probing = False


def _probe(params: pika.ConnectionParameters) -> None:
    global _probing
    while True:
        time.sleep(PROBE_EVERY)
        try:
            pika.BlockingConnection(params).close()
            break
        except Exception:
            continue
    _broker_down.clear()
    with _probe_lock:
        _probing = False


def _mark_down(params: pika.ConnectionParameters) -> None:
    global _probing
    _broker_down.set()
    with _probe_lock:
        if _probing:
            return
        _probing = True
    threading.Thread(target=_probe, args=(params,), daemon=True, name="broker-probe").start()


def connect(retries: int = 30, delay: float = 2.0, heartbeat: int = 60) -> pika.BlockingConnection:
    quick = retries == 1
    if quick and _broker_down.is_set():
        raise ConnectionError("No se pudo conectar a RabbitMQ")
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS),
        heartbeat=heartbeat,
        socket_timeout=3,
    )
    for attempt in range(1, retries + 1):
        try:
            conn = pika.BlockingConnection(params)
            _broker_down.clear()
            return conn
        except _NET_ERRORS:
            if attempt == retries:
                break
            log.warning("RabbitMQ no disponible (intento %d/%d)", attempt, retries)
            time.sleep(delay)
    if quick:
        _mark_down(params)
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

    # Correos: el mismo esquema, en su propio exchange. Cola durable, reintentos 2 s / 5 s / 10 s con TTL y su DLQ
    # (en el mismo DLX que la de pedidos, con otra routing key)
    ch.queue_declare(config.EMAIL_DLQ, durable=True)
    ch.queue_bind(config.EMAIL_DLQ, config.DLX, config.EMAIL_ROUTING_KEY)
    ch.exchange_declare(config.EMAIL_EXCHANGE, "direct", durable=True)
    ch.queue_declare(config.EMAIL_QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": config.DLX, "x-dead-letter-routing-key": config.EMAIL_ROUTING_KEY})
    ch.queue_bind(config.EMAIL_QUEUE, config.EMAIL_EXCHANGE, config.EMAIL_ROUTING_KEY)
    for delay, queue in zip(config.RETRY_DELAYS, config.EMAIL_RETRY_QUEUES):
        ch.queue_declare(queue, durable=True, arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": config.EMAIL_EXCHANGE,
            "x-dead-letter-routing-key": config.EMAIL_ROUTING_KEY,
        })

    # Pagos Yape: el mismo esquema otra vez. Cola durable "pagos", reintentos 2 s / 5 s / 10 s con TTL y "pagos.dlq"
    ch.queue_declare(config.PAGOS_DLQ, durable=True)
    ch.queue_bind(config.PAGOS_DLQ, config.DLX, config.PAGOS_ROUTING_KEY)
    ch.exchange_declare(config.PAGOS_EXCHANGE, "direct", durable=True)
    ch.queue_declare(config.PAGOS_QUEUE, durable=True, arguments={
        "x-dead-letter-exchange": config.DLX, "x-dead-letter-routing-key": config.PAGOS_ROUTING_KEY})
    ch.queue_bind(config.PAGOS_QUEUE, config.PAGOS_EXCHANGE, config.PAGOS_ROUTING_KEY)
    for delay, queue in zip(config.RETRY_DELAYS, config.PAGOS_RETRY_QUEUES):
        ch.queue_declare(queue, durable=True, arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": config.PAGOS_EXCHANGE,
            "x-dead-letter-routing-key": config.PAGOS_ROUTING_KEY,
        })


def purge_queues() -> None:
    conn = connect(retries=1)
    try:
        ch = conn.channel()
        declare_topology(ch)
        for q in (config.QUEUE, config.VOLATILE_QUEUE, config.DLQ, *config.RETRY_QUEUES,
                  config.EMAIL_QUEUE, config.EMAIL_DLQ, *config.EMAIL_RETRY_QUEUES,
                  config.PAGOS_QUEUE, config.PAGOS_DLQ, *config.PAGOS_RETRY_QUEUES):
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

    def publish_email(self, payload: dict) -> None:
        """Correos: siempre por el camino durable (el interruptor de persistencia del panel es solo para pedidos)."""
        with self._lock:
            if time.monotonic() < self._down_until:
                raise ConnectionError("RabbitMQ no disponible")
            last_exc = None
            for _ in range(2):
                try:
                    self._channel().basic_publish(
                        exchange=config.EMAIL_EXCHANGE,
                        routing_key=config.EMAIL_ROUTING_KEY,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent,
                                                        content_type="application/json", headers={"retries": 0}),
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

    def publish_pago(self, payload: dict) -> None:
        """Notificaciones de Yape: siempre por el camino durable, persistentes y con publisher confirms."""
        with self._lock:
            if time.monotonic() < self._down_until:
                raise ConnectionError("RabbitMQ no disponible")
            last_exc = None
            for _ in range(2):
                try:
                    self._channel().basic_publish(
                        exchange=config.PAGOS_EXCHANGE,
                        routing_key=config.PAGOS_ROUTING_KEY,
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent,
                                                        content_type="application/json", headers={"retries": 0}),
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
    """Consume con ack manual. Tres desenlaces según el tipo de problema:
      - fallo del pedido (ProcessingError): reintento con espera creciente (2 s, 5 s, 10 s) y, al agotarlos, DLQ;
      - rechazo de negocio (OrderRejected: sin stock, pago rechazado): es un resultado final, se confirma (ack);
      - infraestructura caída (p. ej. PostgreSQL): vuelve a la cola con espera, sin gastar reintentos ni ir a la DLQ.
    Se reconecta solo si RabbitMQ se cae y vuelve."""

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
                ch.confirm_delivery()  # la copia que va a la cola de reintento también debe confirmarla el broker
                for queue in (config.QUEUE, config.VOLATILE_QUEUE):
                    ch.basic_consume(queue, on_message_callback=self._on_message, auto_ack=False)
                log.info("Escuchando '%s' y '%s'", config.QUEUE, config.VOLATILE_QUEUE)
                while not stop.is_set():
                    conn.process_data_events(time_limit=0.5)
            except (*_NET_ERRORS, ConnectionError) as exc:
                log.warning("Conexión con RabbitMQ perdida (%s). Reintentando...", exc)
                stop.wait(2)
            except Exception:
                # red de seguridad: un error inesperado no debe matar el hilo del worker; el mensaje sin ack vuelve a la cola
                log.exception("Error inesperado en el consumidor. Reconectando...")
                stop.wait(2)
            finally:
                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass

    def _on_message(self, ch, method, props, body) -> None:
        try:
            order_id = json.loads(body)["order_id"]
        except (ValueError, KeyError, TypeError):
            log.error("Mensaje ilegible -> DLQ: %r", body[:100])
            ch.basic_nack(method.delivery_tag, requeue=False)
            return
        headers = props.headers or {}
        retries = headers.get("retries", 0)
        try:
            try:
                time.sleep(self.control.get()["worker_delay"])  # trabajo simulado (stock, pago, correo...)
                self.service.process_order(order_id)
            except OrderRejected as exc:        # resultado final ya guardado (REJECTED): se confirma, sin reintentos
                log.info("Pedido %s rechazado: %s", order_id[:8], exc)
            except ProcessingError as exc:      # fallo del pedido: reintento diferido y luego DLQ
                self._retry_or_dead_letter(ch, method, body, headers, retries, order_id, exc)
                return
            ch.basic_ack(method.delivery_tag)   # solo se confirma TRAS persistir el resultado
        except _NET_ERRORS:
            raise   # el canal murió: run() reconecta y RabbitMQ reentrega el mensaje que quedó sin ack
        except Exception as exc:
            # error de infraestructura (p. ej. la base de datos está caída, también al marcar el pedido como
            # FALLIDO): no es culpa del pedido, así que no gasta reintentos; vuelve a la cola y espera
            log.warning("Infraestructura no disponible procesando %s: %s. Mensaje devuelto a la cola.",
                        order_id[:8], exc.__class__.__name__)
            time.sleep(2)
            ch.basic_nack(method.delivery_tag, requeue=True)

    def _retry_or_dead_letter(self, ch, method, body, headers, retries, order_id, exc) -> None:
        if retries < config.MAX_RETRIES:
            wait_queue = config.RETRY_QUEUES[retries]
            log.error("Pedido %s falló (%s). Reintento %d en %d s", order_id[:8], exc, retries + 1,
                      config.RETRY_DELAYS[retries])
            # con confirm_delivery, basic_publish espera a que el broker guarde la copia (si no, lanza una
            # excepción y el original queda sin ack): nunca se confirma el original sin tener la copia a salvo
            ch.basic_publish("", wait_queue, body, pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                headers={**headers, "retries": retries + 1, "x-last-error": str(exc),
                         "x-first-failure": headers.get("x-first-failure") or datetime.now(timezone.utc).isoformat()},
            ), mandatory=True)
            ch.basic_ack(method.delivery_tag)
        else:
            log.error("Pedido %s agotó sus %d reintentos -> DLQ", order_id[:8], config.MAX_RETRIES)
            self.service.mark_failed(order_id)   # si la BD está caída lanza y el mensaje vuelve a la cola
            ch.basic_nack(method.delivery_tag, requeue=False)  # RabbitMQ lo envía a la DLQ (con cabecera x-death)


class EmailConsumer:
    """Consumidor del mailer, con las mismas garantías que OrderConsumer: ack manual tras enviar, reintentos con espera
    creciente (2 s, 5 s, 10 s) ante fallos de SMTP o red, DLQ al tercer fallo y reconexión automática."""

    def __init__(self, service):
        self.service = service

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            conn = None
            try:
                conn = connect(retries=1)
                ch = conn.channel()
                declare_topology(ch)
                ch.basic_qos(prefetch_count=1)
                ch.confirm_delivery()   # la copia que va a la cola de reintento también debe confirmarla el broker
                ch.basic_consume(config.EMAIL_QUEUE, on_message_callback=self._on_message, auto_ack=False)
                log.info("Escuchando '%s'", config.EMAIL_QUEUE)
                while not stop.is_set():
                    conn.process_data_events(time_limit=0.5)
            except (*_NET_ERRORS, ConnectionError) as exc:
                log.warning("Conexión con RabbitMQ perdida (%s). Reintentando...", exc.__class__.__name__)
                stop.wait(2)
            except Exception:
                log.exception("Error inesperado en el mailer. Reconectando...")
                stop.wait(2)
            finally:
                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass

    def _on_message(self, ch, method, props, body) -> None:
        try:
            payload = json.loads(body)
            email_id = str(payload["email_id"])
        except (ValueError, KeyError, TypeError):
            log.error("Mensaje de correo ilegible -> DLQ")
            ch.basic_nack(method.delivery_tag, requeue=False)
            return
        headers = props.headers or {}
        retries = headers.get("retries", 0)
        try:
            try:
                self.service.deliver(payload)
            except EmailSendError as exc:            # SMTP o red: reintento diferido y luego DLQ
                self._retry_or_dead_letter(ch, method, body, headers, retries, email_id, exc)
                return
            except (ValueError, KeyError, TypeError) as exc:   # evento mal formado: reintentar no lo arregla
                log.error("Correo %s inválido (%s) -> DLQ", email_id[:8], exc.__class__.__name__)
                ch.basic_nack(method.delivery_tag, requeue=False)
                return
            ch.basic_ack(method.delivery_tag)        # solo tras enviar y registrar el email_id
        except _NET_ERRORS:
            raise
        except Exception as exc:
            # infraestructura (p. ej. la BD de idempotencia caída): no gasta reintentos, vuelve a la cola y espera
            log.warning("Infraestructura no disponible con el correo %s: %s. Devuelto a la cola.",
                        email_id[:8], exc.__class__.__name__)
            time.sleep(2)
            ch.basic_nack(method.delivery_tag, requeue=True)

    def _retry_or_dead_letter(self, ch, method, body, headers, retries, email_id, exc) -> None:
        if retries < config.MAX_RETRIES:
            log.error("Correo %s falló (%s). Reintento %d en %d s", email_id[:8], exc, retries + 1,
                      config.RETRY_DELAYS[retries])
            ch.basic_publish("", config.EMAIL_RETRY_QUEUES[retries], body, pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                headers={**headers, "retries": retries + 1, "x-last-error": str(exc),
                         "x-first-failure": headers.get("x-first-failure") or datetime.now(timezone.utc).isoformat()},
            ), mandatory=True)
            ch.basic_ack(method.delivery_tag)
        else:
            log.error("Correo %s agotó sus %d reintentos -> DLQ", email_id[:8], config.MAX_RETRIES)
            ch.basic_nack(method.delivery_tag, requeue=False)


class PagoConsumer:
    """Consumidor de la cola "pagos" (notificaciones de Yape), con las mismas garantías que OrderConsumer: ack manual
    tras guardar el resultado, reintentos 2 s / 5 s / 10 s ante errores de negocio, DLQ al tercer fallo, errores de
    infraestructura (BD caída) de vuelta a la cola sin gastar reintentos, y reconexión automática."""

    def __init__(self, service):
        self.service = service

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            conn = None
            try:
                conn = connect(retries=1)
                ch = conn.channel()
                declare_topology(ch)
                ch.basic_qos(prefetch_count=1)
                ch.confirm_delivery()   # la copia que va a la cola de reintento también debe confirmarla el broker
                ch.basic_consume(config.PAGOS_QUEUE, on_message_callback=self._on_message, auto_ack=False)
                log.info("Escuchando '%s'", config.PAGOS_QUEUE)
                while not stop.is_set():
                    conn.process_data_events(time_limit=0.5)
            except (*_NET_ERRORS, ConnectionError) as exc:
                log.warning("Conexión con RabbitMQ perdida (%s). Reintentando...", exc.__class__.__name__)
                stop.wait(2)
            except Exception:
                log.exception("Error inesperado en el consumidor de pagos. Reconectando...")
                stop.wait(2)
            finally:
                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass

    def _on_message(self, ch, method, props, body) -> None:
        try:
            notificacion_id = int(json.loads(body)["notificacion_id"])
        except (ValueError, KeyError, TypeError):
            log.error("Mensaje de pago ilegible -> DLQ")
            ch.basic_nack(method.delivery_tag, requeue=False)
            return
        headers = props.headers or {}
        retries = headers.get("retries", 0)
        try:
            try:
                estado = self.service.procesar(notificacion_id)
            except ProcessingError as exc:      # error de negocio: reintento diferido y luego DLQ
                self._retry_or_dead_letter(ch, method, body, headers, retries, notificacion_id, exc)
                return
            log.info("Notificación Yape %s: %s", notificacion_id, estado)
            ch.basic_ack(method.delivery_tag)   # solo tras guardar el resultado en la BD
        except _NET_ERRORS:
            raise
        except Exception as exc:
            # infraestructura (p. ej. PostgreSQL caído): el pago no tiene la culpa; vuelve a la cola, nunca a la DLQ
            log.warning("Infraestructura no disponible con la notificación %s: %s. Devuelta a la cola.",
                        notificacion_id, exc.__class__.__name__)
            time.sleep(2)
            ch.basic_nack(method.delivery_tag, requeue=True)

    def _retry_or_dead_letter(self, ch, method, body, headers, retries, notificacion_id, exc) -> None:
        if retries < config.MAX_RETRIES:
            log.error("Notificación %s falló (%s). Reintento %d en %d s", notificacion_id, exc, retries + 1,
                      config.RETRY_DELAYS[retries])
            ch.basic_publish("", config.PAGOS_RETRY_QUEUES[retries], body, pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                headers={**headers, "retries": retries + 1, "x-last-error": str(exc),
                         "x-first-failure": headers.get("x-first-failure") or datetime.now(timezone.utc).isoformat()},
            ), mandatory=True)
            ch.basic_ack(method.delivery_tag)
        else:
            log.error("Notificación %s agotó sus %d reintentos -> DLQ", notificacion_id, config.MAX_RETRIES)
            ch.basic_nack(method.delivery_tag, requeue=False)


class DeadLetterInspector:
    """Revisa, reprocesa o descarta los mensajes de una DLQ sin perderlos en el camino. Por defecto la de pedidos;
    con email=True, la de correos (los mensajes se identifican por email_id en vez de order_id); con pagos=True, la de
    pagos Yape (por notificacion_id)."""

    def __init__(self, email: bool = False, pagos: bool = False):
        self.email = email
        if pagos:
            self.queue, self.exchange = config.PAGOS_DLQ, config.PAGOS_EXCHANGE
            self.routing_key, self.id_field = config.PAGOS_ROUTING_KEY, "notificacion_id"
            return
        self.queue = config.EMAIL_DLQ if email else config.DLQ
        self.exchange = config.EMAIL_EXCHANGE if email else config.EXCHANGE
        self.routing_key = config.EMAIL_ROUTING_KEY if email else config.ROUTING_KEY
        self.id_field = "email_id" if email else "order_id"

    def _open(self):
        conn = connect(retries=1, heartbeat=0)
        ch = conn.channel()
        declare_topology(ch)
        return conn, ch

    def _id(self, body) -> str | None:
        try:
            return json.loads(body).get(self.id_field)
        except (ValueError, AttributeError):
            return None

    def peek(self, limit: int = 50) -> list[dict]:
        """Lee los mensajes sin confirmarlos; al cerrar el canal RabbitMQ los devuelve intactos a la DLQ."""
        conn, ch = self._open()
        items = []
        try:
            for _ in range(limit):
                method, props, body = ch.basic_get(self.queue, auto_ack=False)
                if method is None:
                    break
                headers = props.headers or {}
                death = (headers.get("x-death") or [{}])[0]
                died_at = death.get("time")
                extra = {}
                if self.email:   # tipo y destinatario; nunca los datos (los enlaces llevan tokens)
                    try:
                        p = json.loads(body)
                        extra = {"kind": p.get("kind"), "to": p.get("to")}
                    except ValueError:
                        pass
                items.append({
                    self.id_field: self._id(body),
                    **extra,
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
                method, props, body = ch.basic_get(self.queue, auto_ack=False)
                if method is None:
                    break
                order_id = self._id(body)
                if order_ids is None or order_id in order_ids:
                    action(ch, body)
                    ch.basic_ack(method.delivery_tag)
                    done.append(order_id)
        finally:
            conn.close()   # los no elegidos (sin ack) vuelven a la DLQ
        return done

    def reprocess(self, order_ids: set[str] | None = None) -> list[str]:
        def republish(ch, body):
            ch.basic_publish(self.exchange, self.routing_key, body, pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent, content_type="application/json",
                headers={"retries": 0, "x-reprocessed": True}), mandatory=True)
        return self._take(order_ids, republish)

    def discard(self, order_ids: set[str] | None = None) -> list[str]:
        return self._take(order_ids, lambda ch, body: None)


def _text(value) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)
