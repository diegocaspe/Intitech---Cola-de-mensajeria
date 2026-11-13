"""Lectura de estadísticas de las colas para el panel.

El tamaño de las colas y los consumidores se leen en tiempo real con una declaración pasiva por AMQP
(la API HTTP de gestión de RabbitMQ refresca con varios segundos de retraso). De la API HTTP solo se
toma el número de mensajes sin confirmar (en proceso)."""
import base64
import json
import threading
from urllib.request import Request, urlopen

from app import config
from app.messaging.rabbitmq import connect, declare_topology


class QueueMonitor:
    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._ch = None

    def _passive(self, name: str) -> tuple[int, int]:
        if self._conn is None or self._conn.is_closed or self._ch is None or self._ch.is_closed:
            self._conn = connect(retries=1, heartbeat=0)
            self._ch = self._conn.channel()
            declare_topology(self._ch)   # tras un reinicio del broker la cola volátil ya no existe: se recrea
        ok = self._ch.queue_declare(name, passive=True)
        return ok.method.message_count, ok.method.consumer_count

    def _reset(self):
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        self._conn = self._ch = None

    def stats(self) -> dict | None:
        """None si el broker no responde."""
        with self._lock:
            try:
                durable, consumers = self._passive(config.QUEUE)
                volatile, _ = self._passive(config.VOLATILE_QUEUE)
                dlq, _ = self._passive(config.DLQ)
                stages = [self._passive(q)[0] for q in config.RETRY_QUEUES]   # esperando 2 s, 5 s, 10 s
                retrying = sum(stages)
                mail_ready, mail_consumers = self._passive(config.EMAIL_QUEUE)
                mail_retrying = sum(self._passive(q)[0] for q in config.EMAIL_RETRY_QUEUES)
                mail_dlq, _ = self._passive(config.EMAIL_DLQ)
                pay_ready, pay_consumers = self._passive(config.PAGOS_QUEUE)
                pay_retrying = sum(self._passive(q)[0] for q in config.PAGOS_RETRY_QUEUES)
                pay_dlq, _ = self._passive(config.PAGOS_DLQ)
            except Exception:
                self._reset()
                return None
        unacked = min(_mgmt_unacked(), consumers)
        mail_unacked = min(_mgmt_unacked(config.EMAIL_QUEUE), mail_consumers)
        pay_unacked = min(_mgmt_unacked(config.PAGOS_QUEUE), pay_consumers)
        return {"ready": durable + volatile, "unacked": unacked, "consumers": consumers,
                "persistent": durable + unacked, "volatile": volatile, "retrying": retrying, "retry_stages": stages, "dlq": dlq,
                # correos: esperando en la cola (incluye el que se está enviando), en reintento y en su DLQ
                "emails": {"queued": mail_ready + mail_unacked, "retrying": mail_retrying, "dlq": mail_dlq,
                           "consumers": mail_consumers},
                # pagos Yape: en la cola "pagos" (incluye el que se procesa), en reintento y en "pagos.dlq"
                "pagos": {"queued": pay_ready + pay_unacked, "unacked": pay_unacked, "retrying": pay_retrying,
                          "dlq": pay_dlq, "consumers": pay_consumers}}


def _mgmt_unacked(queue: str = config.QUEUE) -> int:
    url = (f"http://{config.RABBITMQ_HOST}:{config.RABBITMQ_MGMT_PORT}{config.RABBITMQ_MGMT_PATH_PREFIX}"
           f"/api/queues/%2F/{queue}")
    token = base64.b64encode(f"{config.RABBITMQ_MGMT_USER}:{config.RABBITMQ_MGMT_PASS}".encode()).decode()
    try:
        with urlopen(Request(url, headers={"Authorization": f"Basic {token}"}), timeout=1.0) as r:
            return json.load(r).get("messages_unacknowledged", 0)
    except Exception:
        return 0
