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
            except Exception:
                self._reset()
                return None
        unacked = min(_mgmt_unacked(), consumers)
        return {"ready": durable + volatile, "unacked": unacked, "consumers": consumers,
                "persistent": durable + unacked, "volatile": volatile, "retrying": retrying, "retry_stages": stages, "dlq": dlq}


def _mgmt_unacked() -> int:
    url = f"http://{config.RABBITMQ_HOST}:{config.RABBITMQ_MGMT_PORT}/api/queues/%2F/{config.QUEUE}"
    token = base64.b64encode(f"{config.RABBITMQ_USER}:{config.RABBITMQ_PASS}".encode()).decode()
    try:
        with urlopen(Request(url, headers={"Authorization": f"Basic {token}"}), timeout=1.0) as r:
            return json.load(r).get("messages_unacknowledged", 0)
    except Exception:
        return 0
