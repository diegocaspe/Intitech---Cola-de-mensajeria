"""Experimento de persistencia: mide con números qué pasa con los mensajes si el broker cae.

Usa sus propias colas (lab.persistente / lab.volatil), así que no toca los pedidos reales:
  1. publica N mensajes persistentes en una cola durable y N transitorios en una cola no durable,
     midiendo la velocidad de cada modo (con publisher confirms);
  2. mata RabbitMQ (SIGKILL) y lo vuelve a levantar;
  3. cuenta cuántos mensajes sobrevivieron en cada cola."""
import threading
import time

import pika

from app import config
from app.messaging.rabbitmq import connect

MODES = (
    ("Con persistencia", config.LAB_DURABLE_QUEUE, True),
    ("Sin persistencia", config.LAB_VOLATILE_QUEUE, False),
)


class PersistenceLab:
    def __init__(self, docker, events):
        self.docker = docker
        self.events = events
        self.state = {"running": False, "step": "", "results": None, "finished_at": None, "error": None}

    def start(self) -> bool:
        if self.state["running"]:
            return False
        self.state = {**self.state, "running": True, "step": "Preparando", "error": None}
        threading.Thread(target=self._run, daemon=True).start()
        return True

    def _step(self, text: str) -> None:
        self.state = {**self.state, "step": text}

    def _run(self) -> None:
        n = config.LAB_MESSAGES
        try:
            self.events.add("info", f"Experimento iniciado: {n} mensajes con persistencia y {n} sin persistencia", "flask")
            results = []
            conn = connect(retries=3, heartbeat=0)
            ch = conn.channel()
            ch.confirm_delivery()
            for label, queue, durable in MODES:
                ch.queue_declare(queue, durable=durable)
                ch.queue_purge(queue)
                self._step(f"Publicando {n} mensajes · {label.lower()}")
                props = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent if durable
                                             else pika.DeliveryMode.Transient)
                t0 = time.perf_counter()
                for i in range(n):
                    ch.basic_publish("", queue, f'{{"lab": {i}}}'.encode(), props)  # cada uno espera su confirmación
                elapsed = time.perf_counter() - t0
                results.append({"mode": label, "durable": durable, "sent": n, "rate": round(n / elapsed)})
            conn.close()

            self._step("Tirando RabbitMQ (SIGKILL)")
            self.docker.kill("rabbitmq")
            time.sleep(2)
            self._step("Levantando RabbitMQ")
            self.docker.start("rabbitmq")
            conn = self._wait_broker()

            self._step("Contando mensajes que sobrevivieron")
            ch = conn.channel()
            for r, (_, queue, _) in zip(results, MODES):
                try:
                    r["survived"] = ch.queue_declare(queue, passive=True).method.message_count
                except pika.exceptions.ChannelClosedByBroker:   # la cola no durable ya ni existe
                    r["survived"] = 0
                    ch = conn.channel()
                r["lost"] = r["sent"] - r["survived"]
            for _, queue, _ in MODES:                            # limpieza
                ch.queue_delete(queue)
            conn.close()

            self.state = {**self.state, "results": results, "finished_at": time.time()}
            d, v = results
            self.events.add("ok" if d["lost"] == 0 else "bad",
                            f"Experimento: con persistencia se perdieron {d['lost']} de {n}; "
                            f"sin persistencia se perdieron {v['lost']} de {n}", "flask")
        except Exception as exc:
            self.state = {**self.state, "error": f"{exc.__class__.__name__}: {exc}"}
            self.events.add("bad", f"El experimento falló: {exc.__class__.__name__}", "flask")
        finally:
            self.state = {**self.state, "running": False, "step": ""}

    def _wait_broker(self, timeout: float = 90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                return connect(retries=1, heartbeat=0)
            except ConnectionError:
                time.sleep(2)
        raise TimeoutError("RabbitMQ no volvió a tiempo")
