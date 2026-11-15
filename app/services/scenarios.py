"""Escenarios automáticos de autosabotaje: un botón «Ejecutar» corre toda la secuencia sola, paso a paso, narrando.

El orquestador no sabe de Docker, RabbitMQ ni PostgreSQL: pide todo a un objeto `ops` (ver ScenarioOps), que en la app
reutiliza los mismos sabotajes del panel (matar/revivir, persistencia, venenosos, reconciliar, reprocesar la DLQ) y en
las pruebas es un sistema falso. Reglas comunes:
  - un escenario a la vez (candado);
  - cada uno empieza con un estado limpio y termina con «Recuperar todo», pase lo que pase (error, tiempo máximo o
    «Detener»);
  - cada «esperar hasta que…» tiene un tiempo máximo (60 s sin avanzar);
  - solo usa pedidos de prueba (correo @prueba.intitech) y el cuadre se calcula solo con ellos.
"""
import logging
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Protocol

log = logging.getLogger(__name__)

TEST_DOMAIN = "prueba.intitech"           # correo de los pedidos de prueba: nunca coincide con un cliente
SPEEDS = {"expo": 6.0, "rapido": 2.0}     # segundos que se mantiene cada estado para que el público lo vea
WAIT_MAX_S = 60.0
FINAL = ("PROCESSED", "REJECTED", "FAILED")


class ScenarioError(Exception):
    """El escenario no pudo seguir (tiempo máximo, resultado inesperado): se muestra en la tarjeta."""


class ScenarioStopped(Exception):
    """El presentador pulsó «Detener»."""


class DbUnavailable(Exception):
    """PostgreSQL no aceptó el pedido: es lo que la API responde como HTTP 503 «Servicio no disponible»."""


class ScenarioOps(Protocol):
    def recover(self) -> None:
        """Recuperar todo: levanta lo caído, persistencia encendida, venenosos apagado, velocidad normal, sin simulador."""
    def kill(self, service: str) -> None:
        """service: worker | rabbitmq | postgres."""
    def revive(self, service: str) -> None: ...
    def set_persistent(self, on: bool) -> None: ...
    def set_poison(self, on: bool) -> None: ...
    def set_worker_delay(self, seconds: float) -> None: ...
    def create_test_order(self, email: str, poison: bool = False) -> str:
        """Crea un pedido de prueba como lo haría la API. Lanza DbUnavailable si la BD no responde."""
    def snapshot(self) -> dict:
        """broker_up, db_up, worker_up, consumers, depth (en cola + en proceso), retrying, retry_stages, dlq,
        in_disk, in_memory, outbox (eventos de pedidos sin publicar). Los de la cola son None con el broker caído."""
    def statuses(self, ids: list[str]) -> dict[str, str]: ...
    def reconcile(self, ids: list[str]) -> int:
        """Reenvía a la cola los pedidos de esa lista que se publicaron pero se perdieron en el broker."""
    def reprocess_dlq(self, ids: list[str]) -> int: ...
    def clean_test_dlq(self) -> int:
        """Descarta de la DLQ solo los mensajes de pedidos de prueba."""


@dataclass(frozen=True)
class Scenario:
    n: int
    title: str
    steps: tuple[str, ...]


PREP = "Preparar un estado limpio (todo en pie, cola vacía)"
END = "Resumen y recuperación"
SCENARIOS = {
    1: Scenario(1, "Black Friday", (PREP, "Enviar la ráfaga de pedidos de prueba", "La API responde al instante",
                                    "Esperar a que la cola llegue a 0", END)),
    2: Scenario(2, "Matar al worker", (PREP, "Enviar 20 pedidos", "Matar al worker mientras procesa un pedido",
                                       "Worker caído: la cola espera", "Revivir el worker", "Esperar a que la cola llegue a 0",
                                       END)),
    3: Scenario(3, "Tirar RabbitMQ", (PREP, "Tirar RabbitMQ", "Enviar 50 pedidos", "Los pedidos esperan en la outbox",
                                      "Revivir RabbitMQ", "Esperar outbox 0 y cola 0", END)),
    4: Scenario(4, "Prueba de persistencia", (PREP, "A · Apagar persistencia y matar al worker", "A · Enviar 10 pedidos",
                                              "A · Tirar y revivir RabbitMQ", "A · ¿Qué quedó en la cola?",
                                              "B · Encender persistencia y enviar 10 pedidos", "B · Tirar y revivir RabbitMQ",
                                              "B · ¿Qué quedó en la cola?", "Reconciliar desde la BD y revivir el worker",
                                              "Esperar a que la cola llegue a 0", END)),
    5: Scenario(5, "Pedidos venenosos", (PREP, "Activar venenosos y enviar 20 pedidos", "Reintentos 2 s → 5 s → 10 s",
                                         "Esperar a que la cola llegue a 0", "Desactivar venenosos y reprocesar la DLQ",
                                         "Esperar a que la DLQ llegue a 0", END)),
    6: Scenario(6, "Tirar PostgreSQL", (PREP, "Matar al worker y enviar 20 pedidos", "Tirar PostgreSQL y revivir el worker",
                                        "Intentar 5 pedidos nuevos", "La cola espera sin ir a la DLQ", "Revivir PostgreSQL",
                                        "Esperar a que la cola llegue a 0", END)),
}


class ScenarioRunner:
    def __init__(self, ops: ScenarioOps, events=None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, wait_max: float = WAIT_MAX_S, poll: float = 0.25):
        self.ops, self.events = ops, events
        self.sleep, self.clock, self.wait_max, self.poll = sleep, clock, wait_max, poll
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.state = {"running": None, "last": None, "speed": "expo", "runs": {}}
        self._run: dict = {}
        self._i = -1
        self.ids: list[str] = []

    # --- control ------------------------------------------------------------------------------------------------
    def start(self, n: int, speed: str = "expo", count: int = 300, background: bool = True) -> bool:
        """False si ya corre otro escenario. Con background=False corre en este hilo (pruebas)."""
        if n not in SCENARIOS or speed not in SPEEDS or count not in (300, 500):
            raise ValueError("Escenario, velocidad o cantidad inválidos")
        if not self._lock.acquire(blocking=False):
            return False
        self._stop.clear()
        sc = SCENARIOS[n]
        self._run = {"n": n, "title": sc.title, "status": "running", "narration": "", "focus": None, "summary": None,
                     "error": None, "steps": [{"text": t, "st": "pending"} for t in sc.steps]}
        self.state = {"running": n, "last": n, "speed": speed, "runs": {**self.state["runs"], n: self._run}}
        if background:
            threading.Thread(target=self._execute, args=(n, speed, count), daemon=True, name=f"escenario-{n}").start()
        else:
            self._execute(n, speed, count)
        return True

    def stop(self) -> bool:
        if self.state["running"] is None:
            return False
        self._stop.set()
        return True

    @property
    def running(self) -> bool:
        return self.state["running"] is not None

    def _execute(self, n: int, speed: str, count: int) -> None:
        self.pause_s, self.ids, self._i = SPEEDS[speed], [], -1
        self._event("info", f"Escenario automático {n} · {SCENARIOS[n].title}: inicia")
        try:
            getattr(self, f"_s{n}")(count)
            self._run["status"] = "ok"
            self._event("ok", f"Escenario {n} · {SCENARIOS[n].title}: terminó con el cuadre correcto")
        except ScenarioStopped:
            self._finish_step("error")
            self._run.update(status="stopped", narration="Detenido por el presentador. Se recuperó todo.")
            self._event("warn", f"Escenario {n} detenido: se ejecutó «Recuperar todo»")
        except ScenarioError as exc:
            self._finish_step("error")
            self._run.update(status="error", error=str(exc))
            self._event("bad", f"Escenario {n}: {exc}. Se ejecutó «Recuperar todo»")
        except Exception as exc:   # nunca deja el sistema a medio sabotear por un fallo inesperado
            log.exception("Escenario %s falló", n)
            self._finish_step("error")
            self._run.update(status="error", error=f"Falla inesperada: {exc.__class__.__name__}")
            self._event("bad", f"Escenario {n}: falla inesperada ({exc.__class__.__name__}). Se ejecutó «Recuperar todo»")
        finally:
            try:
                self.ops.recover()
            except Exception:
                log.exception("No se pudo recuperar tras el escenario %s", n)
            self._run["focus"] = None
            self.state = {**self.state, "running": None}
            self._lock.release()

    # --- pasos, narración, pausas y esperas ----------------------------------------------------------------------
    def _event(self, kind: str, text: str) -> None:
        if self.events:
            self.events.add(kind, text, "flask")

    def _finish_step(self, st: str) -> None:
        if 0 <= self._i < len(self._run["steps"]):
            self._run["steps"][self._i]["st"] = st

    def _step(self, narration: str | None = None, focus: str | None = None) -> None:
        self._check()
        self._finish_step("done")
        self._i += 1
        step = self._run["steps"][self._i]
        step["st"] = "active"
        self._run.update(narration=narration or step["text"], focus=focus)

    def _say(self, text: str) -> None:
        self._run["narration"] = text

    def _check(self) -> None:
        if self._stop.is_set():
            raise ScenarioStopped()

    def _pause(self) -> None:
        end = self.clock() + self.pause_s
        while self.clock() < end:
            self._check()
            self.sleep(min(0.1, end - self.clock()))
        self._check()

    def _wait(self, what: str, cond: Callable[[dict], bool], narrate: Callable[[dict], str] | None = None,
              progress: Callable[[dict], object] | None = None) -> dict:
        """Espera hasta cond(snapshot). Si en wait_max segundos no se cumple (o, con progress, no avanza), es un error."""
        deadline, last = self.clock() + self.wait_max, None
        while True:
            self._check()
            snap = self.ops.snapshot()
            try:
                if narrate:
                    self._say(narrate(snap))
                if cond(snap):
                    return snap
            except DbUnavailable:   # la BD está volviendo (o cayéndose): todavía no se cumple, se sigue esperando
                pass
            if progress:
                key = progress(snap)
                if key != last:
                    last, deadline = key, self.clock() + self.wait_max
            if self.clock() >= deadline:
                raise ScenarioError(f"Tiempo máximo ({self.wait_max:.0f} s) esperando {what}")
            self.sleep(self.poll)

    # --- pedidos de prueba y cuadre --------------------------------------------------------------------------------
    def _send(self, n: int, poison_every: int = 0) -> list[str]:
        """n pedidos de prueba. Con poison_every=5, uno al azar de cada bloque de 5 es venenoso."""
        sent = []
        slots: list[bool] = []
        for i in range(n):
            self._check()
            if poison_every and not slots:
                slots = [True] + [False] * (poison_every - 1)
                random.shuffle(slots)
            poison = slots.pop() if poison_every else False
            email = f"escenario{self._run['n']}-{len(self.ids) + 1}@{TEST_DOMAIN}"
            order_id = self.ops.create_test_order(email, poison)
            self.ids.append(order_id)
            sent.append(order_id)
        return sent

    def _tally(self, ids: list[str] | None = None) -> Counter:
        return Counter(self.ops.statuses(ids if ids is not None else self.ids).values())

    def _all_final(self, ids: list[str] | None = None) -> bool:
        ids = ids if ids is not None else self.ids
        t = self._tally(ids)
        return sum(t[s] for s in FINAL) == len(ids)

    def _drained(self, snap: dict) -> bool:
        return snap["depth"] == 0 and snap["retrying"] == 0 and snap["outbox"] == 0

    def _summary(self, started: float, expect: dict[str, int], extra: list[str] | None = None,
                 rejected_503: int = 0) -> None:
        t = self._tally()
        sent, proc, rej, dlq = len(self.ids), t["PROCESSED"], t["REJECTED"], t["FAILED"]
        balanced = sent == proc + rej + dlq
        self._run["summary"] = {"sent": sent, "processed": proc, "rejected": rej, "dlq": dlq,
                                "rejected_503": rejected_503, "balanced": balanced,
                                "elapsed_s": round(self.clock() - started, 1), "lines": extra or []}
        got = {"PROCESSED": proc, "REJECTED": rej, "FAILED": dlq}
        wrong = {k: (v, got[k]) for k, v in expect.items() if got[k] != v}
        if not balanced or wrong:
            raise ScenarioError("El cuadre no cerró: " + ", ".join(f"{k} esperado {e}, obtenido {g}"
                                                                    for k, (e, g) in wrong.items()) if wrong
                                else "El cuadre no cerró: quedan pedidos sin resultado")
        self._finish_step("done")
        self._say("Listo: el sistema quedó recuperado y en pie.")

    # --- preparación común ---------------------------------------------------------------------------------------
    def _prepare(self) -> float:
        self._step("Recuperando todo y esperando a que la cola quede vacía…")
        self.ops.recover()
        self._wait("que todo esté en pie", lambda s: s["broker_up"] and s["db_up"] and s["worker_up"] and s["consumers"])
        self.ops.clean_test_dlq()
        self._wait("que la cola quede vacía", self._drained, lambda s: f"Esperando cola vacía: {s['depth']} en cola")
        return self.clock()

    # --- 1. Black Friday -----------------------------------------------------------------------------------------
    def _s1(self, count: int) -> None:
        started = self._prepare()
        self._step(f"Enviando {count} pedidos de prueba de golpe…", "nApi")
        t0 = self.clock()
        self._send(count)
        ms = (self.clock() - t0) * 1000 / count
        self._step(f"La API respondió al instante ({ms:.0f} ms por pedido); los pedidos esperan en la cola.", "nQ")
        self._pause()
        self._step(focus="nWork")
        rate = {"t": self.clock(), "done": 0, "v": 0.0}

        def narrate(s):
            done = self._tally()["PROCESSED"]
            dt = self.clock() - rate["t"]
            if dt >= 1:
                rate.update(v=(done - rate["done"]) / dt, t=self.clock(), done=done)
            return f"En cola: {s['depth'] or 0} · procesados {done} de {count} · {rate['v']:.1f} pedidos/s"
        self._wait("que la cola llegue a 0", lambda s: self._drained(s) and self._all_final(), narrate,
                   progress=lambda s: s["depth"])
        self._step()
        self._summary(started, {"PROCESSED": count}, [f"Tiempo total: {self.clock() - t0:.1f} s"])

    # --- 2. Matar al worker --------------------------------------------------------------------------------------
    def _s2(self, _count: int) -> None:
        started = self._prepare()
        self._step("Enviando 20 pedidos (el worker va más lento para verlo trabajar)…", "nApi")
        self.ops.set_worker_delay(0.5)
        self._send(20)
        self._step("Esperando a que el worker esté a mitad de un pedido…", "nWork")
        self._wait("que el worker empiece a procesar", lambda s: 1 <= self._tally()["PROCESSED"] < 20)
        self.ops.kill("worker")
        snap = self._wait("que el worker se detenga", lambda s: not s["worker_up"])
        self._step(focus="nWork")
        waiting = 20 - self._tally()["PROCESSED"]
        self._say(f"Worker caído. {waiting} pedidos esperan en la cola; ninguno se perdió.")
        self._pause()
        self._step("Reviviendo el worker…", "nWork")
        self.ops.revive("worker")
        self._wait("que el worker vuelva", lambda s: s["worker_up"] and s["consumers"])
        self._step(focus="nQ")
        self._wait("que la cola llegue a 0", lambda s: self._drained(s) and self._all_final(),
                   lambda s: f"El worker retoma la cola: {s['depth'] or 0} en cola", progress=lambda s: s["depth"])
        self._step()
        self._summary(started, {"PROCESSED": 20}, [f"{waiting} esperaron en la cola con el worker caído"])

    # --- 3. Tirar RabbitMQ ---------------------------------------------------------------------------------------
    def _s3(self, _count: int) -> None:
        started = self._prepare()
        self._step("Tirando RabbitMQ…", "nQ")
        self.ops.kill("rabbitmq")
        self._wait("que RabbitMQ se detenga", lambda s: not s["broker_up"])
        self._step("La tienda sigue vendiendo; los pedidos se guardan en la outbox.", "nDb")
        self._send(50)
        self._step(focus="nDb")
        snap = self._wait("que los 50 eventos estén en la outbox", lambda s: s["outbox"] >= 50)
        self._say(f"outbox: {snap['outbox']} pendientes. RabbitMQ sigue caído y no se perdió ninguno.")
        self._pause()
        self._step("Reviviendo RabbitMQ…", "nQ")
        self.ops.revive("rabbitmq")
        self._wait("que RabbitMQ vuelva", lambda s: s["broker_up"])
        self._step("El relay publica los pendientes…", "nRelay")
        self._wait("outbox 0 y cola 0", lambda s: self._drained(s) and self._all_final(),
                   lambda s: f"outbox: {s['outbox']} pendientes · en cola: {s['depth'] or 0}",
                   progress=lambda s: (s["outbox"], s["depth"]))
        self._step()
        self._summary(started, {"PROCESSED": 50}, ["Los 50 esperaron en la outbox mientras RabbitMQ estuvo caído"])

    # --- 4. Prueba de persistencia -------------------------------------------------------------------------------
    def _crash_and_revive_broker(self) -> dict:
        self.ops.kill("rabbitmq")
        self._wait("que RabbitMQ se detenga", lambda s: not s["broker_up"])
        self._say("RabbitMQ caído… levantándolo.")
        self.ops.revive("rabbitmq")
        self._wait("que RabbitMQ vuelva", lambda s: s["broker_up"])
        return self._wait("leer la cola", lambda s: s["depth"] is not None)

    def _s4(self, _count: int) -> None:
        started = self._prepare()
        self._step("Parte A: persistencia APAGADA y worker detenido (nadie consume la cola)…", "nWork")
        self.ops.set_persistent(False)
        self.ops.kill("worker")
        self._wait("que el worker se detenga", lambda s: not s["worker_up"])
        self._step(focus="nQ")
        lost_ids = self._send(10)
        snap = self._wait("que los 10 lleguen a la cola", lambda s: s["outbox"] == 0 and (s["in_memory"] or 0) >= 10)
        self._say(f"Cola: {snap['in_memory']} en memoria · {snap['in_disk']} en disco.")
        self._pause()
        self._step("Tirando y reviviendo RabbitMQ…", "nQ")
        snap = self._crash_and_revive_broker()
        self._step(focus="nQ")
        lost = 10 - snap["in_memory"]
        self._say(f"Sin persistencia la cola volvió vacía: se perdieron {lost} mensajes.")
        self._pause()
        self._step("Parte B: persistencia ENCENDIDA. Enviando 10 pedidos…", "nQ")
        self.ops.set_persistent(True)
        self._send(10)
        snap = self._wait("que los 10 lleguen a la cola", lambda s: s["outbox"] == 0 and (s["in_disk"] or 0) >= 10)
        self._say(f"Cola: {snap['in_disk']} en disco · {snap['in_memory']} en memoria.")
        self._pause()
        self._step("Tirando y reviviendo RabbitMQ…", "nQ")
        snap = self._crash_and_revive_broker()
        self._step(focus="nQ")
        kept = snap["in_disk"]
        self._say(f"Con persistencia la cola volvió con los {kept} mensajes.")
        self._pause()
        self._step("Reconciliando desde PostgreSQL los pedidos perdidos en la parte A…", "nDb")
        rescued = self.ops.reconcile(lost_ids)
        self._say(f"Reconciliación: {rescued} pedidos reenviados desde la BD. Reviviendo el worker…")
        self.ops.revive("worker")
        self._wait("que el worker vuelva", lambda s: s["worker_up"] and s["consumers"])
        self._step(focus="nWork")
        self._wait("que la cola llegue a 0", lambda s: self._drained(s) and self._all_final(),
                   lambda s: f"Procesando: {s['depth'] or 0} en cola", progress=lambda s: s["depth"])
        self._step()
        self._summary(started, {"PROCESSED": 20}, [f"Sin persistencia se perdieron {lost} mensajes en el broker",
                                                   f"Con persistencia sobrevivieron {kept}",
                                                   f"{rescued} rescatados por reconciliación desde la BD"])

    # --- 5. Pedidos venenosos ------------------------------------------------------------------------------------
    def _s5(self, _count: int) -> None:
        started = self._prepare()
        self._step("Venenosos ACTIVOS: 1 de cada 5 pedidos fallará. Enviando 20…", "nWork")
        self.ops.set_poison(True)
        # más lento, como en el 2, para poder explicarlo en voz alta; «Recuperar todo» al final devuelve la velocidad normal
        self.ops.set_worker_delay(0.6)
        self._send(20, poison_every=5)

        def narrate(s):
            t = self._tally()
            stages = " → ".join(f"{d}: {n}" for d, n in zip(("2 s", "5 s", "10 s"), s.get("retry_stages") or (0, 0, 0)))
            return f"Procesados {t['PROCESSED']} · reintentando {s['retrying'] or 0} ({stages}) · en la DLQ {t['FAILED']}"
        self._step(focus="nRetry")
        self._wait("los primeros reintentos", lambda s: (s["retrying"] or 0) > 0 or self._tally()["FAILED"] > 0, narrate)
        self._say("Un pedido venenoso falló: espera 2 s en la cola de reintento orders.retry.2s y vuelve a intentarse. "
                  "Los demás siguen.")
        self._pause()
        self._wait("que el primero llegue a la DLQ", lambda s: self._tally()["FAILED"] > 0, narrate)
        self._run["focus"] = "nDlq"
        self._say("Agotó sus 3 reintentos (2 s, 5 s, 10 s): RabbitMQ lo apartó a la DLQ. La cola principal no se bloqueó.")
        self._pause()
        self._step(focus="nDlq")
        self._wait("que la cola llegue a 0", lambda s: self._drained(s) and self._all_final(), narrate,
                   progress=lambda s: (s["retrying"], s["depth"], s["dlq"]))
        bad = [i for i, st in self.ops.statuses(self.ids).items() if st == "FAILED"]
        self._say(f"{len(bad)} pedidos venenosos en la DLQ; los otros {20 - len(bad)} siguieron sin esperar.")
        self._pause()
        self._step("Venenosos apagados. Reprocesando la DLQ…", "nDlq")
        self.ops.set_poison(False)
        self.ops.reprocess_dlq(bad)
        self._step(focus="nWork")
        self._wait("que la DLQ llegue a 0", lambda s: self._drained(s) and self._tally()["PROCESSED"] == 20,
                   lambda s: f"Reprocesando: {self._tally()['FAILED']} en la DLQ", progress=lambda s: s["depth"])
        self._step()
        self._summary(started, {"PROCESSED": 20}, [f"{len(bad)} fueron a la DLQ y se completaron al reprocesar"])

    # --- 6. Tirar PostgreSQL -------------------------------------------------------------------------------------
    def _s6(self, _count: int) -> None:
        started = self._prepare()
        self._step("Deteniendo el worker y enviando 20 pedidos (quedan en la cola)…", "nWork")
        self.ops.kill("worker")
        self._wait("que el worker se detenga", lambda s: not s["worker_up"])
        self._send(20)
        self._wait("que los 20 lleguen a la cola", lambda s: s["outbox"] == 0 and (s["depth"] or 0) >= 20)
        self._step("Tirando PostgreSQL y reviviendo el worker…", "nDb")
        self.ops.kill("postgres")
        self._wait("que PostgreSQL se detenga", lambda s: not s["db_up"])
        self.ops.revive("worker")
        self._wait("que el worker vuelva", lambda s: s["worker_up"] and s["consumers"])
        self._step("Intentando 5 pedidos nuevos con la base de datos caída…", "nApi")
        rejected = 0
        for _ in range(5):
            self._check()
            try:
                self.ids.append(self.ops.create_test_order(f"escenario6-nuevo@{TEST_DOMAIN}"))
            except DbUnavailable:
                rejected += 1
                self._say(f"{rejected} rechazados con aviso (503 «Servicio no disponible, intenta en unos segundos.»)")
        if rejected != 5:
            raise ScenarioError(f"Solo {rejected} de 5 pedidos nuevos se rechazaron con la BD caída")
        self._step(focus="nDb")
        before = self.ops.snapshot()
        self._say("Los pedidos nuevos se rechazan con aviso; los 20 de la cola esperan y NO van a la DLQ.")
        self._pause()
        after = self.ops.snapshot()
        if (after["dlq"] or 0) > (before["dlq"] or 0):
            raise ScenarioError("Con la BD caída llegaron mensajes a la DLQ")
        self._step("Reviviendo PostgreSQL…", "nDb")
        self.ops.revive("postgres")
        self._wait("que PostgreSQL vuelva", lambda s: s["db_up"])
        self._step(focus="nWork")
        self._wait("que la cola llegue a 0", lambda s: self._drained(s) and self._all_final(),
                   lambda s: f"El worker retoma la cola: {s['depth'] or 0} en cola", progress=lambda s: s["depth"])
        self._step()
        self._summary(started, {"PROCESSED": 20, "FAILED": 0}, ["0 llegaron a la DLQ con la BD caída"],
                      rejected_503=rejected)
