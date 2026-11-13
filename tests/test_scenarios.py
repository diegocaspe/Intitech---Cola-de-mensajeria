"""Pruebas del orquestador de escenarios automáticos con un sistema falso (sin Docker, RabbitMQ ni PostgreSQL).

World imita lo justo: la outbox se publica si hay BD y broker; la cola se procesa si además hay worker; sin persistencia,
tirar el broker borra lo que estaba en memoria; los venenosos (con el modo activo) pasan por 3 reintentos y la DLQ."""
import itertools
import threading
import time

import pytest

from app.services.scenarios import SCENARIOS, DbUnavailable, ScenarioRunner


class Clock:
    def __init__(self):
        self.t = 0.0
        self.hooks = []   # (instante, función): se ejecuta una vez al pasar ese instante

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += max(s, 0.01)
        for at, fn in list(self.hooks):
            if self.t >= at:
                self.hooks.remove((at, fn))
                fn()


class World:
    def __init__(self):
        self.calls = []
        self.orders = {}
        self.ids = (f"o{i}" for i in itertools.count(1))
        self.up = {"worker": True, "rabbitmq": True, "postgres": True}
        self.persist, self.poison = True, False
        self.queue, self.retry, self.dlq = [], [], []

    # --- sabotajes ---
    def recover(self):
        self.calls.append("recover")
        self.up = {k: True for k in self.up}
        self.persist, self.poison = True, False

    def kill(self, svc):
        self.calls.append(f"kill {svc}")
        self.up[svc] = False
        if svc == "rabbitmq":   # lo que vivía solo en memoria se pierde
            self.queue = [i for i in self.queue if self.orders[i]["durable"]]

    def revive(self, svc):
        self.calls.append(f"revive {svc}")
        self.up[svc] = True

    def set_persistent(self, on):
        self.calls.append(f"persist {on}")
        self.persist = on

    def set_poison(self, on):
        self.calls.append(f"poison {on}")
        self.poison = on

    def set_worker_delay(self, s):
        self.calls.append(f"delay {s}")

    # --- pedidos ---
    def create_test_order(self, email, poison=False):
        assert email.endswith("@prueba.intitech")
        if not self.up["postgres"]:
            raise DbUnavailable()
        oid = next(self.ids)
        self.orders[oid] = {"status": "PENDING", "poison": poison, "durable": True, "tries": 0}
        return oid

    def _tick(self):
        db, broker, worker = self.up["postgres"], self.up["rabbitmq"], self.up["worker"]
        if db and broker:                                   # el relay publica la outbox
            for oid, o in self.orders.items():
                if o["status"] == "PENDING":
                    o.update(status="PUBLISHED", durable=self.persist)
                    self.queue.append(oid)
        if broker:                                          # vencen las esperas de reintento
            back = [r for r in self.retry if r[1] <= 1]
            self.retry = [(i, n - 1) for i, n in self.retry if n > 1]
            self.queue += [i for i, _ in back]
        if db and broker and worker:                        # el worker procesa hasta 3 por vuelta
            for oid in self.queue[:3]:
                self.queue.remove(oid)
                o = self.orders[oid]
                if o["poison"] and self.poison:
                    o["tries"] += 1
                    if o["tries"] <= 3:
                        self.retry.append((oid, 2))
                    else:
                        o["status"] = "FAILED"
                        self.dlq.append(oid)
                else:
                    o["status"] = "PROCESSED"

    def snapshot(self):
        self._tick()
        broker = self.up["rabbitmq"]
        mem = sum(1 for i in self.queue if not self.orders[i]["durable"])
        return {"broker_up": broker, "db_up": self.up["postgres"], "worker_up": self.up["worker"],
                "consumers": 1 if self.up["worker"] and broker else 0,
                "depth": len(self.queue) if broker else None, "retrying": len(self.retry) if broker else None,
                "retry_stages": [len(self.retry), 0, 0], "dlq": len(self.dlq) if broker else None,
                "in_disk": len(self.queue) - mem if broker else None, "in_memory": mem if broker else None,
                "outbox": sum(1 for o in self.orders.values() if o["status"] == "PENDING")}

    def statuses(self, ids):
        assert self.up["postgres"], "no se puede leer la BD caída"
        return {i: self.orders[i]["status"] for i in ids}

    def reconcile(self, ids):
        self.calls.append("reconcile")
        lost = [i for i in ids if self.orders[i]["status"] == "PUBLISHED" and i not in self.queue]
        self.queue += lost
        return len(lost)

    def reprocess_dlq(self, ids):
        self.calls.append("reprocess")
        for i in ids:
            self.dlq.remove(i)
            self.orders[i].update(status="PUBLISHED", tries=0)
            self.queue.append(i)
        return len(ids)

    def clean_test_dlq(self):
        self.dlq.clear()
        return 0


def make(world=None):
    clock, world = Clock(), world or World()
    return ScenarioRunner(world, sleep=clock.sleep, clock=clock.now, poll=0.25), world, clock


def subsequence(calls, expected):
    it = iter(calls)
    return all(any(c == e for c in it) for e in expected)


@pytest.mark.parametrize("n", sorted(SCENARIOS))
def test_every_scenario_finishes_balanced_with_all_steps_done_and_recovers(n):
    runner, world, _ = make()
    assert runner.start(n, "rapido", background=False)
    run = runner.state["runs"][n]
    assert run["status"] == "ok", run["error"]
    assert [s["st"] for s in run["steps"]] == ["done"] * len(SCENARIOS[n].steps)
    s = run["summary"]
    assert s["balanced"] and s["sent"] == s["processed"] + s["rejected"] + s["dlq"]
    assert world.calls[0] == "recover" and world.calls[-1] == "recover"          # empieza limpio y termina en pie
    assert all(world.up.values()) and world.persist and not world.poison
    assert not runner.running and run["focus"] is None
    assert runner.state["last"] == n                                              # el panel deja expandida esta


def test_black_friday_sends_the_chosen_amount():
    runner, world, _ = make()
    runner.start(1, "rapido", count=500, background=False)
    assert runner.state["runs"][1]["summary"]["processed"] == 500


def test_persistence_scenario_runs_its_steps_in_order_and_rescues_the_lost_ones():
    runner, world, _ = make()
    runner.start(4, "rapido", background=False)
    assert subsequence(world.calls, ["recover", "persist False", "kill worker", "kill rabbitmq", "revive rabbitmq",
                                     "persist True", "kill rabbitmq", "revive rabbitmq", "reconcile",
                                     "revive worker", "recover"])
    s = runner.state["runs"][4]["summary"]
    assert s["sent"] == s["processed"] == 20
    assert "Sin persistencia se perdieron 10 mensajes en el broker" in s["lines"]
    assert "10 rescatados por reconciliación desde la BD" in s["lines"]


def test_poison_scenario_sends_four_to_the_dlq_then_reprocesses_them():
    runner, world, _ = make()
    runner.start(5, "rapido", background=False)
    assert subsequence(world.calls, ["poison True", "poison False", "reprocess", "recover"])
    assert runner.state["runs"][5]["summary"]["lines"] == ["4 fueron a la DLQ y se completaron al reprocesar"]


def test_db_scenario_rejects_five_with_notice_and_keeps_the_dlq_empty():
    runner, world, _ = make()
    runner.start(6, "rapido", background=False)
    assert subsequence(world.calls, ["kill worker", "kill postgres", "revive worker", "revive postgres"])
    s = runner.state["runs"][6]["summary"]
    assert (s["processed"], s["rejected_503"], s["dlq"]) == (20, 5, 0)


def test_wait_has_a_maximum_time_then_shows_the_error_and_recovers():
    world = World()
    world.revive = lambda svc: world.calls.append(f"revive {svc} (no vuelve)")   # el worker nunca vuelve
    runner, _, clock = make(world)
    runner.start(2, "rapido", background=False)
    run = runner.state["runs"][2]
    assert run["status"] == "error" and run["error"] == "Tiempo máximo (60 s) esperando que el worker vuelva"
    assert [s["st"] for s in run["steps"]].count("error") == 1                      # el paso en curso queda marcado
    assert world.calls[-1] == "recover" and not runner.running
    assert runner.start(1, "rapido", background=False)                              # el candado quedó libre


def test_stop_cuts_the_scenario_and_recovers_everything():
    runner, world, clock = make()
    def press_stop():                                                               # «Detener» con RabbitMQ caído
        assert not world.up["rabbitmq"]
        runner.stop()
    clock.hooks.append((3.0, press_stop))
    runner.start(3, "expo", background=False)
    run = runner.state["runs"][3]
    assert run["status"] == "stopped" and run["summary"] is None
    assert "revive rabbitmq" not in world.calls                                     # el guion no siguió
    assert "error" in [s["st"] for s in run["steps"]] and "pending" in [s["st"] for s in run["steps"]]
    assert world.calls[-1] == "recover" and world.up["rabbitmq"]                   # RabbitMQ quedó en pie


def test_only_one_scenario_at_a_time():
    world, gate = World(), threading.Event()
    original = world.recover
    def slow_recover():
        gate.wait(5)
        original()
    world.recover = slow_recover
    runner = ScenarioRunner(world, poll=0.01)
    runner.pause_s = 0
    assert runner.start(1, "rapido") is True                                       # en segundo plano
    assert runner.start(2, "rapido") is False                                      # el otro se rechaza
    assert runner.stop() is True
    gate.set()
    for _ in range(200):
        if not runner.running:
            break
        time.sleep(0.01)
    assert not runner.running and runner.state["runs"][1]["status"] == "stopped"


def test_rejects_unknown_scenario_speed_or_amount():
    runner, _, _ = make()
    for args in ((7, "rapido"), (1, "lento"), (1, "rapido", 400)):
        with pytest.raises(ValueError):
            runner.start(*args, background=False)
