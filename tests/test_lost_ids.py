"""/api/metrics → lost_ids: qué pedidos perdió RabbitMQ (la pizarra de PostgreSQL los marca en rojo).

Con SQLite y un RabbitMQ falso: sin Docker, PostgreSQL ni broker. La regla es la misma que la de "lost": solo hay
perdidos con la cola vacía, porque RabbitMQ no deja ver qué mensajes tiene sin consumirlos."""
import os
import secrets

import pytest
from psycopg_pool import ConnectionPool

from app import config
from app.database import connection
from app.repositories.sqlite_repository import SqliteOrderRepository, SqliteOutbox, SqliteSentEmails, _ago
from app.services.order_service import OrderService
from app.services.payments import FakePaymentProvider
from tests.test_order_service import ITEMS, FakePublisher

# variables que exige config.require_env("api"); valores de juguete solo si no vienen del entorno
ENV = {"RABBITMQ_USER": "guest", "RABBITMQ_PASS": "guest", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p",
       "POSTGRES_DB": "db", "ADMIN_EMAIL": "jefe@prueba.intitech"}


@pytest.fixture(scope="module")
def api():
    """Importa la API sin abrir conexiones: el pool de PostgreSQL se crea cerrado."""
    with pytest.MonkeyPatch.context() as mp:
        for k, v in ENV.items():
            if not os.getenv(k, "").strip():
                mp.setenv(k, v)
        # aleatorios y largos: así pasan la revisión de secretos débiles de config.weak_secrets
        mp.setenv("SECRET_KEY", secrets.token_hex(32))
        mp.setenv("ADMIN_PASSWORD", secrets.token_hex(12))
        mp.setattr(connection, "create_pool", lambda url, max_size=10: ConnectionPool(url, open=False))
        from app.routes import api as module
        yield module


@pytest.fixture
def world(api, monkeypatch):
    """La API apunta a una BD SQLite en memoria y a un RabbitMQ falso cuya cola decide cada prueba."""
    repo = SqliteOrderRepository(":memory:")
    svc = OrderService(repo, FakePublisher(), SqliteOutbox(repo), payments=FakePaymentProvider(sleep=lambda _: None))
    queue = {"ready": 0, "unacked": 0, "retrying": 0}
    monkeypatch.setattr(api, "service", svc)
    monkeypatch.setattr(api, "sent_emails", SqliteSentEmails(repo))
    monkeypatch.setattr(api.monitor, "stats", lambda: dict(queue))
    monkeypatch.setattr(api, "_services", lambda q: None)          # sin Docker
    monkeypatch.setattr(api.db_watch, "snapshot", dict(api.db_watch.snapshot))
    return api, svc, repo, queue


def _publicados_hace_rato(svc, repo, n):
    """n pedidos que el relay publicó hace más de LOST_AFTER_S y que ningún worker procesó."""
    ids = [svc.create_order(f"c{i}@b.com", ITEMS).id for i in range(n)]
    svc.relay_outbox(min_age_s=0)
    old = _ago(config.LOST_AFTER_S + 60)
    for oid in ids:
        repo._run("UPDATE outbox SET published_at = ? WHERE aggregate_id = ?", (old, oid))
    return ids


def test_con_la_cola_vacia_lost_ids_trae_los_pedidos_perdidos(world):
    api, svc, repo, _ = world
    perdidos = _publicados_hace_rato(svc, repo, 3)
    reciente = svc.create_order("nuevo@b.com", ITEMS).id   # recién publicado: aún no cuenta como perdido
    svc.relay_outbox(min_age_s=0)
    assert api.db_watch._read()

    m = api.metrics({})
    assert m["lost"] == 3
    assert sorted(m["lost_ids"]) == sorted(perdidos)
    assert reciente not in m["lost_ids"]


def test_con_mensajes_en_la_cola_lost_ids_queda_vacio(world):
    api, svc, repo, queue = world
    _publicados_hace_rato(svc, repo, 2)
    queue["ready"] = 2   # RabbitMQ todavía los tiene: no se puede saber cuáles faltan
    assert api.db_watch._read()

    m = api.metrics({})
    assert m["lost"] == 0
    assert m["lost_ids"] == []
