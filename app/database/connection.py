"""Conexión a PostgreSQL: pool de conexiones y creación de las tablas (orders, outbox y users)."""
import logging
import time

import psycopg
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    customer_email  TEXT NOT NULL,
    items           JSONB NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('PENDING','PUBLISHED','PROCESSED','FAILED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_email  ON orders (customer_email, created_at DESC);

-- Transactional Outbox: cada pedido guarda su evento en la MISMA transacción; un relay lo publica después
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending   ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate ON outbox (aggregate_id);
-- migración: pedidos antiguos que quedaron sin publicar reciben su evento
INSERT INTO outbox (aggregate_id, event_type, payload)
SELECT o.id, 'OrderCreated', jsonb_build_object('order_id', o.id) FROM orders o
WHERE o.status = 'PENDING' AND NOT EXISTS (SELECT 1 FROM outbox x WHERE x.aggregate_id = o.id);

CREATE TABLE IF NOT EXISTS users (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    email          TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL DEFAULT 'customer' CHECK (role IN ('customer','admin')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def create_pool(url: str, max_size: int = 10) -> ConnectionPool:
    """El pool reabre conexiones rotas (p. ej. si PostgreSQL se reinicia) y no bloquea el arranque."""
    pool = ConnectionPool(url, min_size=1, max_size=max_size, open=False, timeout=3,
                          check=ConnectionPool.check_connection, kwargs={"connect_timeout": 3})
    pool.open(wait=False)
    return pool


def init_schema(pool: ConnectionPool, attempts: int = 30) -> None:
    for i in range(1, attempts + 1):
        try:
            with pool.connection() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as exc:
            log.warning("PostgreSQL no disponible (%d/%d): %s", i, attempts, exc)
            time.sleep(2)
    raise ConnectionError("No se pudo inicializar la base de datos")
