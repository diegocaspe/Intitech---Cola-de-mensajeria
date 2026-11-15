"""Conexión a PostgreSQL: pool de conexiones y creación de las tablas (orders, outbox, users, email_tokens,
sent_emails y products)."""
import logging
import time

import psycopg
from psycopg_pool import ConnectionPool
from psycopg_pool.base import AttemptWithBackoff

from app.database.seed import seed_products

log = logging.getLogger(__name__)

# api, relay, worker y mailer ejecutan el esquema (y la siembra) al arrancar: este candado los pone en fila, uno a la
# vez, dentro de la misma transacción. Se libera solo al hacer commit
_LOCK_SCHEMA = 7_031_926

SCHEMA = f"""
SELECT pg_advisory_xact_lock({_LOCK_SCHEMA});
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    customer_email  TEXT NOT NULL,
    items           JSONB NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('PENDING','PUBLISHED','PROCESSED','FAILED','REJECTED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- migración: las bases creadas antes no aceptaban 'REJECTED'. Solo se toca si hace falta (api, relay y worker
-- ejecutan este esquema al arrancar: así no compiten por bloquear la tabla en cada inicio)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'orders_status_check'
                   AND pg_get_constraintdef(oid) LIKE '%REJECTED%') THEN
        ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check;
        ALTER TABLE orders ADD CONSTRAINT orders_status_check
            CHECK (status IN ('PENDING','PUBLISHED','PROCESSED','FAILED','REJECTED'));
    END IF;
END $$;
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

-- Correos automáticos. Las cuentas que ya existían al agregar esta columna quedan como 'legacy' (de prueba: no reciben
-- correos); las que se registran desde la tienda entran como 'pending' y pasan a 'verified' al abrir el enlace
ALTER TABLE users ADD COLUMN IF NOT EXISTS email_status TEXT NOT NULL DEFAULT 'legacy'
    CHECK (email_status IN ('legacy','pending','verified'));

-- Tokens de un solo uso enviados por correo (verificar cuenta, restablecer contraseña). Solo se guarda su hash SHA-256
CREATE TABLE IF NOT EXISTS email_tokens (
    token_hash  TEXT PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose     TEXT NOT NULL CHECK (purpose IN ('verify','reset')),
    created_at  TIMESTAMPTZ NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    used_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_email_tokens_user ON email_tokens (user_id, purpose, created_at DESC);

-- Idempotencia del mailer: un email_id que ya está aquí no se vuelve a enviar
CREATE TABLE IF NOT EXISTS sent_emails (
    email_id   TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    recipient  TEXT NOT NULL,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- los eventos de correo son pocos: este índice permite contarlos y descontarlos rápido de la numeración del panel
CREATE INDEX IF NOT EXISTS idx_outbox_emails ON outbox (id) WHERE event_type <> 'OrderCreated';

-- La Zona Yape real se eliminó: sus tablas se borran si quedaron de una versión anterior
DROP TABLE IF EXISTS yape_notificaciones;
DROP TABLE IF EXISTS yape_pedidos;

-- Catálogo con el stock real. catalogo.json solo es la semilla (ver seed.py). CHECK: la última red de seguridad, la BD
-- nunca acepta stock negativo aunque un UPDATE se equivoque
CREATE TABLE IF NOT EXISTS products (
    sku             TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    list_price      NUMERIC(10,2) NOT NULL,
    rating          REAL NOT NULL DEFAULT 0,
    reviews         INTEGER NOT NULL DEFAULT 0,
    stock           INTEGER NOT NULL CHECK (stock >= 0),
    image           TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    related         JSONB NOT NULL DEFAULT '[]',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);

-- migración: los productos de la antigua Zona Yape (pago_yape_real = true) salen de la tabla junto con la columna
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'products' AND column_name = 'pago_yape_real') THEN
        DELETE FROM products WHERE pago_yape_real;
        ALTER TABLE products DROP COLUMN pago_yape_real;
    END IF;
END $$;

-- el stock de un pedido se descuenta UNA sola vez: lo hace el worker al confirmarlo
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stock_descontado BOOLEAN NOT NULL DEFAULT false;

-- Pago con tarjeta SIMULADO (app/services/payments.py). Nunca se guarda el número completo ni el CVV: solo el token de
-- prueba, la marca y los últimos 4 dígitos. payment_status NULL = pedido anterior a los pagos con tarjeta
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_token    TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS card_brand       TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS card_last4       TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status   TEXT
    CHECK (payment_status IN ('PENDING','APPROVED','DECLINED','REFUNDED'));
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_reason   TEXT;
-- errores temporales del proveedor en este pedido: el cobro con la tarjeta «error temporal» falla solo el primero
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_failures INTEGER NOT NULL DEFAULT 0;
"""


# Tras una caída, el pool reintenta conectar con espera exponencial (1, 2, 4, 8, 16 s...): cuanto más dura la
# caída, más tarda en notar que la BD volvió. Con la BD en la misma máquina basta reintentar cada 0,5 s, siempre.
AttemptWithBackoff.INITIAL_DELAY = 0.5
AttemptWithBackoff.DELAY_BACKOFF = 1.0


def create_pool(url: str, max_size: int = 10) -> ConnectionPool:
    """El pool reabre conexiones rotas (p. ej. si PostgreSQL se reinicia) y no bloquea el arranque."""
    pool = ConnectionPool(url, min_size=1, max_size=max_size, open=False, timeout=3,
                          check=ConnectionPool.check_connection, kwargs={"connect_timeout": 3})
    pool.open(wait=False)
    return pool


def init_schema(pool: ConnectionPool, attempts: int = 30) -> None:
    for i in range(1, attempts + 1):
        try:
            with pool.connection() as conn:   # esquema + siembra en una transacción, con el candado tomado
                conn.execute(SCHEMA)
                if seeded := seed_products(conn):
                    log.info("Tabla products vacía: sembrados %d productos desde catalogo.json", seeded)
            return
        except psycopg.OperationalError as exc:
            log.warning("PostgreSQL no disponible (%d/%d): %s", i, attempts, exc)
            time.sleep(2)
    raise ConnectionError("No se pudo inicializar la base de datos")
