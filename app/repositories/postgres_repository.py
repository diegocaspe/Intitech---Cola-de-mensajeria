"""Repositorios de PostgreSQL: pedidos, outbox y usuarios. La conexión y las tablas están en app/database."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Callable

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from app.repositories.interfaces import Outbox, OrderRepository, SentEmailRepository, UserRepository
from app.models.email import EmailEvent, EmailToken, TokenPurpose, TokenResult
from app.models.order import Order, OrderItem, OrderStatus, money
from app.models.user import EmailStatus, Role, User


def _insert_email(conn, event: EmailEvent) -> None:
    """Evento de correo en el outbox, dentro de la transacción de quien llama. aggregate_id = email_id: nunca
    coincide con un pedido, así las consultas de pedidos que cruzan outbox con orders no lo ven."""
    conn.execute("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, %s, %s)",
                 (event.email_id, event.kind.value, Jsonb(event.to_payload())))


# Número de un evento de pedido en la fila del panel (1, 2, 3...): su id menos los eventos de correo intercalados.
# %s = id del primer evento de pedido
_ORDER_NUMBER = """x.id - %s + 1 - (SELECT COUNT(*) FROM outbox e
                                     WHERE e.event_type <> 'OrderCreated' AND e.id < x.id AND e.id > %s)"""


class PostgresOrderRepository(OrderRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def _q(self, sql: str, args: tuple = ()) -> list[tuple]:
        # cada operación es una transacción: commit al salir, rollback si hay error
        with self.pool.connection() as conn:
            cur = conn.execute(sql, args)
            return cur.fetchall() if cur.description else []

    def save(self, order: Order) -> None:
        with self.pool.connection() as conn, conn.transaction():   # pedido + evento: todo o nada
            conn.execute("INSERT INTO orders (id, customer_email, items, status, created_at) VALUES (%s,%s,%s,%s,%s)",
                         (order.id, order.customer_email, Jsonb([i.to_dict() for i in order.items]),
                          order.status.value, order.created_at))
            conn.execute("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, 'OrderCreated', %s)",
                         (order.id, Jsonb({"order_id": order.id})))

    def get(self, order_id: str) -> Order | None:
        rows = self._q("SELECT * FROM orders WHERE id=%s", (order_id,))
        return self._to_order(rows[0]) if rows else None

    def list(self, status: OrderStatus | None = None, limit: int | None = None,
             older_than_s: float = 0) -> list[Order]:
        where, args = [], []
        if status:
            where.append("status=%s"); args.append(status.value)
        if older_than_s:
            where.append("created_at < now() - make_interval(secs => %s)"); args.append(older_than_s)
        sql = "SELECT * FROM orders" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY created_at"
        if limit:
            sql += " LIMIT %s"; args.append(int(limit))
        return [self._to_order(r) for r in self._q(sql, tuple(args))]

    def recent(self, limit: int) -> list[Order]:
        return [self._to_order(r) for r in self._q("SELECT * FROM orders ORDER BY created_at DESC LIMIT %s", (limit,))]

    def list_by_email(self, email: str, limit: int) -> list[Order]:
        rows = self._q("SELECT * FROM orders WHERE customer_email=%s ORDER BY created_at DESC LIMIT %s", (email, limit))
        return [self._to_order(r) for r in rows]

    def update_status(self, order_id: str, status: OrderStatus) -> None:
        self._q("UPDATE orders SET status=%s WHERE id=%s", (status.value, order_id))

    def mark_processed(self, order_id: str, receipt: EmailEvent | None = None) -> bool:
        with self.pool.connection() as conn, conn.transaction():   # estado + comprobante: todo o nada
            done = conn.execute("UPDATE orders SET status='PROCESSED' WHERE id=%s AND status<>'PROCESSED' RETURNING id",
                                (order_id,)).fetchone()
            if done and receipt:
                _insert_email(conn, receipt)
        return done is not None

    def mark_published(self, order_id: str) -> None:
        self._q("UPDATE orders SET status='PUBLISHED' WHERE id=%s AND status='PENDING'", (order_id,))

    def count_by_status(self) -> dict[str, int]:
        counts = {s.value: 0 for s in OrderStatus}
        for status, n in self._q("SELECT status, COUNT(*) FROM orders GROUP BY status"):
            counts[status] = n
        return counts

    def revenue(self, status: OrderStatus) -> Decimal:
        rows = self._q("""SELECT COALESCE(SUM((e->>'quantity')::int * (e->>'unit_price')::numeric), 0)
                          FROM orders, jsonb_array_elements(items) e WHERE status=%s""", (status.value,))
        return money(rows[0][0])   # NUMERIC llega como Decimal: suma exacta en la BD

    def count_ahead(self, order: Order) -> int:
        return self._q("SELECT COUNT(*) FROM orders WHERE status IN ('PENDING','PUBLISHED') AND created_at < %s",
                       (order.created_at,))[0][0]

    def count_since(self, seconds: float) -> int:
        return self._q("SELECT COUNT(*) FROM orders WHERE created_at >= now() - make_interval(secs => %s)",
                       (seconds,))[0][0]

    def stale_published(self, older_than_s: float, limit: int = 1000) -> list[str]:
        rows = self._q("""SELECT o.id FROM orders o JOIN outbox x ON x.aggregate_id = o.id
                          WHERE o.status = 'PUBLISHED' AND x.event_type = 'OrderCreated'
                            AND x.published_at < now() - make_interval(secs => %s)
                          ORDER BY x.published_at LIMIT %s""", (older_than_s, limit))
        return [r[0] for r in rows]

    def clear(self) -> None:
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("DELETE FROM outbox")
            conn.execute("DELETE FROM orders")

    @staticmethod
    def _to_order(row) -> Order:
        id_, email, items, status, created = row
        return Order(email, [OrderItem(**i) for i in items], id=id_, status=OrderStatus(status),
                     created_at=created.isoformat())


class PostgresOutbox(Outbox):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def relay(self, publish: Callable[[str, str, dict], None], limit: int, older_than_s: float) -> int:
        sent = 0
        with self.pool.connection() as conn, conn.transaction():
            # FOR UPDATE SKIP LOCKED: si hubiera varios relays, cada uno toma filas distintas (nunca el mismo evento)
            rows = conn.execute("""SELECT id, aggregate_id, event_type, payload FROM outbox
                                   WHERE published_at IS NULL AND created_at < now() - make_interval(secs => %s)
                                   ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""", (older_than_s, limit)).fetchall()
            for event_id, aggregate_id, event_type, payload in rows:
                try:
                    publish(event_type, aggregate_id, payload)
                except Exception:
                    break                      # el broker sigue caído: lo demás espera a la próxima vuelta
                conn.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (event_id,))
                sent += 1
        return sent

    def add_event(self, event: EmailEvent) -> None:
        with self.pool.connection() as conn:
            _insert_email(conn, event)

    def pending_email_count(self) -> int:
        with self.pool.connection() as conn:
            return conn.execute("""SELECT COUNT(*) FROM outbox
                                   WHERE published_at IS NULL AND event_type <> 'OrderCreated'""").fetchone()[0]

    def mark_sent(self, order_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE outbox SET published_at = now() WHERE aggregate_id = %s AND published_at IS NULL",
                         (order_id,))

    def pending_count(self) -> int:
        with self.pool.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND event_type = 'OrderCreated'"
                                ).fetchone()[0]

    def _first_order_event(self, conn) -> int:
        return conn.execute("SELECT MIN(id) FROM outbox WHERE event_type = 'OrderCreated'").fetchone()[0] or 1

    def recent(self, limit: int) -> list[dict]:
        with self.pool.connection() as conn:
            first = self._first_order_event(conn)
            rows = conn.execute(f"""SELECT x.id, x.aggregate_id, x.event_type, x.created_at, x.published_at,
                                           {_ORDER_NUMBER} FROM outbox x WHERE x.event_type = 'OrderCreated'
                                    ORDER BY x.id DESC LIMIT %s""", (first, first, limit)).fetchall()
        # TIMESTAMPTZ llega como datetime con zona horaria: isoformat() la conserva (p. ej. ...+00:00)
        return [{"id": id_, "aggregate_id": agg, "event_type": ev, "created_at": created.isoformat(),
                 "published_at": published.isoformat() if published else None, "number": number}
                for id_, agg, ev, created, published, number in rows]

    def published_stats(self, window_s: float) -> tuple[int, int]:
        with self.pool.connection() as conn:
            return conn.execute("""SELECT COUNT(published_at),
                                          COUNT(*) FILTER (WHERE published_at >= now() - make_interval(secs => %s))
                                   FROM outbox WHERE event_type = 'OrderCreated'""", (window_s,)).fetchone()

    def queue_window(self, edge: int) -> dict:
        waiting = """FROM orders o JOIN outbox x ON x.aggregate_id = o.id
                     WHERE o.status = 'PUBLISHED' AND x.event_type = 'OrderCreated' AND x.published_at IS NOT NULL"""
        with self.pool.connection() as conn:
            count = conn.execute("SELECT COUNT(*) " + waiting).fetchone()[0]
            first = self._first_order_event(conn)
            cols = f"SELECT o.id, {_ORDER_NUMBER} {waiting}"
            head = conn.execute(f"{cols} ORDER BY x.published_at, x.id LIMIT %s", (first, first, edge)).fetchall()
            tail = conn.execute(f"{cols} ORDER BY x.published_at DESC, x.id DESC LIMIT %s", (first, first, edge)).fetchall()
        return {"count": count, "head": [r[0] for r in head], "tail": [r[0] for r in reversed(tail)], "first_event": first,
                "numbers": {order_id: number for order_id, number in head + tail}}


class PostgresUserRepository(UserRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def get_by_email(self, email: str) -> User | None:
        with self.pool.connection() as conn:
            row = conn.execute("SELECT id, name, email, password_hash, role, email_status FROM users WHERE email=%s",
                               (email,)).fetchone()
        return User(row[0], row[1], row[2], row[3], Role(row[4]), EmailStatus(row[5])) if row else None

    @staticmethod
    def _insert_token(conn, user_id: int, token: EmailToken) -> None:
        conn.execute("INSERT INTO email_tokens (token_hash, user_id, purpose, created_at, expires_at) "
                     "VALUES (%s,%s,%s,%s,%s)",
                     (token.token_hash, user_id, token.purpose.value, token.created_at, token.expires_at))

    def add_customer_with_email(self, name: str, email: str, password_hash: str, token: EmailToken,
                                event: EmailEvent) -> User:
        try:
            with self.pool.connection() as conn, conn.transaction():   # usuario + token + correo: todo o nada
                uid = conn.execute("INSERT INTO users (name, email, password_hash, role, email_status) "
                                   "VALUES (%s,%s,%s,'customer','pending') RETURNING id",
                                   (name, email, password_hash)).fetchone()[0]
                self._insert_token(conn, uid, token)
                _insert_email(conn, event)
        except psycopg.errors.UniqueViolation:
            raise ValueError("correo duplicado")
        return User(uid, name, email, password_hash, Role.CUSTOMER, EmailStatus.PENDING)

    def add_token_with_email(self, user_id: int, token: EmailToken, event: EmailEvent) -> None:
        with self.pool.connection() as conn, conn.transaction():
            self._insert_token(conn, user_id, token)
            _insert_email(conn, event)

    def last_token_at(self, user_id: int, purpose: TokenPurpose) -> datetime | None:
        with self.pool.connection() as conn:
            return conn.execute("SELECT MAX(created_at) FROM email_tokens WHERE user_id=%s AND purpose=%s",
                                (user_id, purpose.value)).fetchone()[0]

    def consume_token(self, token_hash: str, purpose: TokenPurpose, now: datetime,
                      new_password_hash: str | None = None) -> TokenResult:
        with self.pool.connection() as conn, conn.transaction():
            # FOR UPDATE: dos clics simultáneos en el mismo enlace no pueden gastarlo dos veces
            row = conn.execute("SELECT user_id, expires_at, used_at FROM email_tokens "
                               "WHERE token_hash=%s AND purpose=%s FOR UPDATE", (token_hash, purpose.value)).fetchone()
            if row is None:
                return TokenResult.INVALID
            user_id, expires_at, used_at = row
            if used_at is not None:
                return TokenResult.USED
            if expires_at <= now:
                return TokenResult.EXPIRED
            conn.execute("UPDATE email_tokens SET used_at=%s WHERE token_hash=%s", (now, token_hash))
            if purpose == TokenPurpose.RESET:
                # quien recibe el enlace en su bandeja también demuestra que el correo es suyo
                conn.execute("""UPDATE users SET password_hash=%s,
                                    email_status = CASE WHEN email_status='pending' THEN 'verified' ELSE email_status END
                                WHERE id=%s""", (new_password_hash, user_id))
                conn.execute("UPDATE email_tokens SET used_at=%s WHERE user_id=%s AND purpose='reset' AND used_at IS NULL",
                             (now, user_id))
            else:
                conn.execute("UPDATE users SET email_status='verified' WHERE id=%s AND email_status='pending'", (user_id,))
        return TokenResult.OK

    def add(self, name: str, email: str, password_hash: str, role: Role) -> User:
        try:
            with self.pool.connection() as conn:
                uid = conn.execute("INSERT INTO users (name, email, password_hash, role) VALUES (%s,%s,%s,%s) "
                                   "RETURNING id", (name, email, password_hash, role.value)).fetchone()[0]
        except psycopg.errors.UniqueViolation:
            raise ValueError("correo duplicado")
        return User(uid, name, email, password_hash, role)

    def update_credentials(self, email: str, password_hash: str, role: Role) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE users SET password_hash=%s, role=%s WHERE email=%s", (password_hash, role.value, email))


class PostgresSentEmails(SentEmailRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def was_sent(self, email_id: str) -> bool:
        with self.pool.connection() as conn:
            return conn.execute("SELECT 1 FROM sent_emails WHERE email_id=%s", (email_id,)).fetchone() is not None

    def mark_sent(self, email_id: str, kind: str, to: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("INSERT INTO sent_emails (email_id, kind, recipient) VALUES (%s,%s,%s) "
                         "ON CONFLICT (email_id) DO NOTHING", (email_id, kind, to))

    def count(self) -> int:
        with self.pool.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM sent_emails").fetchone()[0]
