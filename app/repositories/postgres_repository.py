"""Repositorios de PostgreSQL: pedidos, outbox y usuarios. La conexión y las tablas están en app/database."""
from __future__ import annotations

from decimal import Decimal
from typing import Callable

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from app.repositories.interfaces import Outbox, OrderRepository, UserRepository
from app.models.order import Order, OrderItem, OrderStatus, money
from app.models.user import Role, User


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
                          WHERE o.status = 'PUBLISHED' AND x.published_at < now() - make_interval(secs => %s)
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

    def relay(self, publish: Callable[[str], None], limit: int, older_than_s: float) -> int:
        sent = 0
        with self.pool.connection() as conn, conn.transaction():
            # FOR UPDATE SKIP LOCKED: si hubiera varios relays, cada uno toma filas distintas (nunca el mismo evento)
            rows = conn.execute("""SELECT id, aggregate_id FROM outbox
                                   WHERE published_at IS NULL AND created_at < now() - make_interval(secs => %s)
                                   ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""", (older_than_s, limit)).fetchall()
            for event_id, order_id in rows:
                try:
                    publish(order_id)
                except Exception:
                    break                      # el broker sigue caído: lo demás espera a la próxima vuelta
                conn.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (event_id,))
                sent += 1
        return sent

    def mark_sent(self, order_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE outbox SET published_at = now() WHERE aggregate_id = %s AND published_at IS NULL",
                         (order_id,))

    def pending_count(self) -> int:
        with self.pool.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]

    def recent(self, limit: int) -> list[dict]:
        with self.pool.connection() as conn:
            rows = conn.execute("""SELECT id, aggregate_id, event_type, created_at, published_at FROM outbox
                                   ORDER BY id DESC LIMIT %s""", (limit,)).fetchall()
        # TIMESTAMPTZ llega como datetime con zona horaria: isoformat() la conserva (p. ej. ...+00:00)
        return [{"id": id_, "aggregate_id": agg, "event_type": ev, "created_at": created.isoformat(),
                 "published_at": published.isoformat() if published else None}
                for id_, agg, ev, created, published in rows]


class PostgresUserRepository(UserRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def get_by_email(self, email: str) -> User | None:
        with self.pool.connection() as conn:
            row = conn.execute("SELECT id, name, email, password_hash, role FROM users WHERE email=%s",
                               (email,)).fetchone()
        return User(row[0], row[1], row[2], row[3], Role(row[4])) if row else None

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
