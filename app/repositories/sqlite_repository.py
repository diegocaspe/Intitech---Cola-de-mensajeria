from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.repositories.interfaces import Outbox, OrderRepository
from app.models.order import Order, OrderItem, OrderStatus


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ago(seconds: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


class SqliteOrderRepository(OrderRepository):
    """Adaptador alternativo (sin servidor). Lo usan las pruebas unitarias; la app usa PostgreSQL."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._memory_conn = None
        if db_path == ":memory:":
            self._memory_conn = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._run("PRAGMA journal_mode=WAL")
        self._run(
            """CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                customer_email TEXT NOT NULL,
                items TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL)"""
        )
        self._run("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        self._run("""CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
                payload TEXT NOT NULL, created_at TEXT NOT NULL, published_at TEXT)""")

    def _run(self, sql: str, args: tuple = ()):
        """Una conexión por operación: segura entre hilos y entre procesos (API y workers)."""
        if self._memory_conn:
            with self._memory_conn as c:
                return c.execute(sql, args).fetchall()
        with closing(sqlite3.connect(self.db_path, timeout=15)) as c:
            with c:
                return c.execute(sql, args).fetchall()

    def _tx(self, *statements: tuple[str, tuple]) -> None:
        """Varias sentencias en una sola transacción (todo o nada)."""
        if self._memory_conn:
            with self._memory_conn as c:
                for sql, args in statements:
                    c.execute(sql, args)
            return
        with closing(sqlite3.connect(self.db_path, timeout=15)) as c:
            with c:
                for sql, args in statements:
                    c.execute(sql, args)

    def save(self, order: Order) -> None:
        self._tx(
            ("INSERT INTO orders VALUES (?,?,?,?,?)",
             (order.id, order.customer_email, json.dumps([i.__dict__ for i in order.items]),
              order.status.value, order.created_at)),
            ("INSERT INTO outbox (aggregate_id, event_type, payload, created_at) VALUES (?, 'OrderCreated', ?, ?)",
             (order.id, json.dumps({"order_id": order.id}), _now())),
        )

    def get(self, order_id: str) -> Order | None:
        rows = self._run("SELECT * FROM orders WHERE id=?", (order_id,))
        return self._to_order(rows[0]) if rows else None

    def list(self, status: OrderStatus | None = None, limit: int | None = None,
             older_than_s: float = 0) -> list[Order]:
        where, args = [], []
        if status:
            where.append("status=?"); args.append(status.value)
        if older_than_s:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_s)
            where.append("created_at<?"); args.append(cutoff.isoformat())
        sql = "SELECT * FROM orders" + (" WHERE " + " AND ".join(where) if where else "")
        args = tuple(args)
        sql += " ORDER BY created_at"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [self._to_order(r) for r in self._run(sql, args)]

    def recent(self, limit: int) -> list[Order]:
        rows = self._run(f"SELECT * FROM orders ORDER BY created_at DESC LIMIT {int(limit)}")
        return [self._to_order(r) for r in rows]

    def list_by_email(self, email: str, limit: int) -> list[Order]:
        rows = self._run(f"SELECT * FROM orders WHERE customer_email=? ORDER BY created_at DESC LIMIT {int(limit)}",
                         (email,))
        return [self._to_order(r) for r in rows]

    def update_status(self, order_id: str, status: OrderStatus) -> None:
        self._run("UPDATE orders SET status=? WHERE id=?", (status.value, order_id))

    def mark_published(self, order_id: str) -> None:
        self._run("UPDATE orders SET status='PUBLISHED' WHERE id=? AND status='PENDING'", (order_id,))

    def count_by_status(self) -> dict[str, int]:
        counts = {s.value: 0 for s in OrderStatus}
        for status, n in self._run("SELECT status, COUNT(*) FROM orders GROUP BY status"):
            counts[status] = n
        return counts

    def revenue(self, status: OrderStatus) -> float:
        rows = self._run(
            """SELECT SUM(json_extract(j.value,'$.quantity') * json_extract(j.value,'$.unit_price'))
               FROM orders, json_each(orders.items) j WHERE orders.status=?""",
            (status.value,),
        )
        return round(rows[0][0] or 0.0, 2)

    def count_ahead(self, order: Order) -> int:
        rows = self._run("SELECT COUNT(*) FROM orders WHERE status IN ('PENDING','PUBLISHED') AND created_at<?",
                         (order.created_at,))
        return rows[0][0]

    def count_since(self, seconds: float) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
        return self._run("SELECT COUNT(*) FROM orders WHERE created_at>=?", (cutoff,))[0][0]

    def stale_published(self, older_than_s: float, limit: int = 1000) -> list[str]:
        rows = self._run(f"""SELECT o.id FROM orders o JOIN outbox x ON x.aggregate_id = o.id
                             WHERE o.status = 'PUBLISHED' AND x.published_at < ? ORDER BY x.published_at
                             LIMIT {int(limit)}""", (_ago(older_than_s),))
        return [r[0] for r in rows]

    def clear(self) -> None:
        self._tx(("DELETE FROM outbox", ()), ("DELETE FROM orders", ()))

    @staticmethod
    def _to_order(row) -> Order:
        id_, email, items, status, created = row
        return Order(email, [OrderItem(**i) for i in json.loads(items)],
                     id=id_, status=OrderStatus(status), created_at=created)


class SqliteOutbox(Outbox):
    def __init__(self, repo: SqliteOrderRepository):
        self.repo = repo

    def relay(self, publish: Callable[[str], None], limit: int, older_than_s: float) -> int:
        rows = self.repo._run(f"""SELECT id, aggregate_id FROM outbox WHERE published_at IS NULL AND created_at <= ?
                                  ORDER BY id LIMIT {int(limit)}""", (_ago(older_than_s),))
        sent = 0
        for event_id, order_id in rows:
            try:
                publish(order_id)
            except Exception:
                break
            self.repo._run("UPDATE outbox SET published_at = ? WHERE id = ?", (_now(), event_id))
            sent += 1
        return sent

    def mark_sent(self, order_id: str) -> None:
        self.repo._run("UPDATE outbox SET published_at = ? WHERE aggregate_id = ? AND published_at IS NULL",
                       (_now(), order_id))

    def pending_count(self) -> int:
        return self.repo._run("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")[0][0]

    def recent(self, limit: int) -> list[dict]:
        rows = self.repo._run(f"""SELECT id, aggregate_id, event_type, created_at, published_at FROM outbox
                                  ORDER BY id DESC LIMIT {int(limit)}""")
        # las fechas ya se guardan en ISO con zona horaria (ver _now)
        return [{"id": id_, "aggregate_id": agg, "event_type": ev, "created_at": created, "published_at": published}
                for id_, agg, ev, created, published in rows]
