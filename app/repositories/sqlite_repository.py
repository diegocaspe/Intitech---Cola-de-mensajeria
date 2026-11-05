from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable

from app.repositories.interfaces import ORDER_EVENT, Outbox, OrderRepository, SentEmailRepository, UserRepository
from app.models.email import EmailEvent, EmailToken, TokenPurpose, TokenResult
from app.models.order import Order, OrderItem, OrderStatus, money
from app.models.user import EmailStatus, Role, User


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _email_row(event: EmailEvent) -> tuple[str, tuple]:
    return ("INSERT INTO outbox (aggregate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
            (event.email_id, event.kind.value, json.dumps(event.to_payload()), _now()))


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
             (order.id, order.customer_email, json.dumps([i.to_dict() for i in order.items]),
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

    def _write(self, fn) -> object:
        """fn(cursor) dentro de una sola transacción (todo o nada); devuelve lo que devuelva fn."""
        if self._memory_conn:
            with self._memory_conn as c:
                return fn(c)
        with closing(sqlite3.connect(self.db_path, timeout=15)) as c:
            with c:
                return fn(c)

    def mark_processed(self, order_id: str, receipt: EmailEvent | None = None) -> bool:
        def tx(c):
            done = c.execute("UPDATE orders SET status='PROCESSED' WHERE id=? AND status<>'PROCESSED'",
                             (order_id,)).rowcount
            if done and receipt:
                c.execute(*_email_row(receipt))
            return bool(done)
        return self._write(tx)

    def mark_published(self, order_id: str) -> None:
        self._run("UPDATE orders SET status='PUBLISHED' WHERE id=? AND status='PENDING'", (order_id,))

    def count_by_status(self) -> dict[str, int]:
        counts = {s.value: 0 for s in OrderStatus}
        for status, n in self._run("SELECT status, COUNT(*) FROM orders GROUP BY status"):
            counts[status] = n
        return counts

    def revenue(self, status: OrderStatus) -> Decimal:
        # SQLite no tiene un tipo decimal exacto (multiplicaría en float): se suma en Python con Decimal
        return money(sum((o.total for o in self.list(status)), Decimal(0)))

    def count_ahead(self, order: Order) -> int:
        rows = self._run("SELECT COUNT(*) FROM orders WHERE status IN ('PENDING','PUBLISHED') AND created_at<?",
                         (order.created_at,))
        return rows[0][0]

    def count_since(self, seconds: float) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
        return self._run("SELECT COUNT(*) FROM orders WHERE created_at>=?", (cutoff,))[0][0]

    def stale_published(self, older_than_s: float, limit: int = 1000) -> list[str]:
        rows = self._run(f"""SELECT o.id FROM orders o JOIN outbox x ON x.aggregate_id = o.id
                             WHERE o.status = 'PUBLISHED' AND x.event_type = 'OrderCreated' AND x.published_at < ?
                             ORDER BY x.published_at
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

    def relay(self, publish: Callable[[str, str, dict], None], limit: int, older_than_s: float) -> int:
        rows = self.repo._run(f"""SELECT id, aggregate_id, event_type, payload FROM outbox
                                  WHERE published_at IS NULL AND created_at <= ?
                                  ORDER BY id LIMIT {int(limit)}""", (_ago(older_than_s),))
        sent = 0
        for event_id, aggregate_id, event_type, payload in rows:
            try:
                publish(event_type, aggregate_id, json.loads(payload))
            except Exception:
                break
            self.repo._run("UPDATE outbox SET published_at = ? WHERE id = ?", (_now(), event_id))
            sent += 1
        return sent

    def add_event(self, event: EmailEvent) -> None:
        self.repo._run(*_email_row(event))

    def pending_email_count(self) -> int:
        return self.repo._run("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND event_type <> ?",
                              (ORDER_EVENT,))[0][0]

    def events(self, event_type: str | None = None) -> list[dict]:
        """Todos los eventos con su payload (solo para las pruebas)."""
        rows = self.repo._run("SELECT event_type, payload, published_at FROM outbox ORDER BY id")
        return [{"event_type": t, "payload": json.loads(p), "published_at": pub} for t, p, pub in rows
                if event_type is None or t == event_type]

    def mark_sent(self, order_id: str) -> None:
        self.repo._run("UPDATE outbox SET published_at = ? WHERE aggregate_id = ? AND published_at IS NULL",
                       (_now(), order_id))

    def pending_count(self) -> int:
        return self.repo._run("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND event_type = ?",
                              (ORDER_EVENT,))[0][0]

    def _numbers(self) -> tuple[int, dict[int, int]]:
        """Número de cada evento de pedido en la fila (1, 2, 3...), sin contar los correos intercalados."""
        ids = [r[0] for r in self.repo._run("SELECT id FROM outbox WHERE event_type = ? ORDER BY id", (ORDER_EVENT,))]
        return (ids[0] if ids else 1), {event_id: n for n, event_id in enumerate(ids, 1)}

    def recent(self, limit: int) -> list[dict]:
        rows = self.repo._run(f"""SELECT id, aggregate_id, event_type, created_at, published_at FROM outbox
                                  WHERE event_type = ? ORDER BY id DESC LIMIT {int(limit)}""", (ORDER_EVENT,))
        _, numbers = self._numbers()
        # las fechas ya se guardan en ISO con zona horaria (ver _now)
        return [{"id": id_, "aggregate_id": agg, "event_type": ev, "created_at": created, "published_at": published,
                 "number": numbers.get(id_)}
                for id_, agg, ev, created, published in rows]

    def published_stats(self, window_s: float) -> tuple[int, int]:
        total = self.repo._run("SELECT COUNT(published_at) FROM outbox WHERE event_type = ?", (ORDER_EVENT,))[0][0]
        recent = self.repo._run("SELECT COUNT(*) FROM outbox WHERE event_type = ? AND published_at >= ?",
                                (ORDER_EVENT, _ago(window_s)))[0][0]
        return total, recent

    def queue_window(self, edge: int) -> dict:
        waiting = """FROM orders o JOIN outbox x ON x.aggregate_id = o.id
                     WHERE o.status = 'PUBLISHED' AND x.event_type = 'OrderCreated' AND x.published_at IS NOT NULL"""
        count = self.repo._run("SELECT COUNT(*) " + waiting)[0][0]
        first, numbers = self._numbers()
        head = self.repo._run(f"SELECT o.id, x.id {waiting} ORDER BY x.published_at, x.id LIMIT {int(edge)}")
        tail = self.repo._run(f"SELECT o.id, x.id {waiting} ORDER BY x.published_at DESC, x.id DESC LIMIT {int(edge)}")
        return {"count": count, "head": [r[0] for r in head], "tail": [r[0] for r in reversed(tail)], "first_event": first,
                "numbers": {order_id: numbers[event_id] for order_id, event_id in head + tail}}


class SqliteUserRepository(UserRepository):
    """Usuarios y tokens de correo sobre la misma base que los pedidos (comparten el outbox). Para las pruebas."""

    def __init__(self, repo: SqliteOrderRepository):
        self.repo = repo
        repo._run("""CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL, role TEXT NOT NULL, email_status TEXT NOT NULL DEFAULT 'legacy')""")
        repo._run("""CREATE TABLE IF NOT EXISTS email_tokens (
                token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, purpose TEXT NOT NULL,
                created_at TEXT NOT NULL, expires_at TEXT NOT NULL, used_at TEXT)""")

    def get_by_email(self, email: str) -> User | None:
        rows = self.repo._run("SELECT id, name, email, password_hash, role, email_status FROM users WHERE email=?",
                              (email,))
        if not rows:
            return None
        r = rows[0]
        return User(r[0], r[1], r[2], r[3], Role(r[4]), EmailStatus(r[5]))

    def add(self, name: str, email: str, password_hash: str, role: Role) -> User:
        try:
            self.repo._run("INSERT INTO users (name, email, password_hash, role) VALUES (?,?,?,?)",
                           (name, email, password_hash, role.value))
        except sqlite3.IntegrityError:
            raise ValueError("correo duplicado")
        return self.get_by_email(email)

    def update_credentials(self, email: str, password_hash: str, role: Role) -> None:
        self.repo._run("UPDATE users SET password_hash=?, role=? WHERE email=?", (password_hash, role.value, email))

    @staticmethod
    def _token_row(user_id: int, token: EmailToken) -> tuple[str, tuple]:
        return ("INSERT INTO email_tokens (token_hash, user_id, purpose, created_at, expires_at) VALUES (?,?,?,?,?)",
                (token.token_hash, user_id, token.purpose.value, token.created_at.isoformat(),
                 token.expires_at.isoformat()))

    def add_customer_with_email(self, name: str, email: str, password_hash: str, token: EmailToken,
                                event: EmailEvent) -> User:
        def tx(c):
            uid = c.execute("INSERT INTO users (name, email, password_hash, role, email_status) "
                            "VALUES (?,?,?,'customer','pending')", (name, email, password_hash)).lastrowid
            c.execute(*self._token_row(uid, token))
            c.execute(*_email_row(event))
            return uid
        try:
            uid = self.repo._write(tx)
        except sqlite3.IntegrityError:
            raise ValueError("correo duplicado")
        return User(uid, name, email, password_hash, Role.CUSTOMER, EmailStatus.PENDING)

    def add_token_with_email(self, user_id: int, token: EmailToken, event: EmailEvent) -> None:
        self.repo._tx(self._token_row(user_id, token), _email_row(event))

    def last_token_at(self, user_id: int, purpose: TokenPurpose) -> datetime | None:
        value = self.repo._run("SELECT MAX(created_at) FROM email_tokens WHERE user_id=? AND purpose=?",
                               (user_id, purpose.value))[0][0]
        return datetime.fromisoformat(value) if value else None

    def consume_token(self, token_hash: str, purpose: TokenPurpose, now: datetime,
                      new_password_hash: str | None = None) -> TokenResult:
        def tx(c):
            row = c.execute("SELECT user_id, expires_at, used_at FROM email_tokens WHERE token_hash=? AND purpose=?",
                            (token_hash, purpose.value)).fetchone()
            if row is None:
                return TokenResult.INVALID
            user_id, expires_at, used_at = row
            if used_at is not None:
                return TokenResult.USED
            if datetime.fromisoformat(expires_at) <= now:
                return TokenResult.EXPIRED
            c.execute("UPDATE email_tokens SET used_at=? WHERE token_hash=?", (now.isoformat(), token_hash))
            if purpose == TokenPurpose.RESET:
                c.execute("""UPDATE users SET password_hash=?,
                                 email_status = CASE WHEN email_status='pending' THEN 'verified' ELSE email_status END
                             WHERE id=?""", (new_password_hash, user_id))
                c.execute("UPDATE email_tokens SET used_at=? WHERE user_id=? AND purpose='reset' AND used_at IS NULL",
                          (now.isoformat(), user_id))
            else:
                c.execute("UPDATE users SET email_status='verified' WHERE id=? AND email_status='pending'", (user_id,))
            return TokenResult.OK
        return self.repo._write(tx)


class SqliteSentEmails(SentEmailRepository):
    def __init__(self, repo: SqliteOrderRepository):
        self.repo = repo
        repo._run("""CREATE TABLE IF NOT EXISTS sent_emails (
                email_id TEXT PRIMARY KEY, kind TEXT NOT NULL, recipient TEXT NOT NULL, sent_at TEXT NOT NULL)""")

    def was_sent(self, email_id: str) -> bool:
        return bool(self.repo._run("SELECT 1 FROM sent_emails WHERE email_id=?", (email_id,)))

    def mark_sent(self, email_id: str, kind: str, to: str) -> None:
        self.repo._run("INSERT OR IGNORE INTO sent_emails VALUES (?,?,?,?)", (email_id, kind, to, _now()))

    def count(self) -> int:
        return self.repo._run("SELECT COUNT(*) FROM sent_emails")[0][0]
