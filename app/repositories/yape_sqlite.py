"""Repositorio de la Zona Yape real sobre SQLite: misma base que pedidos y outbox (para las pruebas, sin servidor)."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from decimal import Decimal

from app.models.order import Order
from app.models.yape import (EstadoNotificacion, EstadoPedidoYape, PAGO_EVENT, SinMontosYape, SinStockYape,
                             YapeNotificacion, YapePedido, pago_aggregate)
from app.repositories.interfaces import YapeRepository
from app.repositories.sqlite_repository import SqliteOrderRepository, _now, descontar

PENDIENTE = EstadoPedidoYape.PENDIENTE.value
_PEDIDO_COLS = ("id, customer_email, customer_name, sku, producto, monto, estado, creado_en, expira_en, pagado_en, "
                "notificacion_id")
_NOTIF_COLS = ("id, hash, titulo, texto, recibida_en, estado, simulada, monto_leido, pagador, pedido_id, detalle, "
               "procesada_en")


def _ts(dt: datetime | None) -> str | None:
    """Siempre el mismo formato (UTC con microsegundos): así las fechas se comparan bien como texto."""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds") if dt else None


def _dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _money(v: Decimal | None) -> str | None:
    return str(Decimal(v).quantize(Decimal("0.01"))) if v is not None else None


def _pedido(r) -> YapePedido:
    return YapePedido(r[0], r[1], r[2], r[3], r[4], Decimal(r[5]), EstadoPedidoYape(r[6]), _dt(r[7]), _dt(r[8]),
                      _dt(r[9]), r[10])


def _notif(r) -> YapeNotificacion:
    return YapeNotificacion(r[0], r[1], r[2], r[3], _dt(r[4]), EstadoNotificacion(r[5]), bool(r[6]),
                            Decimal(r[7]) if r[7] is not None else None, r[8], r[9], r[10], _dt(r[11]))


def _in(estados) -> tuple[str, list[str]]:
    vals = [e.value for e in estados]
    return ",".join("?" * len(vals)), vals


class SqliteYapeRepository(YapeRepository):
    def __init__(self, repo: SqliteOrderRepository):
        self.repo = repo
        self._lock = threading.Lock()   # equivalente al pg_advisory_xact_lock del checkout
        repo._run("""CREATE TABLE IF NOT EXISTS yape_pedidos (
                id TEXT PRIMARY KEY, customer_email TEXT NOT NULL, customer_name TEXT NOT NULL, sku TEXT NOT NULL,
                producto TEXT NOT NULL, monto TEXT NOT NULL,
                estado TEXT NOT NULL CHECK (estado IN ('pendiente_pago_yape','pagado','expirado')),
                creado_en TEXT NOT NULL, expira_en TEXT NOT NULL, pagado_en TEXT, notificacion_id INTEGER)""")
        repo._run("""CREATE UNIQUE INDEX IF NOT EXISTS uq_yape_monto_pendiente ON yape_pedidos (monto)
                     WHERE estado = 'pendiente_pago_yape'""")
        repo._run("""CREATE TABLE IF NOT EXISTS yape_notificaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT NOT NULL UNIQUE, titulo TEXT NOT NULL DEFAULT '',
                texto TEXT NOT NULL, recibida_en TEXT NOT NULL, estado TEXT NOT NULL DEFAULT 'recibida',
                simulada INTEGER NOT NULL DEFAULT 0, monto_leido TEXT, pagador TEXT, pedido_id TEXT, detalle TEXT,
                procesada_en TEXT)""")

    def crear_pedido(self, pedido: YapePedido, montos: list[Decimal], now: datetime) -> YapePedido:
        def tx(c):
            c.execute("UPDATE yape_pedidos SET estado='expirado' WHERE estado=? AND expira_en < ?", (PENDIENTE, _ts(now)))
            # disponible = stock de la tabla (los pagados ya se descontaron) − pendientes vigentes (reservan)
            row = c.execute("SELECT stock FROM products WHERE sku=?", (pedido.sku,)).fetchone()
            ocupados = c.execute("SELECT COUNT(*) FROM yape_pedidos WHERE sku=? AND estado=?",
                                 (pedido.sku, PENDIENTE)).fetchone()[0]
            if row is None or ocupados >= row[0]:
                raise SinStockYape("Se agotó el stock de este producto")
            usados = {Decimal(r[0]) for r in c.execute("SELECT monto FROM yape_pedidos WHERE estado=?", (PENDIENTE,))}
            libre = next((m for m in montos if m not in usados), None)
            if libre is None:
                raise SinMontosYape("No quedan montos libres")
            pedido.monto, pedido.estado = libre, EstadoPedidoYape.PENDIENTE
            c.execute(f"INSERT INTO yape_pedidos ({_PEDIDO_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (pedido.id, pedido.customer_email, pedido.customer_name, pedido.sku, pedido.producto,
                       _money(pedido.monto), PENDIENTE, _ts(pedido.creado_en), _ts(pedido.expira_en), None, None))
            return pedido
        with self._lock:
            return self.repo._write(tx)

    def get_pedido(self, pedido_id: str) -> YapePedido | None:
        rows = self.repo._run(f"SELECT {_PEDIDO_COLS} FROM yape_pedidos WHERE id=?", (pedido_id,))
        return _pedido(rows[0]) if rows else None

    def pedidos(self, estados: tuple[EstadoPedidoYape, ...], limit: int) -> list[YapePedido]:
        marks, vals = _in(estados)
        rows = self.repo._run(f"SELECT {_PEDIDO_COLS} FROM yape_pedidos WHERE estado IN ({marks}) "
                              f"ORDER BY creado_en DESC LIMIT {int(limit)}", tuple(vals))
        return [_pedido(r) for r in rows]

    def expirar(self, now: datetime) -> int:
        return self.repo._write(lambda c: c.execute(
            "UPDATE yape_pedidos SET estado='expirado' WHERE estado=? AND expira_en < ?", (PENDIENTE, _ts(now))).rowcount)

    def reservados(self, now: datetime) -> dict[str, int]:
        rows = self.repo._run("SELECT sku, COUNT(*) FROM yape_pedidos WHERE estado=? AND expira_en >= ? GROUP BY sku",
                              (PENDIENTE, _ts(now)))
        return {sku: n for sku, n in rows}

    def guardar_notificacion(self, n: YapeNotificacion) -> tuple[int, bool]:
        def tx(c):
            cur = c.execute("""INSERT OR IGNORE INTO yape_notificaciones (hash, titulo, texto, recibida_en, estado,
                                   simulada, monto_leido, pagador, detalle) VALUES (?,?,?,?,?,?,?,?,?)""",
                            (n.hash, n.titulo, n.texto, _ts(n.recibida_en), n.estado.value, int(n.simulada),
                             _money(n.monto_leido), n.pagador, n.detalle))
            if cur.rowcount == 0:
                return c.execute("SELECT id FROM yape_notificaciones WHERE hash=?", (n.hash,)).fetchone()[0], False
            new_id = cur.lastrowid
            c.execute("INSERT INTO outbox (aggregate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
                      (pago_aggregate(new_id), PAGO_EVENT, json.dumps({"notificacion_id": new_id}), _now()))
            return new_id, True
        return self.repo._write(tx)

    def get_notificacion(self, notificacion_id: int) -> YapeNotificacion | None:
        rows = self.repo._run(f"SELECT {_NOTIF_COLS} FROM yape_notificaciones WHERE id=?", (notificacion_id,))
        return _notif(rows[0]) if rows else None

    def buscar_coincidencia(self, monto: Decimal, recibida_en: datetime) -> YapePedido | None:
        rows = self.repo._run(f"""SELECT {_PEDIDO_COLS} FROM yape_pedidos
                                  WHERE monto=? AND estado IN (?, 'expirado') AND creado_en <= ? AND expira_en >= ?
                                  ORDER BY creado_en DESC LIMIT 1""",
                              (_money(monto), PENDIENTE, _ts(recibida_en), _ts(recibida_en)))
        return _pedido(rows[0]) if rows else None

    def asociar(self, notificacion_id: int, pedido_id: str, order: Order, now: datetime,
                notif_desde: tuple[EstadoNotificacion, ...], detalle: str, monto: Decimal | None,
                pagador: str | None) -> str:
        def tx(c):
            st = c.execute("SELECT estado FROM yape_notificaciones WHERE id=?", (notificacion_id,)).fetchone()
            if st is None or st[0] not in [e.value for e in notif_desde]:
                return "notificacion_cerrada"
            ped = c.execute("SELECT estado, sku FROM yape_pedidos WHERE id=?", (pedido_id,)).fetchone()
            if ped is None or ped[0] == EstadoPedidoYape.PAGADO.value:
                return "pedido_pagado"
            c.execute("UPDATE yape_pedidos SET estado='pagado', pagado_en=?, notificacion_id=? WHERE id=?",
                      (_ts(now), notificacion_id, pedido_id))
            descontado = descontar(c, ped[1], 1)   # el pago se lleva su unidad (si ya no alcanza, la rechaza el worker)
            c.execute("""UPDATE yape_notificaciones SET estado='asociada', pedido_id=?, detalle=?, procesada_en=?,
                             monto_leido=COALESCE(?, monto_leido), pagador=COALESCE(?, pagador) WHERE id=?""",
                      (pedido_id, detalle, _ts(now), _money(monto), pagador, notificacion_id))
            c.execute("""INSERT INTO orders (id, customer_email, items, status, created_at, stock_descontado)
                         VALUES (?,?,?,?,?,?)""",
                      (order.id, order.customer_email, json.dumps([i.to_dict() for i in order.items]),
                       order.status.value, order.created_at, int(descontado)))
            c.execute("INSERT INTO outbox (aggregate_id, event_type, payload, created_at) VALUES (?, 'OrderCreated', ?, ?)",
                      (order.id, json.dumps({"order_id": order.id}), _now()))
            return "ok"
        return self.repo._write(tx)

    def marcar_notificacion(self, notificacion_id: int, estado: EstadoNotificacion, now: datetime,
                            desde: tuple[EstadoNotificacion, ...], detalle: str | None = None,
                            monto: Decimal | None = None, pagador: str | None = None) -> bool:
        marks, vals = _in(desde)
        return self.repo._write(lambda c: c.execute(
            f"""UPDATE yape_notificaciones SET estado=?, procesada_en=?, detalle=COALESCE(?, detalle),
                    monto_leido=COALESCE(?, monto_leido), pagador=COALESCE(?, pagador)
                WHERE id=? AND estado IN ({marks})""",
            (estado.value, _ts(now), detalle, _money(monto), pagador, notificacion_id, *vals)).rowcount > 0)

    def pago_en_camino(self, pedido: YapePedido) -> YapeNotificacion | None:
        rows = self.repo._run(f"""SELECT {_NOTIF_COLS} FROM yape_notificaciones
                                  WHERE estado='recibida' AND monto_leido=? AND recibida_en BETWEEN ? AND ?
                                  ORDER BY id LIMIT 1""",
                              (_money(pedido.monto), _ts(pedido.creado_en), _ts(pedido.expira_en)))
        return _notif(rows[0]) if rows else None

    def antes_en_cola(self, notificacion_id: int) -> int:
        return self.repo._run("SELECT COUNT(*) FROM yape_notificaciones WHERE estado='recibida' AND id < ?",
                              (notificacion_id,))[0][0]

    def notificaciones(self, limit: int) -> list[dict]:
        cols = ", ".join("n." + c.strip() for c in _NOTIF_COLS.split(","))
        rows = self.repo._run(f"""SELECT {cols}, (x.id IS NOT NULL), (x.published_at IS NOT NULL), o.status
                                  FROM yape_notificaciones n
                                  LEFT JOIN outbox x ON x.aggregate_id = 'pago-' || n.id AND x.event_type = ?
                                  LEFT JOIN orders o ON o.id = n.pedido_id
                                  ORDER BY n.id DESC LIMIT {int(limit)}""", (PAGO_EVENT,))
        return [{"notificacion": _notif(r[:12]), "outbox_publicado": bool(r[13]) if r[12] else None,
                 "order_status": r[14]} for r in rows]

    def clear(self) -> None:
        self.repo._tx(("DELETE FROM yape_notificaciones", ()), ("DELETE FROM yape_pedidos", ()))
