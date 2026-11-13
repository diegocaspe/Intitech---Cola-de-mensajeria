"""Repositorio de la Zona Yape real sobre PostgreSQL (tablas yape_pedidos y yape_notificaciones, ver app/database)."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from app.models.order import Order
from app.models.yape import (EstadoNotificacion, EstadoPedidoYape, PAGO_EVENT, SinMontosYape, SinStockYape,
                             YapeNotificacion, YapePedido, pago_aggregate)
from app.repositories.interfaces import YapeRepository
from app.repositories.postgres_repository import DESCONTAR_STOCK

# Un solo número para pg_advisory_xact_lock: serializa los checkouts Yape (son pocos) para que el monto único y la
# reserva de stock se decidan de a uno, sin carreras. El índice único parcial es la segunda red de seguridad.
_LOCK_CHECKOUT = 7_031_925

_PEDIDO_COLS = ("id, customer_email, customer_name, sku, producto, monto, estado, creado_en, expira_en, pagado_en, "
                "notificacion_id")
_NOTIF_COLS = ("id, hash, titulo, texto, recibida_en, estado, simulada, monto_leido, pagador, pedido_id, detalle, "
               "procesada_en")
PENDIENTE = EstadoPedidoYape.PENDIENTE.value


def _pedido(row) -> YapePedido:
    return YapePedido(row[0], row[1], row[2], row[3], row[4], Decimal(row[5]), EstadoPedidoYape(row[6]), row[7],
                      row[8], row[9], row[10])


def _notif(row) -> YapeNotificacion:
    return YapeNotificacion(row[0], row[1], row[2], row[3], row[4], EstadoNotificacion(row[5]), row[6],
                            Decimal(row[7]) if row[7] is not None else None, row[8], row[9], row[10], row[11])


def _values(estados) -> list[str]:
    return [e.value for e in estados]


class PostgresYapeRepository(YapeRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def crear_pedido(self, pedido: YapePedido, montos: list[Decimal], now: datetime) -> YapePedido:
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_CHECKOUT,))
            conn.execute("UPDATE yape_pedidos SET estado='expirado' WHERE estado=%s AND expira_en < %s", (PENDIENTE, now))
            # disponible = stock de la tabla (los pagados ya se descontaron) − pendientes vigentes (reservan)
            row = conn.execute("SELECT stock FROM products WHERE sku=%s", (pedido.sku,)).fetchone()
            ocupados = conn.execute("SELECT COUNT(*) FROM yape_pedidos WHERE sku=%s AND estado=%s",
                                    (pedido.sku, PENDIENTE)).fetchone()[0]
            if row is None or ocupados >= row[0]:
                raise SinStockYape("Se agotó el stock de este producto")
            usados = {Decimal(r[0]) for r in conn.execute("SELECT monto FROM yape_pedidos WHERE estado=%s", (PENDIENTE,))}
            libre = next((m for m in montos if m not in usados), None)
            if libre is None:
                raise SinMontosYape("No quedan montos libres")
            pedido.monto, pedido.estado = libre, EstadoPedidoYape.PENDIENTE
            conn.execute(f"INSERT INTO yape_pedidos ({_PEDIDO_COLS}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                         (pedido.id, pedido.customer_email, pedido.customer_name, pedido.sku, pedido.producto,
                          pedido.monto, pedido.estado.value, pedido.creado_en, pedido.expira_en, None, None))
        return pedido

    def get_pedido(self, pedido_id: str) -> YapePedido | None:
        with self.pool.connection() as conn:
            row = conn.execute(f"SELECT {_PEDIDO_COLS} FROM yape_pedidos WHERE id=%s", (pedido_id,)).fetchone()
        return _pedido(row) if row else None

    def pedidos(self, estados: tuple[EstadoPedidoYape, ...], limit: int) -> list[YapePedido]:
        with self.pool.connection() as conn:
            rows = conn.execute(f"SELECT {_PEDIDO_COLS} FROM yape_pedidos WHERE estado = ANY(%s) "
                                "ORDER BY creado_en DESC LIMIT %s", (_values(estados), limit)).fetchall()
        return [_pedido(r) for r in rows]

    def expirar(self, now: datetime) -> int:
        with self.pool.connection() as conn:
            return conn.execute("UPDATE yape_pedidos SET estado='expirado' WHERE estado=%s AND expira_en < %s",
                                (PENDIENTE, now)).rowcount

    def reservados(self, now: datetime) -> dict[str, int]:
        with self.pool.connection() as conn:
            rows = conn.execute("""SELECT sku, COUNT(*) FROM yape_pedidos
                                   WHERE estado=%s AND expira_en >= %s GROUP BY sku""", (PENDIENTE, now)).fetchall()
        return {sku: n for sku, n in rows}

    def guardar_notificacion(self, n: YapeNotificacion) -> tuple[int, bool]:
        with self.pool.connection() as conn, conn.transaction():   # texto crudo + evento: todo o nada
            row = conn.execute("""INSERT INTO yape_notificaciones (hash, titulo, texto, recibida_en, estado, simulada,
                                      monto_leido, pagador, detalle)
                                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (hash) DO NOTHING RETURNING id""",
                               (n.hash, n.titulo, n.texto, n.recibida_en, n.estado.value, n.simulada, n.monto_leido,
                                n.pagador, n.detalle)).fetchone()
            if row is None:                                         # duplicada: ya está guardada con su evento
                return conn.execute("SELECT id FROM yape_notificaciones WHERE hash=%s", (n.hash,)).fetchone()[0], False
            conn.execute("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, %s, %s)",
                         (pago_aggregate(row[0]), PAGO_EVENT, Jsonb({"notificacion_id": row[0]})))
        return row[0], True

    def get_notificacion(self, notificacion_id: int) -> YapeNotificacion | None:
        with self.pool.connection() as conn:
            row = conn.execute(f"SELECT {_NOTIF_COLS} FROM yape_notificaciones WHERE id=%s", (notificacion_id,)).fetchone()
        return _notif(row) if row else None

    def buscar_coincidencia(self, monto: Decimal, recibida_en: datetime) -> YapePedido | None:
        with self.pool.connection() as conn:
            row = conn.execute(f"""SELECT {_PEDIDO_COLS} FROM yape_pedidos
                                   WHERE monto=%s AND estado IN (%s,'expirado') AND creado_en <= %s AND expira_en >= %s
                                   ORDER BY creado_en DESC LIMIT 1""",
                               (monto, PENDIENTE, recibida_en, recibida_en)).fetchone()
        return _pedido(row) if row else None

    def asociar(self, notificacion_id: int, pedido_id: str, order: Order, now: datetime,
                notif_desde: tuple[EstadoNotificacion, ...], detalle: str, monto: Decimal | None,
                pagador: str | None) -> str:
        with self.pool.connection() as conn, conn.transaction():
            # FOR UPDATE: dos workers con la misma notificación (o el admin a la vez) no pueden asociarla dos veces
            st = conn.execute("SELECT estado FROM yape_notificaciones WHERE id=%s FOR UPDATE", (notificacion_id,)).fetchone()
            if st is None or st[0] not in _values(notif_desde):
                return "notificacion_cerrada"
            ped = conn.execute("SELECT estado, sku FROM yape_pedidos WHERE id=%s FOR UPDATE", (pedido_id,)).fetchone()
            if ped is None or ped[0] == EstadoPedidoYape.PAGADO.value:
                return "pedido_pagado"
            conn.execute("""UPDATE yape_pedidos SET estado='pagado', pagado_en=%s, notificacion_id=%s WHERE id=%s""",
                         (now, notificacion_id, pedido_id))
            # el pago se lleva su unidad: mismo UPDATE atómico que el worker. Si ya no alcanza (pago tardío de un pedido
            # vencido), el pago igual queda asociado y el worker rechazará el pedido por falta de stock
            descontado = conn.execute(DESCONTAR_STOCK, {"sku": ped[1], "qty": 1}).rowcount == 1
            conn.execute("""UPDATE yape_notificaciones SET estado='asociada', pedido_id=%s, detalle=%s, procesada_en=%s,
                                monto_leido=COALESCE(%s, monto_leido), pagador=COALESCE(%s, pagador) WHERE id=%s""",
                         (pedido_id, detalle, now, monto, pagador, notificacion_id))
            # nace el pedido normal con su evento: desde aquí, el flujo de siempre (relay → cola → worker → correo)
            conn.execute("""INSERT INTO orders (id, customer_email, items, status, created_at, stock_descontado)
                            VALUES (%s,%s,%s,%s,%s,%s)""",
                         (order.id, order.customer_email, Jsonb([i.to_dict() for i in order.items]),
                          order.status.value, order.created_at, descontado))
            conn.execute("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, 'OrderCreated', %s)",
                         (order.id, Jsonb({"order_id": order.id})))
        return "ok"

    def marcar_notificacion(self, notificacion_id: int, estado: EstadoNotificacion, now: datetime,
                            desde: tuple[EstadoNotificacion, ...], detalle: str | None = None,
                            monto: Decimal | None = None, pagador: str | None = None) -> bool:
        with self.pool.connection() as conn:
            return conn.execute("""UPDATE yape_notificaciones SET estado=%s, procesada_en=%s,
                                       detalle=COALESCE(%s, detalle), monto_leido=COALESCE(%s, monto_leido),
                                       pagador=COALESCE(%s, pagador)
                                   WHERE id=%s AND estado = ANY(%s)""",
                                (estado.value, now, detalle, monto, pagador, notificacion_id, _values(desde))).rowcount > 0

    def pago_en_camino(self, pedido: YapePedido) -> YapeNotificacion | None:
        with self.pool.connection() as conn:
            row = conn.execute(f"""SELECT {_NOTIF_COLS} FROM yape_notificaciones
                                   WHERE estado='recibida' AND monto_leido=%s AND recibida_en BETWEEN %s AND %s
                                   ORDER BY id LIMIT 1""", (pedido.monto, pedido.creado_en, pedido.expira_en)).fetchone()
        return _notif(row) if row else None

    def antes_en_cola(self, notificacion_id: int) -> int:
        with self.pool.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM yape_notificaciones WHERE estado='recibida' AND id < %s",
                                (notificacion_id,)).fetchone()[0]

    def notificaciones(self, limit: int) -> list[dict]:
        with self.pool.connection() as conn:
            rows = conn.execute(f"""SELECT {', '.join('n.' + c.strip() for c in _NOTIF_COLS.split(','))},
                                           (x.id IS NOT NULL), (x.published_at IS NOT NULL), o.status
                                    FROM yape_notificaciones n
                                    LEFT JOIN outbox x ON x.aggregate_id = 'pago-' || n.id AND x.event_type = %s
                                    LEFT JOIN orders o ON o.id = n.pedido_id
                                    ORDER BY n.id DESC LIMIT %s""", (PAGO_EVENT, limit)).fetchall()
        return [{"notificacion": _notif(r[:12]), "outbox_publicado": r[13] if r[12] else None, "order_status": r[14]}
                for r in rows]

    def clear(self) -> None:
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("DELETE FROM yape_notificaciones")
            conn.execute("DELETE FROM yape_pedidos")
