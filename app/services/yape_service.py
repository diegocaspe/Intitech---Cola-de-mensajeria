"""Zona Yape real: checkout con monto único, webhook de MacroDroid, procesamiento del pago y revisión manual.

Solo conoce las interfaces de repositories/interfaces.py. Las horas salen de self.clock (las pruebas lo adelantan)."""
import logging
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from app.models.order import Order, OrderItem, OrderStatus
from app.models.yape import (EstadoNotificacion as EN, EstadoPedidoYape as EP, SinMontosYape, SinStockYape, YapeError,
                             YapeNotificacion, YapePedido, notification_hash, utcnow)
from app.repositories.interfaces import OrderRepository, ProductRepository, YapeRepository
from app.services import yape_parser
from app.services.order_service import ProcessingError

log = logging.getLogger(__name__)
CENT = Decimal("0.01")

# Fases que ve el cliente en su pantalla de pago
ESPERANDO, EN_COLA, PROCESANDO, PAGO_REGISTRADO, CONFIRMADO, EXPIRADO, SIN_STOCK = (
    "esperando_yapeo", "en_cola", "procesando", "pago_registrado", "confirmado", "expirado", "sin_stock")


class YapeService:
    def __init__(self, repo: YapeRepository, orders: OrderRepository | None = None,
                 products: ProductRepository | None = None, settings: dict | None = None, clock=utcnow):
        self.repo = repo
        self.orders = orders
        self.products = products
        self.settings = settings or {}
        self.clock = clock

    @property
    def enabled(self) -> bool:
        return bool(self.settings.get("enabled"))

    def _montos(self) -> list[Decimal]:
        """Todos los montos del rango, de céntimo en céntimo, en orden aleatorio (así no son predecibles)."""
        lo, hi = self.settings["monto_min"], self.settings["monto_max"]
        montos = [lo + CENT * i for i in range(int((hi - lo) / CENT) + 1)]
        random.shuffle(montos)
        return montos

    # --- checkout (API) -------------------------------------------------------
    def checkout(self, email: str, name: str, sku: str) -> YapePedido:
        """Un pedido = 1 unidad de 1 producto. Si la BD está caída lanza (la API responde "alta demanda") y no se crea
        nada ni se muestra el QR. Lanza SinMontosYape si todos los montos están en uso."""
        if not self.enabled:
            raise YapeError("La Zona Yape real está desactivada")
        product = self.products.get(sku) if self.products else None
        if product is None or not product.pago_yape_real:
            raise YapeError("Ese producto no es de la Zona Yape real")
        now = self.clock()
        pedido = YapePedido(str(uuid.uuid4()), email, name, sku, product.name, Decimal(0), EP.PENDIENTE, now,
                            now + timedelta(minutes=self.settings["minutos"]))
        return self.repo.crear_pedido(pedido, self._montos(), now)   # el stock lo lee de la tabla products

    def stock_disponible(self) -> dict[str, int]:
        """Stock de cada producto Yape (tabla products, ya sin los pagados) menos los reservados por pendientes."""
        reservados = self.repo.reservados(self.clock())
        return {p.sku: max(0, p.stock - reservados.get(p.sku, 0))
                for p in (self.products.all() if self.products else []) if p.pago_yape_real}

    def expirar_vencidos(self) -> int:
        return self.repo.expirar(self.clock())

    # --- webhook (API): solo guarda; el worker procesa ----------------------------
    def recibir(self, titulo: str, texto: str, simulada: bool = False) -> tuple[int, bool]:
        """Guarda el texto crudo + su evento en el outbox (misma transacción). Devuelve (id, nueva). El monto que se
        guarda aquí es solo un adelanto para la pantalla del cliente: el worker vuelve a leer el texto al procesar."""
        now = self.clock()
        titulo, texto = (titulo or "")[:500], (texto or "")[:4000]
        lectura = yape_parser.leer(titulo, texto)
        h = notification_hash(titulo, texto, now) if not simulada else uuid.uuid4().hex   # cada simulación es única
        n = YapeNotificacion(None, h, titulo, texto, now, EN.RECIBIDA, simulada,
                             lectura.monto if lectura.es_pago else None, lectura.pagador)
        return self.repo.guardar_notificacion(n)

    def simular(self, pedido_id: str | None = None, monto: Decimal | None = None) -> tuple[int, Decimal]:
        """Notificación de prueba (sin dinero real), por el mismo camino que una real. Marcada como simulada."""
        if pedido_id:
            pedido = self.repo.get_pedido(pedido_id)
            if pedido is None:
                raise YapeError("Pedido Yape no encontrado")
            monto = pedido.monto
        if monto is None or monto <= 0:
            raise YapeError("Indica un pedido o un monto")
        monto = Decimal(monto).quantize(CENT)
        notif_id, _ = self.recibir("Confirmación de Pago",
                                   f"Yape! Cliente Simulado te envió un pago por S/ {monto}", simulada=True)
        return notif_id, monto

    # --- worker: procesa una notificación de la cola "pagos" -----------------------
    def procesar(self, notificacion_id: int) -> str:
        """Idempotente: si la notificación ya se procesó, no hace nada. Los errores de BD se propagan (el consumidor
        devuelve el mensaje a la cola sin gastar reintentos). Devuelve el estado final."""
        n = self.repo.get_notificacion(notificacion_id)
        if n is None:
            raise ProcessingError(f"Notificación {notificacion_id} no existe")
        if n.estado != EN.RECIBIDA:
            log.info("Notificación %s ya procesada (%s): se ignora el duplicado", notificacion_id, n.estado.value)
            return n.estado.value
        now = self.clock()
        lectura = yape_parser.leer(n.titulo, n.texto)
        if not lectura.es_pago:
            self.repo.marcar_notificacion(notificacion_id, EN.IGNORADA, now, (EN.RECIBIDA,), lectura.motivo)
            return EN.IGNORADA.value
        pedido = self.repo.buscar_coincidencia(lectura.monto, n.recibida_en)
        if pedido is None:
            self.repo.marcar_notificacion(notificacion_id, EN.SIN_COINCIDENCIA, now, (EN.RECIBIDA,),
                                          f"Ningún pedido vigente esperaba S/ {lectura.monto} cuando llegó el pago",
                                          lectura.monto, lectura.pagador)
            return EN.SIN_COINCIDENCIA.value
        result = self.repo.asociar(notificacion_id, pedido.id, self._order(pedido, now), now, (EN.RECIBIDA,),
                                   f"Pago de S/ {lectura.monto} asociado automáticamente", lectura.monto, lectura.pagador)
        if result == "pedido_pagado":   # p. ej. el cliente yapeó dos veces: el segundo pago queda para revisión
            self.repo.marcar_notificacion(notificacion_id, EN.SIN_COINCIDENCIA, now, (EN.RECIBIDA,),
                                          "El pedido con ese monto ya estaba pagado (¿yapeo repetido?)",
                                          lectura.monto, lectura.pagador)
            return EN.SIN_COINCIDENCIA.value
        if result == "ok":
            log.info("Pago Yape %s asociado al pedido %s", notificacion_id, pedido.id[:8])
            return EN.ASOCIADA.value
        return (self.repo.get_notificacion(notificacion_id) or n).estado.value   # otro worker se adelantó

    @staticmethod
    def _order(pedido: YapePedido, now: datetime) -> Order:
        """El pedido normal que nace al pagar: mismo id, 1 unidad, al precio realmente yapeado."""
        return Order(pedido.customer_email, [OrderItem(pedido.sku, 1, pedido.monto)], id=pedido.id,
                     created_at=now.isoformat())

    # --- revisión manual (admin) --------------------------------------------------
    def asignar(self, notificacion_id: int, pedido_id: str) -> None:
        """Asocia a mano una notificación sin_coincidencia (o ignorada, si el parser falló) a un pedido pendiente o
        expirado. Mismo efecto que la asociación automática: el pedido sigue el flujo normal."""
        pedido = self.repo.get_pedido(pedido_id)
        if pedido is None:
            raise YapeError("Pedido Yape no encontrado")
        if pedido.estado == EP.PAGADO:
            raise YapeError("Ese pedido ya está pagado")
        now = self.clock()
        result = self.repo.asociar(notificacion_id, pedido.id, self._order(pedido, now), now,
                                   (EN.SIN_COINCIDENCIA, EN.IGNORADA), "Asignado a mano por el administrador", None, None)
        if result == "notificacion_cerrada":
            raise YapeError("Solo se pueden asignar notificaciones sin coincidencia o ignoradas")
        if result == "pedido_pagado":
            raise YapeError("Ese pedido ya está pagado")

    def rechazar(self, notificacion_id: int) -> None:
        if not self.repo.marcar_notificacion(notificacion_id, EN.RECHAZADA, self.clock(),
                                             (EN.SIN_COINCIDENCIA, EN.IGNORADA), "Rechazada por el administrador"):
            raise YapeError("Solo se pueden rechazar notificaciones sin coincidencia o ignoradas")

    # --- pantalla del cliente -----------------------------------------------------
    def estado_cliente(self, pedido_id: str, email: str | None, broker_up: bool = True) -> dict | None:
        """Lo que ve el cliente en su pantalla de pago. Nunca devuelve un error a quien ya pagó: si algo falla después
        del pago se muestra "registrado / en cola", no un fallo. email=None: sin filtrar (admin)."""
        pedido = self.repo.get_pedido(pedido_id)
        if pedido is None or (email is not None and pedido.customer_email != email):
            return None
        now = self.clock()
        out = {"id": pedido.id, "sku": pedido.sku, "producto": pedido.producto, "monto": pedido.monto,
               "estado": pedido.estado.value, "titular": self.settings.get("titular", ""),
               "expira_en": pedido.expira_en.isoformat(), "segundos_restantes": pedido.segundos_restantes(now),
               "minutos": self.settings.get("minutos"), "posicion": 0, "fase": ESPERANDO}
        if pedido.estado == EP.PAGADO:
            order = self.orders.get(pedido.id) if self.orders else None
            if order is None or order.status == OrderStatus.PENDING:
                out["fase"] = EN_COLA if broker_up else PAGO_REGISTRADO
                if order is not None and broker_up:
                    out["posicion"] = self.orders.count_ahead(order) + 1
            elif order.status == OrderStatus.PUBLISHED:
                out["posicion"] = self.orders.count_ahead(order) + 1
                out["fase"] = PROCESANDO if out["posicion"] <= 1 else EN_COLA
            elif order.status == OrderStatus.PROCESSED:
                out["fase"] = CONFIRMADO
            elif order.status == OrderStatus.REJECTED:   # pagó tarde y otro se llevó la última unidad: se le devuelve
                out["fase"] = SIN_STOCK
            else:   # FAILED: el pago está registrado; el administrador lo reprocesa desde la DLQ
                out["fase"] = PAGO_REGISTRADO
            return out
        en_camino = self.repo.pago_en_camino(pedido)
        if en_camino is not None:   # el webhook ya lo guardó; el worker aún no lo procesa
            out["fase"] = EN_COLA if broker_up else PAGO_REGISTRADO
            out["posicion"] = self.repo.antes_en_cola(en_camino.id) + 1
        elif pedido.estado == EP.EXPIRADO:
            out["fase"] = EXPIRADO
        return out

    # --- panel --------------------------------------------------------------------
    def panel(self, limit: int = 30) -> dict:
        now = self.clock()
        pendientes = self.repo.pedidos((EP.PENDIENTE,), 50)
        asignables = self.repo.pedidos((EP.PENDIENTE, EP.EXPIRADO), 30)
        return {
            "pendientes": [{"id": p.id, "cliente": p.customer_name, "email": p.customer_email, "producto": p.producto,
                            "monto": p.monto, "segundos_restantes": p.segundos_restantes(now)}
                           for p in pendientes if p.expira_en >= now],
            "asignables": [{"id": p.id, "cliente": p.customer_name, "monto": p.monto, "estado": p.estado.value,
                            "creado_en": p.creado_en.isoformat()} for p in asignables],
            "notificaciones": [self._fila(r) for r in self.repo.notificaciones(limit)],
        }

    @staticmethod
    def _fila(r: dict) -> dict:
        """Notificación para el panel con su etapa: notificacion → outbox → cola_pagos → worker → confirmado."""
        n: YapeNotificacion = r["notificacion"]
        if r["outbox_publicado"] is False:
            etapa = "outbox"                      # guardada, el relay aún no la publica (¿RabbitMQ caído?)
        elif n.estado == EN.RECIBIDA:
            etapa = "cola_pagos"                  # en la cola "pagos", esperando al worker
        elif n.estado == EN.ASOCIADA:
            etapa = "confirmado" if r["order_status"] == OrderStatus.PROCESSED.value else "worker"
        else:
            etapa = "fin"                         # ignorada, sin coincidencia o rechazada: se detiene aquí
        return {"id": n.id, "recibida_en": n.recibida_en.isoformat(), "titulo": n.titulo, "texto": n.texto,
                "estado": n.estado.value, "simulada": n.simulada, "monto": n.monto_leido, "pagador": n.pagador,
                "pedido_id": n.pedido_id, "detalle": n.detalle, "etapa": etapa, "order_status": r["order_status"]}


__all__ = ["YapeService", "YapeError", "SinMontosYape", "SinStockYape"]
