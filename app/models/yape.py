"""Zona Yape real: pedidos que se pagan con Yape de verdad y las notificaciones que llegan del celular.

Solo datos y reglas (sin tecnología). Un pedido Yape vive en su propia tabla mientras espera el pago; cuando el pago
se asocia, en la MISMA transacción nace el pedido normal (tabla orders, mismo id) con su evento en el outbox y desde
ahí sigue el flujo de siempre: relay → cola de pedidos → worker → confirmado → correo con el comprobante."""
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

PAGO_EVENT = "pago.yape_recibido"   # evento del outbox que lleva una notificación a la cola "pagos"


def pago_aggregate(notificacion_id: int) -> str:
    """aggregate_id del evento en el outbox: nunca coincide con el id de un pedido (las consultas de pedidos que cruzan
    outbox con orders no lo ven)."""
    return f"pago-{notificacion_id}"


class EstadoPedidoYape(str, Enum):
    PENDIENTE = "pendiente_pago_yape"   # esperando el yapeo, con el stock reservado
    PAGADO = "pagado"                   # el pago se asoció: ya existe su pedido normal en orders
    EXPIRADO = "expirado"               # venció sin pago: el stock y el monto quedan libres


class EstadoNotificacion(str, Enum):
    RECIBIDA = "recibida"                  # guardada por el webhook, esperando al worker
    ASOCIADA = "asociada"                  # pago asociado a un pedido
    SIN_COINCIDENCIA = "sin_coincidencia"  # es un pago, pero ningún pedido vigente tenía ese monto: revisión manual
    IGNORADA = "ignorada"                  # no es un pago recibido (Yapeaste, promociones, recordatorios...)
    RECHAZADA = "rechazada"                # el administrador la descartó


class YapeError(Exception):
    """Error de negocio de la Zona Yape (se muestra al cliente o al administrador)."""


class SinStockYape(YapeError):
    pass


class SinMontosYape(YapeError):
    """Todos los montos del rango están en uso por pedidos vigentes."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def notification_hash(titulo: str, texto: str, recibida_en: datetime) -> str:
    """Idempotencia del webhook: el mismo texto llegado en el mismo minuto es la misma notificación (MacroDroid puede
    reenviarla al reintentar)."""
    minuto = recibida_en.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    return hashlib.sha256(f"{titulo}\n{texto}\n{minuto}".encode()).hexdigest()


@dataclass
class YapePedido:
    id: str
    customer_email: str
    customer_name: str
    sku: str
    producto: str
    monto: Decimal
    estado: EstadoPedidoYape
    creado_en: datetime
    expira_en: datetime
    pagado_en: datetime | None = None
    notificacion_id: int | None = None

    def segundos_restantes(self, now: datetime) -> int:
        return max(0, int((self.expira_en - now).total_seconds()))

    def recibio_a_tiempo(self, recibida_en: datetime) -> bool:
        """Un pago vale si LLEGÓ mientras el pedido estaba vigente, aunque el worker lo procese después."""
        return self.creado_en <= recibida_en <= self.expira_en


@dataclass
class YapeNotificacion:
    id: int | None
    hash: str
    titulo: str
    texto: str
    recibida_en: datetime
    estado: EstadoNotificacion = EstadoNotificacion.RECIBIDA
    simulada: bool = False
    monto_leido: Decimal | None = None
    pagador: str | None = None
    pedido_id: str | None = None
    detalle: str | None = None
    procesada_en: datetime | None = None
