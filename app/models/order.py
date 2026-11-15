from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
import uuid

CENT = Decimal("0.01")


def money(value) -> Decimal:
    """Dinero exacto con 2 decimales. Pasa por str para no heredar el error binario del float
    (0.1 + 0.2 = 0.30000000000000004) y redondea como en caja: 0.005 sube a 0.01."""
    return Decimal(str(value)).quantize(CENT, ROUND_HALF_UP)


class OrderStatus(str, Enum):
    PENDING = "PENDING"        # guardada en BD, aún no confirmada por el broker / sin procesar
    PUBLISHED = "PUBLISHED"    # el broker confirmó la recepción del mensaje
    PROCESSED = "PROCESSED"    # el worker la procesó correctamente
    FAILED = "FAILED"          # agotó reintentos, el mensaje fue a la DLQ
    REJECTED = "REJECTED"      # resultado final de negocio (sin stock, pago rechazado): no se reintenta ni va a la DLQ

    @property
    def final(self) -> bool:
        """Ya tiene un resultado definitivo: un mensaje duplicado de este pedido se confirma y se salta."""
        return self in (OrderStatus.PROCESSED, OrderStatus.REJECTED)


class PaymentStatus(str, Enum):
    """Estado del cobro con tarjeta (simulado, ver app/services/payments.py). Lo decide el worker, no la API."""
    PENDING = "PENDING"        # aún no se cobra (el pedido espera en la cola)
    APPROVED = "APPROVED"      # el proveedor aprobó el cobro
    DECLINED = "DECLINED"      # el proveedor lo rechazó (p. ej. fondos insuficientes): el pedido queda REJECTED
    REFUNDED = "REFUNDED"      # se cobró, pero al descontar ya no alcanzó el stock: reembolso simulado


@dataclass
class OrderItem:
    sku: str
    quantity: int
    unit_price: Decimal

    def __post_init__(self):
        self.unit_price = money(self.unit_price)
        if self.quantity <= 0:
            raise ValueError("La cantidad debe ser mayor a 0")
        if self.unit_price < 0:
            raise ValueError("El precio no puede ser negativo")

    def to_dict(self) -> dict:
        """Para guardarlo como JSON: el precio va como texto ("10.50") para no perder exactitud."""
        return {"sku": self.sku, "quantity": self.quantity, "unit_price": str(self.unit_price)}


@dataclass
class Order:
    customer_email: str
    items: list[OrderItem]
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: OrderStatus = OrderStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    # Pago con tarjeta simulado. Solo se guarda el token de prueba, la marca y los últimos 4 dígitos: nunca el número
    # completo ni el CVV. card_last4 None = pedido automático (simulador, escenarios, POST /orders) sin tarjeta
    payment_token: str | None = None
    card_brand: str | None = None
    card_last4: str | None = None
    payment_status: PaymentStatus | None = PaymentStatus.PENDING   # None: pedido anterior a los pagos con tarjeta
    payment_reason: str | None = None
    payment_failures: int = 0       # cobros que fallaron por un error temporal del proveedor (se reintentan)

    def __post_init__(self):
        if not self.items:
            raise ValueError("El pedido debe tener al menos un producto")

    @property
    def card_label(self) -> str | None:
        """Lo que se muestra de la tarjeta: "Visa •••• 1111"."""
        return f"{self.card_brand} •••• {self.card_last4}" if self.card_last4 else None

    @property
    def total(self) -> Decimal:
        return money(sum((i.quantity * i.unit_price for i in self.items), Decimal(0)))
