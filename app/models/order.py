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

    def __post_init__(self):
        if not self.items:
            raise ValueError("El pedido debe tener al menos un producto")

    @property
    def total(self) -> Decimal:
        return money(sum((i.quantity * i.unit_price for i in self.items), Decimal(0)))
