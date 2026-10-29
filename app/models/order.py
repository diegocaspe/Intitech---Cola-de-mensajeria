from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import uuid


class OrderStatus(str, Enum):
    PENDING = "PENDING"        # guardada en BD, aún no confirmada por el broker / sin procesar
    PUBLISHED = "PUBLISHED"    # el broker confirmó la recepción del mensaje
    PROCESSED = "PROCESSED"    # el worker la procesó correctamente
    FAILED = "FAILED"          # agotó reintentos, el mensaje fue a la DLQ


@dataclass
class OrderItem:
    sku: str
    quantity: int
    unit_price: float

    def __post_init__(self):
        if self.quantity <= 0:
            raise ValueError("La cantidad debe ser mayor a 0")
        if self.unit_price < 0:
            raise ValueError("El precio no puede ser negativo")


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
    def total(self) -> float:
        return round(sum(i.quantity * i.unit_price for i in self.items), 2)
