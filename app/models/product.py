from dataclasses import dataclass, field
from decimal import Decimal

from app.models.order import money


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    category: str
    price: Decimal          # precio Black Friday
    list_price: Decimal     # precio normal (tachado)
    rating: float
    reviews: int
    stock: int
    image: str
    description: str
    related: tuple[str, ...] = field(default_factory=tuple)  # cross-selling: "comprados juntos"

    def __post_init__(self):
        # el catálogo JSON trae floats: se convierten a dinero exacto (frozen obliga a usar object.__setattr__)
        object.__setattr__(self, "price", money(self.price))
        object.__setattr__(self, "list_price", money(self.list_price))

    @property
    def discount_pct(self) -> int:
        return round((1 - self.price / self.list_price) * 100)
