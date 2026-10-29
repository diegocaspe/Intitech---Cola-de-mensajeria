from dataclasses import dataclass, field


@dataclass(frozen=True)
class Product:
    sku: str
    name: str
    category: str
    price: float          # precio Black Friday
    list_price: float     # precio normal (tachado)
    rating: float
    reviews: int
    stock: int
    image: str
    description: str
    related: tuple[str, ...] = field(default_factory=tuple)  # cross-selling: "comprados juntos"

    @property
    def discount_pct(self) -> int:
        return round((1 - self.price / self.list_price) * 100)
