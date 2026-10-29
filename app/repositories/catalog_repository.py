"""Catálogo de productos leído de catalogo.json (en un sistema real vendría de su propia tabla o servicio)."""
import json
from pathlib import Path

from app.repositories.interfaces import ProductRepository
from app.models.product import Product

CATALOG_FILE = Path(__file__).parent.parent / "database" / "catalogo.json"


def load_products(path: Path = CATALOG_FILE) -> list[Product]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [Product(**{**p, "related": tuple(p["related"])}) for p in data]


class InMemoryProductRepository(ProductRepository):
    def __init__(self, products: list[Product] | None = None):
        self._by_sku = {p.sku: p for p in (products if products is not None else load_products())}

    def all(self) -> list[Product]:
        return list(self._by_sku.values())

    def get(self, sku: str) -> Product | None:
        return self._by_sku.get(sku)
