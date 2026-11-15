"""Catálogo leído de catalogo.json. La app ya no vende desde aquí: el stock real vive en la tabla products
(PostgresProductRepository). Este archivo queda como semilla y para las pruebas y el mailer (solo nombres)."""
from pathlib import Path

from app.database.seed import CATALOG_FILE, read_catalog
from app.repositories.interfaces import ProductRepository
from app.models.product import Product


def to_product(p: dict) -> Product:
    return Product(**{**p, "related": tuple(p.get("related") or ())})


def load_products(path: Path = CATALOG_FILE) -> list[Product]:
    return [to_product(p) for p in read_catalog(path)]


class InMemoryProductRepository(ProductRepository):
    def __init__(self, products: list[Product] | None = None):
        self._by_sku = {p.sku: p for p in (products if products is not None else load_products())}

    def all(self) -> list[Product]:
        return list(self._by_sku.values())

    def get(self, sku: str) -> Product | None:
        return self._by_sku.get(sku)
