"""Semilla del catálogo: catalogo.json solo se usa para llenar la tabla products la primera vez (y para «Restablecer
stock»). Desde ahí, el stock vive en PostgreSQL."""
import json
from decimal import Decimal
from pathlib import Path

from psycopg.types.json import Jsonb

CATALOG_FILE = Path(__file__).parent / "catalogo.json"

PRODUCT_COLS = ("sku, name, category, price, list_price, rating, reviews, stock, image, description, related, "
                "pago_yape_real")


def read_catalog(path: Path = CATALOG_FILE) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def product_row(p: dict) -> tuple:
    """Una fila de products en el orden de PRODUCT_COLS. Los precios pasan por str: dinero exacto, sin floats."""
    return (p["sku"], p["name"], p["category"], Decimal(str(p["price"])), Decimal(str(p["list_price"])),
            float(p["rating"]), int(p["reviews"]), int(p["stock"]), p["image"], p["description"],
            Jsonb(list(p.get("related", []))), bool(p.get("pago_yape_real", False)))


def seed_products(conn) -> int:
    """Si la tabla está vacía la llena desde catalogo.json. Corre dentro de la transacción de init_schema, que ya tomó
    el candado del esquema: api, relay, worker y mailer no siembran a la vez. ON CONFLICT: idempotente igual."""
    if conn.execute("SELECT EXISTS (SELECT 1 FROM products)").fetchone()[0]:
        return 0
    rows = [product_row(p) for p in read_catalog()]
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO products ({PRODUCT_COLS}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (sku) DO NOTHING", rows)
    return len(rows)
