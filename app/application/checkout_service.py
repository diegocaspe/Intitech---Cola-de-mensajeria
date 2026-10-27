from app.application.order_service import OrderService
from app.application.ports import ProductRepository
from app.domain.order import Order

MAX_QTY = 10


class CheckoutError(ValueError):
    pass


class CheckoutService:
    """Convierte un carrito en pedido. Los precios salen del catálogo, nunca del navegador."""

    def __init__(self, products: ProductRepository, orders: OrderService):
        self.products = products
        self.orders = orders

    def checkout(self, customer_email: str, lines: list[dict]) -> Order:
        if "@" not in customer_email:
            raise CheckoutError("Correo inválido")
        qty_by_sku: dict[str, int] = {}
        for line in lines:
            qty_by_sku[line["sku"]] = qty_by_sku.get(line["sku"], 0) + int(line["quantity"])
        items = []
        for sku, qty in qty_by_sku.items():
            product = self.products.get(sku)
            if product is None:
                raise CheckoutError(f"Producto desconocido: {sku}")
            if not 1 <= qty <= MAX_QTY:
                raise CheckoutError(f"Cantidad inválida para {product.name}")
            items.append({"sku": sku, "quantity": qty, "unit_price": product.price})
        if not items:
            raise CheckoutError("El carrito está vacío")
        return self.orders.create_order(customer_email, items)
