from app.services.order_service import OrderService
from app.repositories.interfaces import ProductRepository
from app.models.order import Order
from app.services.payments import CardError, tokenize

MAX_QTY = 10


class CheckoutError(ValueError):
    pass


class CheckoutService:
    """Convierte un carrito en pedido. Los precios salen del catálogo, nunca del navegador. La tarjeta se cambia aquí
    por un token de prueba: el número completo y el CVV no pasan de este punto (no se guardan ni van a la cola)."""

    def __init__(self, products: ProductRepository, orders: OrderService):
        self.products = products
        self.orders = orders

    def checkout(self, customer_email: str, lines: list[dict], card: dict | None) -> Order:
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
        try:
            token = tokenize(card)
        except CardError as exc:   # el mensaje nunca repite lo que escribió el cliente
            raise CheckoutError(str(exc)) from None
        # el cobro NO ocurre aquí: lo hace el worker (asíncrono). La API responde 201 al instante
        return self.orders.create_order(customer_email, items, card=token)
