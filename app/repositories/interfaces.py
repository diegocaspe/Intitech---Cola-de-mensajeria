"""Puertos: contratos que la capa de aplicación exige a la infraestructura."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Callable, NamedTuple

from app.models.email import EmailEvent, EmailToken, TokenPurpose, TokenResult
from app.models.order import Order, OrderStatus
from app.models.product import Product
from app.models.user import Role, User


class ProcessResult(NamedTuple):
    """Lo que pasó al confirmar un pedido en el worker."""
    status: OrderStatus | None      # PROCESSED, REJECTED (no alcanzó el stock) o None (ya tenía resultado final)
    out_of_stock: str | None = None  # SKU que no alcanzó


class OrderRepository(ABC):
    @abstractmethod
    def save(self, order: Order) -> None:
        """Guarda el pedido Y su evento OrderCreated en el outbox, en una sola transacción (todo o nada)."""

    @abstractmethod
    def get(self, order_id: str) -> Order | None: ...

    @abstractmethod
    def list(self, status: OrderStatus | None = None, limit: int | None = None,
             older_than_s: float = 0) -> list[Order]:
        """older_than_s: solo pedidos creados hace más de N segundos."""

    @abstractmethod
    def recent(self, limit: int) -> list[Order]:
        """Los pedidos más nuevos primero."""

    @abstractmethod
    def list_by_email(self, email: str, limit: int) -> list[Order]:
        """Pedidos de un cliente, los más nuevos primero."""

    @abstractmethod
    def update_status(self, order_id: str, status: OrderStatus) -> None: ...

    @abstractmethod
    def mark_processed(self, order_id: str, receipt: EmailEvent | None = None,
                       stock: list[tuple[str, int]] = ()) -> ProcessResult:
        """En UNA transacción: descuenta el stock de cada (sku, cantidad), en orden de sku, con
        UPDATE ... WHERE stock >= cantidad; marca el pedido PROCESSED y deja su comprobante en el outbox.
        Si un ítem no alcanza, deshace todos los descuentos y el pedido queda REJECTED (todo o nada); si su pago ya
        estaba APPROVED pasa a REFUNDED (reembolso simulado) en esa misma transacción.
        No hace nada si el pedido ya tenía resultado final (un duplicado nunca descuenta dos veces)."""

    # --- pago con tarjeta simulado (los usa el worker; ver OrderService.process_order) ---------------------------
    @abstractmethod
    def stock_of(self, skus: list[str]) -> dict[str, int]:
        """Stock actual de cada SKU según la tabla products, SIN descontar nada (los que no existen no aparecen)."""

    @abstractmethod
    def add_payment_failure(self, order_id: str) -> None:
        """Suma 1 a payment_failures: el proveedor tuvo un error temporal y el cobro se reintentará."""

    @abstractmethod
    def mark_payment_approved(self, order_id: str) -> None:
        """payment_status → APPROVED, en su propia transacción y ANTES de descontar el stock: si RabbitMQ entrega el
        mensaje otra vez, el worker ve que ya se cobró y no vuelve a cobrar."""

    @abstractmethod
    def mark_payment_declined(self, order_id: str, reason: str) -> None:
        """Cobro rechazado: payment_status → DECLINED y el pedido → REJECTED, en un solo UPDATE (solo si aún no tenía
        resultado final). El stock no se toca."""

    @abstractmethod
    def reject_out_of_stock(self, order_id: str, reason: str) -> None:
        """Sin stock ANTES de cobrar: el pedido → REJECTED con el motivo; el pago no se toca (no se cobró nada)."""

    @abstractmethod
    def payment_counts(self) -> dict[str, int]:
        """Pedidos por payment_status (APPROVED, DECLINED, REFUNDED, PENDING)."""

    def statuses(self, order_ids: list[str]) -> dict[str, str]:
        """Estado de cada pedido de la lista (los escenarios automáticos siguen solo sus pedidos de prueba)."""
        found = {i: self.get(i) for i in order_ids}
        return {i: o.status.value for i, o in found.items() if o}

    @abstractmethod
    def requeue_failed(self, order_id: str) -> bool:
        """FAILED -> PUBLISHED al reprocesar desde la DLQ. Nunca toca un pedido con resultado final (PROCESSED o
        REJECTED): si no, un duplicado en la DLQ lo procesaría dos veces. True si lo cambió."""

    @abstractmethod
    def mark_published(self, order_id: str) -> None:
        """PENDING -> PUBLISHED, sin pisar un estado más avanzado (el worker pudo ser más rápido)."""

    @abstractmethod
    def count_by_status(self) -> dict[str, int]: ...

    @abstractmethod
    def revenue(self, status: OrderStatus) -> Decimal: ...

    @abstractmethod
    def count_ahead(self, order: Order) -> int:
        """Pedidos sin procesar creados antes que este (su posición aproximada en la cola)."""

    @abstractmethod
    def count_since(self, seconds: float) -> int:
        """Pedidos creados en los últimos N segundos."""

    @abstractmethod
    def stale_published(self, older_than_s: float, limit: int = 1000) -> list[str]:
        """Pedidos PUBLISHED cuyo mensaje se envió hace más de N s y siguen sin procesarse."""

    @abstractmethod
    def clear(self) -> None:
        """Borra pedidos y outbox."""


ORDER_EVENT = "OrderCreated"   # el resto de eventos del outbox son correos (email.*)


class Outbox(ABC):
    """Transactional Outbox: eventos guardados junto a su operación (pedido, registro...), pendientes de publicar.

    Los métodos de lectura del panel (pending_count, recent, published_stats, queue_window) cuentan solo los eventos
    de pedidos; los de correos se cuentan aparte con pending_email_count."""

    @abstractmethod
    def relay(self, publish: Callable[[str, str, dict], None], limit: int, older_than_s: float) -> int:
        """Publica eventos pendientes con publish(event_type, aggregate_id, payload) y los marca como enviados.
        Bloquea las filas que toma para que dos relays nunca publiquen el mismo evento. Se detiene en el primer
        fallo. Devuelve cuántos envió."""

    @abstractmethod
    def add_event(self, event: EmailEvent) -> None:
        """Guarda un evento de correo que no acompaña a ninguna otra escritura (p. ej. el aviso de inicio de sesión)."""

    @abstractmethod
    def pending_email_count(self) -> int: ...

    @abstractmethod
    def mark_sent(self, order_id: str) -> None: ...

    @abstractmethod
    def pending_count(self) -> int: ...

    @abstractmethod
    def recent(self, limit: int) -> list[dict]:
        """Los eventos más nuevos primero: id, aggregate_id, event_type, created_at y published_at (ISO, o None
        si aún no se publica). Solo para mostrarlos en el panel."""

    @abstractmethod
    def published_stats(self, window_s: float) -> tuple[int, int]:
        """Cuántos eventos hay publicados en total y cuántos se publicaron en los últimos window_s segundos."""

    @abstractmethod
    def queue_window(self, edge: int) -> dict:
        """Pedidos publicados y sin terminar, en orden de publicación: count y los ids de los dos extremos, head (los
        `edge` más antiguos) y tail (los `edge` más nuevos, el último es el más reciente). numbers da el número de cada
        uno (su evento en el outbox, contando desde 1) y first_event el id del primer evento. Solo para el panel."""


class ProductRepository(ABC):
    @abstractmethod
    def all(self) -> list[Product]: ...

    @abstractmethod
    def get(self, sku: str) -> Product | None: ...

    def restore_stock(self) -> int:
        """Vuelve a poner el stock de cada producto según catalogo.json (reiniciar la demo). Devuelve cuántos tocó."""
        raise NotImplementedError

    def initial_stock(self) -> dict[str, int]:
        """Stock de cada SKU según catalogo.json (lo que deja «Restablecer stock»)."""
        raise NotImplementedError


class UserRepository(ABC):
    @abstractmethod
    def get_by_email(self, email: str) -> User | None: ...

    @abstractmethod
    def add(self, name: str, email: str, password_hash: str, role: Role) -> User:
        """Debe lanzar ValueError si el correo ya existe."""

    @abstractmethod
    def update_credentials(self, email: str, password_hash: str, role: Role) -> None: ...

    # --- correos: verificación de cuenta y recuperación de contraseña ---------------------------------------
    # No son abstractos para no obligar a los repositorios que no los usan (p. ej. los de prueba más antiguos)

    def add_customer_with_email(self, name: str, email: str, password_hash: str, token: EmailToken,
                                event: EmailEvent) -> User:
        """Cliente nuevo (pendiente de verificar) + su token + el evento del correo de bienvenida en el outbox, en una
        sola transacción. Lanza ValueError si el correo ya existe."""
        raise NotImplementedError

    def add_token_with_email(self, user_id: int, token: EmailToken, event: EmailEvent) -> None:
        """Nuevo token + evento del correo que lo lleva, en una sola transacción."""
        raise NotImplementedError

    def last_token_at(self, user_id: int, purpose: TokenPurpose) -> datetime | None:
        raise NotImplementedError

    def consume_token(self, token_hash: str, purpose: TokenPurpose, now: datetime,
                      new_password_hash: str | None = None) -> TokenResult:
        """Valida y gasta el token (un solo uso) en una transacción: verify marca la cuenta como verificada; reset
        además cambia la contraseña y anula los otros enlaces de recuperación pendientes."""
        raise NotImplementedError


class OrderPublisher(ABC):
    @abstractmethod
    def publish_order_created(self, order_id: str) -> None:
        """Debe lanzar excepción si el broker no confirma el mensaje."""

    def publish_email(self, payload: dict) -> None:
        """Publica un evento de correo (persistente, con publisher confirms). Lanza excepción si no se confirma."""
        raise NotImplementedError

    def is_healthy(self) -> bool:
        return True


class EmailSendError(Exception):
    """Fallo al entregar un correo (SMTP o red): el mailer lo reintenta con espera creciente y luego va a la DLQ."""


class EmailSender(ABC):
    @abstractmethod
    def send(self, to: str, subject: str, html: str, text: str) -> None:
        """Entrega el correo. Debe lanzar EmailSendError si no se pudo."""


class SentEmailRepository(ABC):
    """Registro de correos ya enviados (por email_id): la idempotencia del mailer."""

    @abstractmethod
    def was_sent(self, email_id: str) -> bool: ...

    @abstractmethod
    def mark_sent(self, email_id: str, kind: str, to: str) -> None: ...

    @abstractmethod
    def count(self) -> int: ...
