"""Puertos: contratos que la capa de aplicación exige a la infraestructura."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from app.models.order import Order, OrderStatus
from app.models.product import Product
from app.models.user import Role, User


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
    def mark_published(self, order_id: str) -> None:
        """PENDING -> PUBLISHED, sin pisar un estado más avanzado (el worker pudo ser más rápido)."""

    @abstractmethod
    def count_by_status(self) -> dict[str, int]: ...

    @abstractmethod
    def revenue(self, status: OrderStatus) -> float: ...

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


class Outbox(ABC):
    """Transactional Outbox: eventos guardados junto al pedido, pendientes de publicar en el broker."""

    @abstractmethod
    def relay(self, publish: Callable[[str], None], limit: int, older_than_s: float) -> int:
        """Publica eventos pendientes y los marca como enviados. Bloquea las filas que toma para que dos
        relays nunca publiquen el mismo evento. Se detiene en el primer fallo. Devuelve cuántos envió."""

    @abstractmethod
    def mark_sent(self, order_id: str) -> None: ...

    @abstractmethod
    def pending_count(self) -> int: ...


class ProductRepository(ABC):
    @abstractmethod
    def all(self) -> list[Product]: ...

    @abstractmethod
    def get(self, sku: str) -> Product | None: ...


class UserRepository(ABC):
    @abstractmethod
    def get_by_email(self, email: str) -> User | None: ...

    @abstractmethod
    def add(self, name: str, email: str, password_hash: str, role: Role) -> User:
        """Debe lanzar ValueError si el correo ya existe."""

    @abstractmethod
    def update_credentials(self, email: str, password_hash: str, role: Role) -> None: ...


class OrderPublisher(ABC):
    @abstractmethod
    def publish_order_created(self, order_id: str) -> None:
        """Debe lanzar excepción si el broker no confirma el mensaje."""

    def is_healthy(self) -> bool:
        return True
