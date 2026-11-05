from dataclasses import dataclass
from enum import Enum


class Role(str, Enum):
    CUSTOMER = "customer"
    ADMIN = "admin"


class EmailStatus(str, Enum):
    LEGACY = "legacy"        # cuenta anterior a los correos automáticos (de prueba): no recibe correos ni ve el aviso
    PENDING = "pending"      # registrada desde la tienda, aún no abre el enlace de verificación
    VERIFIED = "verified"


@dataclass
class User:
    id: int
    name: str
    email: str
    password_hash: str
    role: Role = Role.CUSTOMER
    email_status: EmailStatus = EmailStatus.LEGACY

    @property
    def is_admin(self) -> bool:
        return self.role == Role.ADMIN

    @property
    def receives_emails(self) -> bool:
        """Solo clientes reales registrados desde la tienda: ni el administrador ni las cuentas de prueba."""
        return self.role == Role.CUSTOMER and self.email_status != EmailStatus.LEGACY
