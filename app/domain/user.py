from dataclasses import dataclass
from enum import Enum


class Role(str, Enum):
    CUSTOMER = "customer"
    ADMIN = "admin"


@dataclass
class User:
    id: int
    name: str
    email: str
    password_hash: str
    role: Role = Role.CUSTOMER

    @property
    def is_admin(self) -> bool:
        return self.role == Role.ADMIN
