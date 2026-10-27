import re

from app.application.ports import UserRepository
from app.domain.user import Role, User

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MIN_PASSWORD = 8


class AuthError(ValueError):
    pass


class AuthService:
    """Registro e inicio de sesión. El hasher se inyecta (infraestructura) para no atar la lógica a un algoritmo."""

    def __init__(self, users: UserRepository, hasher):
        self.users = users
        self.hasher = hasher

    def register(self, name: str, email: str, password: str) -> User:
        name, email = name.strip(), email.strip().lower()
        if not name:
            raise AuthError("Ingresa tu nombre")
        if not EMAIL_RE.match(email):
            raise AuthError("Correo inválido")
        if len(password) < MIN_PASSWORD:
            raise AuthError(f"La contraseña debe tener al menos {MIN_PASSWORD} caracteres")
        if self.users.get_by_email(email):
            raise AuthError("Ya existe una cuenta con ese correo")
        try:
            return self.users.add(name, email, self.hasher.hash(password), Role.CUSTOMER)
        except ValueError:
            raise AuthError("Ya existe una cuenta con ese correo")

    def authenticate(self, email: str, password: str) -> User:
        user = self.users.get_by_email(email.strip().lower())
        # mismo mensaje en ambos casos: no revelar qué correos están registrados
        if not user or not self.hasher.verify(password, user.password_hash):
            raise AuthError("Correo o contraseña incorrectos")
        return user

    def ensure_admin(self, email: str, password: str, name: str = "Administrador") -> None:
        """Crea el administrador al arrancar, o actualiza su contraseña con la de la configuración."""
        email = email.strip().lower()
        if self.users.get_by_email(email):
            self.users.update_credentials(email, self.hasher.hash(password), Role.ADMIN)
        else:
            self.users.add(name, email, self.hasher.hash(password), Role.ADMIN)
