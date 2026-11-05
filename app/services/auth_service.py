import hashlib
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.repositories.interfaces import Outbox, UserRepository
from app.models.email import EmailEvent, EmailKind, EmailToken, TokenPurpose, TokenResult
from app.models.user import Role, User

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MIN_PASSWORD = 8


class AuthError(ValueError):
    pass


def hash_token(token: str) -> str:
    """En la BD solo se guarda el hash del token: quien la lea no puede usar los enlaces."""
    return hashlib.sha256(token.encode()).hexdigest()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AuthService:
    """Registro, inicio de sesión, verificación de correo y recuperación de contraseña. El hasher se inyecta
    (infraestructura) para no atar la lógica a un algoritmo.

    Con outbox, los correos se piden dejando un evento en el outbox (en la misma transacción que la operación): este
    service nunca envía correos. Sin outbox (pruebas antiguas) se comporta como antes, sin correos."""

    def __init__(self, users: UserRepository, hasher, outbox: Outbox | None = None,
                 verify_hours: int = 24, reset_minutes: int = 60, reset_throttle_s: int = 60,
                 clock: Callable[[], datetime] = _utcnow):
        self.users = users
        self.hasher = hasher
        self.outbox = outbox
        self.verify_ttl = timedelta(hours=verify_hours)
        self.reset_ttl = timedelta(minutes=reset_minutes)
        self.reset_throttle = timedelta(seconds=reset_throttle_s)
        self.clock = clock

    def _new_token(self, purpose: TokenPurpose, ttl: timedelta) -> tuple[str, EmailToken]:
        token = secrets.token_urlsafe(32)
        now = self.clock()
        return token, EmailToken(hash_token(token), purpose, now + ttl, now)

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
            if self.outbox is None:
                return self.users.add(name, email, self.hasher.hash(password), Role.CUSTOMER)
            # usuario + token de verificación + correo de bienvenida: una sola transacción
            token, record = self._new_token(TokenPurpose.VERIFY, self.verify_ttl)
            event = EmailEvent(EmailKind.REGISTRO, email, name,
                               {"token": token, "expires_hours": int(self.verify_ttl.total_seconds() // 3600)})
            return self.users.add_customer_with_email(name, email, self.hasher.hash(password), record, event)
        except ValueError:
            raise AuthError("Ya existe una cuenta con ese correo")

    def authenticate(self, email: str, password: str) -> User:
        user = self.users.get_by_email(email.strip().lower())
        # mismo mensaje en ambos casos: no revelar qué correos están registrados
        if not user or not self.hasher.verify(password, user.password_hash):
            raise AuthError("Correo o contraseña incorrectos")
        if self.outbox is not None and user.receives_emails:
            self.outbox.add_event(EmailEvent(EmailKind.LOGIN, user.email, user.name,
                                             {"at": self.clock().isoformat()}))
        return user

    def verify_email(self, token: str) -> TokenResult:
        if not token:
            return TokenResult.INVALID
        return self.users.consume_token(hash_token(token), TokenPurpose.VERIFY, self.clock())

    def resend_verification(self, email: str) -> bool:
        """Nuevo enlace de verificación (el anterior pudo vencer). False si la cuenta no lo necesita o se pidió hace
        menos de un minuto."""
        user = self.users.get_by_email(email)
        if self.outbox is None or not user or not user.receives_emails or user.email_status.value != "pending":
            return False
        last = self.users.last_token_at(user.id, TokenPurpose.VERIFY)
        if last and self.clock() - last < self.reset_throttle:
            return False
        token, record = self._new_token(TokenPurpose.VERIFY, self.verify_ttl)
        self.users.add_token_with_email(user.id, record, EmailEvent(
            EmailKind.REGISTRO, user.email, user.name,
            {"token": token, "expires_hours": int(self.verify_ttl.total_seconds() // 3600), "resend": True}))
        return True

    def request_password_reset(self, email: str) -> None:
        """Siempre responde igual (exista o no la cuenta) para no revelar qué correos están registrados. Solo los
        clientes reales reciben el enlace; como mucho uno por minuto para no inundar su bandeja."""
        user = self.users.get_by_email(email.strip().lower())
        if self.outbox is None or not user or not user.receives_emails:
            return
        last = self.users.last_token_at(user.id, TokenPurpose.RESET)
        if last and self.clock() - last < self.reset_throttle:
            return
        token, record = self._new_token(TokenPurpose.RESET, self.reset_ttl)
        self.users.add_token_with_email(user.id, record, EmailEvent(
            EmailKind.RECUPERAR, user.email, user.name,
            {"token": token, "expires_minutes": int(self.reset_ttl.total_seconds() // 60)}))

    def reset_password(self, token: str, new_password: str) -> TokenResult:
        if len(new_password) < MIN_PASSWORD:
            raise AuthError(f"La contraseña debe tener al menos {MIN_PASSWORD} caracteres")
        if not token:
            return TokenResult.INVALID
        return self.users.consume_token(hash_token(token), TokenPurpose.RESET, self.clock(),
                                        self.hasher.hash(new_password))

    def ensure_admin(self, email: str, password: str, name: str = "Administrador") -> None:
        """Crea el administrador al arrancar, o actualiza su contraseña con la de la configuración."""
        email = email.strip().lower()
        if self.users.get_by_email(email):
            self.users.update_credentials(email, self.hasher.hash(password), Role.ADMIN)
        else:
            self.users.add(name, email, self.hasher.hash(password), Role.ADMIN)
