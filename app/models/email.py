"""Evento de correo: lo que un service deja en el outbox para que el mailer lo envíe después.

Solo datos (sin tecnología): el destinatario, el tipo de correo y lo necesario para armar la plantilla.
email_id identifica el correo de punta a punta: RabbitMQ entrega "al menos una vez" y el mailer lo usa para no
enviar dos veces el mismo."""
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class TokenPurpose(str, Enum):
    VERIFY = "verify"   # verificar el correo de una cuenta nueva
    RESET = "reset"     # restablecer la contraseña


class TokenResult(str, Enum):
    OK = "ok"
    EXPIRED = "expirado"
    USED = "usado"
    INVALID = "invalido"


@dataclass
class EmailToken:
    """Token de un solo uso enviado por correo. Solo se guarda su hash: quien lea la BD no puede usarlo."""
    token_hash: str
    purpose: TokenPurpose
    expires_at: datetime
    created_at: datetime


class EmailKind(str, Enum):
    REGISTRO = "email.registro"                    # bienvenida + enlace de verificación
    LOGIN = "email.login"                          # aviso de nuevo inicio de sesión
    COMPRA_CONFIRMADA = "email.compra_confirmada"  # comprobante de compra
    RECUPERAR = "email.recuperar"                  # enlace para restablecer la contraseña


@dataclass
class EmailEvent:
    kind: EmailKind
    to: str
    name: str
    data: dict = field(default_factory=dict)
    email_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_payload(self) -> dict:
        return {"email_id": self.email_id, "kind": self.kind.value, "to": self.to, "name": self.name,
                "data": self.data, "created_at": self.created_at}

    @classmethod
    def from_payload(cls, p: dict) -> "EmailEvent":
        return cls(EmailKind(p["kind"]), p["to"], p.get("name", ""), p.get("data") or {}, p["email_id"],
                   p.get("created_at") or datetime.now(timezone.utc).isoformat())
