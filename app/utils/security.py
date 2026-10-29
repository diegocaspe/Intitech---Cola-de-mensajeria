"""Hash de contraseñas (PBKDF2) y tokens de sesión firmados (HMAC). Solo biblioteca estándar."""
import base64
import hashlib
import hmac
import json
import secrets
import time

ITERATIONS = 200_000


class PasswordHasher:
    def hash(self, password: str) -> str:
        salt = secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), ITERATIONS).hex()
        return f"pbkdf2_sha256${ITERATIONS}${salt}${digest}"

    def verify(self, password: str, stored: str) -> bool:
        try:
            _, iterations, salt, digest = stored.split("$")
        except ValueError:
            return False
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), int(iterations)).hex()
        return hmac.compare_digest(candidate, digest)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class TokenSigner:
    """Token = datos en base64 + firma HMAC. Si alguien lo modifica (p. ej. se pone role=admin), la firma no coincide."""

    def __init__(self, secret: str):
        self._key = secret.encode()

    def sign(self, payload: dict, ttl_seconds: int) -> str:
        body = _b64(json.dumps({**payload, "exp": int(time.time()) + ttl_seconds}).encode())
        return f"{body}.{self._mac(body)}"

    def verify(self, token: str | None) -> dict | None:
        if not token or "." not in token:
            return None
        body, mac = token.rsplit(".", 1)
        if not hmac.compare_digest(mac, self._mac(body)):
            return None
        try:
            data = json.loads(_unb64(body))
        except ValueError:
            return None
        return data if data.get("exp", 0) > time.time() else None

    def _mac(self, body: str) -> str:
        return _b64(hmac.new(self._key, body.encode(), hashlib.sha256).digest())
