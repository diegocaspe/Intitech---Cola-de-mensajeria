import pytest

from app.services.auth_service import AuthError, AuthService
from app.repositories.interfaces import UserRepository
from app.models.user import Role, User
from app.utils.security import PasswordHasher, TokenSigner


class FakeUsers(UserRepository):
    def __init__(self):
        self.by_email = {}

    def get_by_email(self, email):
        return self.by_email.get(email)

    def add(self, name, email, password_hash, role):
        if email in self.by_email:
            raise ValueError("duplicado")
        user = User(len(self.by_email) + 1, name, email, password_hash, role)
        self.by_email[email] = user
        return user

    def update_credentials(self, email, password_hash, role):
        u = self.by_email[email]
        u.password_hash, u.role = password_hash, role


def make():
    return AuthService(FakeUsers(), PasswordHasher())


def test_register_and_login():
    auth = make()
    user = auth.register("Ana", "Ana@Mail.com", "secreta123")
    assert user.role == Role.CUSTOMER and user.email == "ana@mail.com"
    assert user.password_hash != "secreta123"                 # nunca se guarda en texto plano
    assert auth.authenticate("ana@mail.com", "secreta123").id == user.id


@pytest.mark.parametrize("name,email,pw", [
    ("", "a@b.com", "secreta123"), ("Ana", "no-es-correo", "secreta123"), ("Ana", "a@b.com", "corta"),
])
def test_register_rejects_invalid(name, email, pw):
    with pytest.raises(AuthError):
        make().register(name, email, pw)


def test_duplicate_email_and_wrong_password():
    auth = make()
    auth.register("Ana", "a@b.com", "secreta123")
    with pytest.raises(AuthError):
        auth.register("Otra", "a@b.com", "secreta123")
    with pytest.raises(AuthError):
        auth.authenticate("a@b.com", "incorrecta")


def test_ensure_admin_creates_and_updates_password():
    auth = make()
    auth.ensure_admin("admin@tienda.com", "Clave1234")
    assert auth.authenticate("admin@tienda.com", "Clave1234").is_admin
    auth.ensure_admin("admin@tienda.com", "Nueva5678")
    assert auth.authenticate("admin@tienda.com", "Nueva5678").is_admin


def test_token_cannot_be_tampered():
    signer = TokenSigner("clave-secreta")
    token = signer.sign({"email": "ana@mail.com", "role": "customer"}, ttl_seconds=60)
    assert signer.verify(token)["role"] == "customer"
    body, mac = token.rsplit(".", 1)
    forged = signer.sign({"email": "ana@mail.com", "role": "admin"}, 60).rsplit(".", 1)[0] + "." + mac
    assert signer.verify(forged) is None                       # cambiar el rol invalida la firma
    assert TokenSigner("otra-clave").verify(token) is None
    assert signer.verify(signer.sign({"x": 1}, ttl_seconds=-1)) is None   # expirado
