"""Correos automáticos: eventos en el outbox, tokens de un solo uso, idempotencia del mailer y comprobantes.
Todo con SQLite y FakeEmailSender: no necesitan PostgreSQL, RabbitMQ ni Gmail."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app import config
from app.messaging import rabbitmq
from app.messaging.email_senders import FakeEmailSender
from app.messaging.rabbitmq import EmailConsumer
from app.models.email import EmailEvent, EmailKind, TokenResult
from app.models.user import EmailStatus, Role
from app.repositories.sqlite_repository import (SqliteOrderRepository, SqliteOutbox, SqliteProductRepository,
                                                SqliteSentEmails, SqliteUserRepository)
from app.services import email_templates
from app.services.auth_service import AuthService, hash_token
from app.services.notification_service import NotificationService
from app.services.order_service import OrderService
from app.utils.security import PasswordHasher
from tests.test_consumer import FakeChannel
from tests.test_order_service import FakePublisher, make_product

ITEMS = [{"sku": "A1", "quantity": 2, "unit_price": "10.00"}, {"sku": "B2", "quantity": 1, "unit_price": "5.50"}]


class Clock:
    def __init__(self):
        self.now = datetime(2026, 11, 5, 20, 30, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, **delta):
        self.now += timedelta(**delta)


class Publisher(FakePublisher):
    def __init__(self):
        super().__init__()
        self.emails = []

    def publish_email(self, payload):
        self.emails.append(payload)


@pytest.fixture
def env():
    repo = SqliteOrderRepository(":memory:")
    SqliteProductRepository(repo, [make_product("A1"), make_product("B2")])   # stock para los pedidos de prueba
    outbox, users, clock = SqliteOutbox(repo), SqliteUserRepository(repo), Clock()
    pub = Publisher()
    return SimpleNamespace(
        repo=repo, outbox=outbox, users=users, clock=clock, pub=pub,
        auth=AuthService(users, PasswordHasher(), outbox, clock=clock),
        orders=OrderService(repo, pub, outbox, users),
    )


def last_token(env, kind: EmailKind) -> str:
    return env.outbox.events(kind.value)[-1]["payload"]["data"]["token"]


# --- registro y verificación -------------------------------------------------------------------------------------
def test_register_writes_welcome_event_in_the_outbox(env):
    user = env.auth.register("Ana Pérez", "Ana@Mail.com", "secreta123")
    assert user.email_status == EmailStatus.PENDING
    [ev] = env.outbox.events(EmailKind.REGISTRO.value)
    p = ev["payload"]
    assert p["to"] == "ana@mail.com" and p["name"] == "Ana Pérez" and p["email_id"]
    assert ev["published_at"] is None                          # lo publica el relay, no el registro
    assert env.outbox.pending_email_count() == 1
    assert env.outbox.pending_count() == 0                     # los contadores de pedidos del panel no cambian
    # en la BD solo está el hash del token, nunca el token
    [(stored,)] = env.repo._run("SELECT token_hash FROM email_tokens")
    assert stored == hash_token(p["data"]["token"]) != p["data"]["token"]


def test_relay_publishes_email_events_to_the_email_queue(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    assert env.orders.relay_outbox() == 1
    assert env.pub.sent == [] and len(env.pub.emails) == 1     # no va a la cola de pedidos
    assert env.pub.emails[0]["kind"] == "email.registro"
    assert env.outbox.pending_email_count() == 0


def test_verification_token_works_once(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    token = last_token(env, EmailKind.REGISTRO)
    assert env.auth.verify_email(token) == TokenResult.OK
    assert env.users.get_by_email("ana@mail.com").email_status == EmailStatus.VERIFIED
    assert env.auth.verify_email(token) == TokenResult.USED    # reutilizado: rechazado


def test_expired_verification_token_is_rejected(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    token = last_token(env, EmailKind.REGISTRO)
    env.clock.advance(hours=24, seconds=1)
    assert env.auth.verify_email(token) == TokenResult.EXPIRED
    assert env.users.get_by_email("ana@mail.com").email_status == EmailStatus.PENDING
    assert env.auth.verify_email("token-inventado") == TokenResult.INVALID


def test_resend_verification_gives_a_new_link(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    assert env.auth.resend_verification("ana@mail.com") is False        # menos de un minuto: no inunda la bandeja
    env.clock.advance(hours=25)
    assert env.auth.resend_verification("ana@mail.com") is True
    assert env.auth.verify_email(last_token(env, EmailKind.REGISTRO)) == TokenResult.OK


# --- recuperación de contraseña -----------------------------------------------------------------------------------
def test_password_reset_changes_the_password_once(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    env.auth.request_password_reset("ANA@mail.com ")
    token = last_token(env, EmailKind.RECUPERAR)
    assert env.auth.reset_password(token, "nuevaClave9") == TokenResult.OK
    assert env.auth.authenticate("ana@mail.com", "nuevaClave9")
    with pytest.raises(ValueError):
        env.auth.authenticate("ana@mail.com", "secreta123")
    assert env.auth.reset_password(token, "otraClave99") == TokenResult.USED     # reutilizado: rechazado
    # abrir el enlace del correo también demuestra que el correo es suyo
    assert env.users.get_by_email("ana@mail.com").email_status == EmailStatus.VERIFIED


def test_expired_reset_token_is_rejected(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    env.auth.request_password_reset("ana@mail.com")
    token = last_token(env, EmailKind.RECUPERAR)
    env.clock.advance(minutes=61)
    assert env.auth.reset_password(token, "nuevaClave9") == TokenResult.EXPIRED
    assert env.auth.authenticate("ana@mail.com", "secreta123")          # la contraseña no cambió


def test_reset_is_silent_for_unknown_and_test_accounts_and_throttled(env):
    env.users.add("Prueba", "prueba@mail.com", PasswordHasher().hash("secreta123"), Role.CUSTOMER)   # cuenta legacy
    env.auth.request_password_reset("nadie@mail.com")
    env.auth.request_password_reset("prueba@mail.com")
    assert env.outbox.events(EmailKind.RECUPERAR.value) == []
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    env.auth.request_password_reset("ana@mail.com")
    env.auth.request_password_reset("ana@mail.com")                    # segundos después: no se repite
    assert len(env.outbox.events(EmailKind.RECUPERAR.value)) == 1


def test_reset_rejects_short_passwords(env):
    with pytest.raises(ValueError):
        env.auth.reset_password("x", "corta")


# --- aviso de inicio de sesión -------------------------------------------------------------------------------------
def test_login_notice_only_for_real_customers(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    env.auth.authenticate("ana@mail.com", "secreta123")
    [ev] = env.outbox.events(EmailKind.LOGIN.value)
    assert ev["payload"]["to"] == "ana@mail.com" and ev["payload"]["data"]["at"] == env.clock.now.isoformat()

    env.users.add("Prueba", "prueba@mail.com", PasswordHasher().hash("secreta123"), Role.CUSTOMER)   # legacy
    env.auth.ensure_admin("admin@tienda.com", "Admin12345")
    env.auth.authenticate("prueba@mail.com", "secreta123")
    env.auth.authenticate("admin@tienda.com", "Admin12345")
    assert len(env.outbox.events(EmailKind.LOGIN.value)) == 1


# --- comprobante de compra ------------------------------------------------------------------------------------------
def test_confirmed_order_generates_a_receipt_in_the_same_step(env):
    env.auth.register("Ana", "ana@mail.com", "secreta123")
    order = env.orders.create_order("ana@mail.com", ITEMS)
    env.orders.process_order(order.id)
    [ev] = env.outbox.events(EmailKind.COMPRA_CONFIRMADA.value)
    p = ev["payload"]
    assert p["to"] == "ana@mail.com" and p["data"]["order_id"] == order.id
    assert p["data"]["total"] == "25.50"
    assert p["data"]["items"] == [{"sku": "A1", "quantity": 2, "unit_price": "10.00"},
                                  {"sku": "B2", "quantity": 1, "unit_price": "5.50"}]
    env.orders.process_order(order.id)                         # mensaje duplicado: no hay segundo comprobante
    assert len(env.outbox.events(EmailKind.COMPRA_CONFIRMADA.value)) == 1


def test_simulator_admin_and_test_accounts_get_no_receipt(env):
    env.auth.ensure_admin("admin@tienda.com", "Admin12345")
    env.users.add("Prueba", "prueba@mail.com", "x", Role.CUSTOMER)     # legacy
    for email in ("ana123@mail.com", "admin@tienda.com", "prueba@mail.com"):   # el simulador inventa correos
        order = env.orders.create_order(email, ITEMS)
        env.orders.process_order(order.id)
    assert env.outbox.events(EmailKind.COMPRA_CONFIRMADA.value) == []


def test_receipt_template_has_the_required_details():
    ev = EmailEvent(EmailKind.COMPRA_CONFIRMADA, "ana@mail.com", "Ana Pérez", {
        "order_id": "abcd1234-0000", "created_at": "2026-11-05T20:30:00+00:00", "total": "25.50",
        "items": [{"sku": "A1", "quantity": 2, "unit_price": "10.00"}, {"sku": "B2", "quantity": 1, "unit_price": "5.50"}]})
    subject, html, text = email_templates.render(ev, "Inti Tech", "http://x", {"A1": "Audífonos", "B2": "Cable"}.get)
    assert "ABCD1234" in subject
    for part in ("Audífonos", "2 × S/ 10.00", "S/ 20.00", "S/ 25.50", "05/11/2026, 15:30",   # 20:30 UTC = 15:30 Lima
                 "Este correo es un comprobante de compra, no una boleta electrónica"):
        assert part in html
    assert "S/ 25.50" in text and "no una boleta electrónica" in text


def test_templates_escape_user_data():
    ev = EmailEvent(EmailKind.LOGIN, "a@b.com", "<script>alert(1)</script>", {"at": "2026-11-05T20:30:00+00:00"})
    _, html, _ = email_templates.render(ev, "Inti Tech", "http://x")
    assert "<script>" not in html and "&lt;script&gt;" in html


# --- mailer: idempotencia, reintentos y DLQ -------------------------------------------------------------------------
def make_notifier(fail_times=0):
    repo = SqliteOrderRepository(":memory:")
    sender = FakeEmailSender(fail_times)
    return NotificationService(SqliteSentEmails(repo), sender, "Inti Tech", "http://tienda"), sender


def login_payload():
    return EmailEvent(EmailKind.LOGIN, "ana@mail.com", "Ana", {"at": "2026-11-05T20:30:00+00:00"}).to_payload()


def test_duplicate_email_id_is_not_sent_twice():
    svc, sender = make_notifier()
    payload = login_payload()
    assert svc.deliver(payload) is True
    assert svc.deliver(dict(payload)) is False                 # RabbitMQ lo entregó otra vez
    assert len(sender.sent) == 1 and sender.sent[0]["to"] == "ana@mail.com"


def test_verification_link_uses_the_public_url():
    svc, sender = make_notifier()
    svc.deliver(EmailEvent(EmailKind.REGISTRO, "ana@mail.com", "Ana", {"token": "TOK"}).to_payload())
    assert "http://tienda/api/auth/verify?token=TOK" in sender.sent[0]["html"]


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(rabbitmq.time, "sleep", lambda _: None)


def deliver(consumer, payload, retries=0, ch=None):
    ch = ch or FakeChannel()
    consumer._on_message(ch, SimpleNamespace(delivery_tag=3), SimpleNamespace(headers={"retries": retries}),
                         json.dumps(payload).encode())
    return ch


def test_mailer_acks_after_sending(no_sleep):
    svc, sender = make_notifier()
    ch = deliver(EmailConsumer(svc), login_payload())
    assert ch.acks == [3] and ch.nacks == [] and len(sender.sent) == 1


def test_smtp_failure_waits_in_the_retry_queue(no_sleep):
    svc, sender = make_notifier(fail_times=1)
    ch = deliver(EmailConsumer(svc), login_payload())
    assert ch.published[0][0] == config.EMAIL_RETRY_QUEUES[0]  # correos.retry.2s
    assert ch.published[0][1]["retries"] == 1 and "SMTP" in ch.published[0][1]["x-last-error"]
    assert ch.acks == [3] and sender.sent == []


def test_third_smtp_failure_goes_to_the_dlq(no_sleep):
    svc, _ = make_notifier(fail_times=5)
    ch = deliver(EmailConsumer(svc), login_payload(), retries=config.MAX_RETRIES)
    assert ch.nacks == [(3, False)] and ch.published == []     # nack sin requeue: RabbitMQ lo manda a correos.dlq


def test_unreadable_email_message_goes_to_the_dlq(no_sleep):
    svc, _ = make_notifier()
    ch = FakeChannel()
    EmailConsumer(svc)._on_message(ch, SimpleNamespace(delivery_tag=3), SimpleNamespace(headers={}), b"no-json")
    assert ch.nacks == [(3, False)]
