"""Pago con tarjeta SIMULADO y asíncrono: lo cobra el worker. SQLite + RabbitMQ falso: no necesitan servidores.

Cubre: tarjeta aprobada, rechazada (no descuenta stock), error temporal (reintento y luego aprobada), idempotencia
(mensaje duplicado = un solo cobro), reembolso cuando otro pedido se lleva el stock, y que la tarjeta real nunca se
acepte ni se guarde."""
import json
import threading
from datetime import date

import pytest

from app import config
from app.models.email import EmailEvent, EmailKind
from app.models.order import OrderStatus, PaymentStatus
from app.repositories.sqlite_repository import SqliteUserRepository
from app.services import email_templates
from app.services.auth_service import AuthService
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.payments import (TOKEN_APPROVED, TOKEN_DECLINED, TOKEN_FLAKY, CardError, FakePaymentProvider,
                                   tokenize)
from app.utils.security import PasswordHasher
from tests.test_consumer import deliver
from tests.test_order_service import make, make_product

APPROVED = {"holder": "Ana Pérez", "number": "4111 1111 1111 1111", "exp": "12/40", "cvv": "123"}
DECLINED = {**APPROVED, "number": "4000 0000 0000 0002"}
FLAKY = {**APPROVED, "number": "4000 0000 0000 0119"}


def shop(stock=5):
    """Tienda de prueba con un producto A1 y un cliente registrado (para el comprobante)."""
    svc, pub = make(seed=[make_product("A1", stock)])
    svc.users = SqliteUserRepository(svc.repo)
    return svc, CheckoutService(svc.products, svc)


def buy(co, card, qty=1, email="ana@mail.com"):
    return co.checkout(email, [{"sku": "A1", "quantity": qty}], card)


def stock(svc):
    return svc.products.get("A1").stock


# --- tokenización: la tarjeta real nunca entra -------------------------------------------------------------------------
def test_las_tres_tarjetas_de_prueba_se_cambian_por_su_token():
    assert tokenize(APPROVED).token == TOKEN_APPROVED
    assert tokenize(DECLINED).token == TOKEN_DECLINED
    assert tokenize(FLAKY).token == TOKEN_FLAKY
    t = tokenize({**APPROVED, "number": "4111-1111-1111-1111"})
    assert (t.brand, t.last4) == ("Visa", "1111")


@pytest.mark.parametrize("number", ["4242 4242 4242 4242", "5555 5555 5555 4444", "4111 1111 1111 111", "", "abc"])
def test_una_tarjeta_que_no_es_de_prueba_se_rechaza(number):
    with pytest.raises(CardError, match="Usa una tarjeta de prueba") as exc:
        tokenize({**APPROVED, "number": number})
    assert not number or number not in str(exc.value)              # el error no repite el número


def test_el_vencimiento_vale_hasta_el_ultimo_dia_de_su_mes():
    assert tokenize({**APPROVED, "exp": "11/26"}, today=date(2026, 11, 30)).token == TOKEN_APPROVED
    with pytest.raises(CardError, match="vencida"):
        tokenize({**APPROVED, "exp": "10/26"}, today=date(2026, 11, 1))


def test_el_checkout_con_tarjeta_real_no_crea_pedido():
    svc, co = shop()
    with pytest.raises(CheckoutError, match="Usa una tarjeta de prueba"):   # la API lo responde como 400
        buy(co, {**APPROVED, "number": "4242 4242 4242 4242"})
    assert svc.repo.count_by_status()["PENDING"] == 0


def test_no_se_guarda_ni_se_encola_el_numero_ni_el_cvv():
    svc, co = shop()
    order = buy(co, APPROVED)
    row = svc.repo._run("SELECT * FROM orders WHERE id=?", (order.id,))[0]
    dump = json.dumps([str(c) for c in row]) + json.dumps([e["payload"] for e in svc.outbox.events()])
    assert "4111111111111111" not in dump and "4111 1111 1111 1111" not in dump
    assert '"123"' not in dump and "cvv" not in dump.lower()
    stored = svc.get_order(order.id)
    assert (stored.payment_token, stored.card_brand, stored.card_last4) == (TOKEN_APPROVED, "Visa", "1111")
    assert stored.payment_status == PaymentStatus.PENDING          # la API no cobra: lo hace el worker después
    [event] = svc.outbox.events("OrderCreated")
    assert event["payload"] == {"order_id": order.id}              # el mensaje de la cola sigue llevando solo el id


# --- worker: aprobada, rechazada, error temporal ------------------------------------------------------------------------
def test_tarjeta_aprobada_cobra_descuenta_y_confirma():
    svc, co = shop(stock=5)
    order = buy(co, APPROVED, qty=2)
    ch = deliver(svc, order.id)
    assert ch.acks == [7] and ch.published == []
    stored = svc.get_order(order.id)
    assert stored.status == OrderStatus.PROCESSED and stored.payment_status == PaymentStatus.APPROVED
    assert stock(svc) == 3
    assert svc.payments.charges == [order.id]
    assert svc.track(order.id)["card"] == "Visa •••• 1111"
    assert svc.get_order(order.id).payment_failures == 0 and svc.track(order.id)["payment_retry"] is False


def test_tarjeta_rechazada_no_descuenta_stock_ni_reintenta():
    svc, co = shop(stock=5)
    order = buy(co, DECLINED)
    ch = deliver(svc, order.id)
    assert ch.acks == [7] and ch.nacks == [] and ch.published == []   # resultado de negocio: ack, sin reintento ni DLQ
    stored = svc.get_order(order.id)
    assert stored.status == OrderStatus.REJECTED and stored.payment_status == PaymentStatus.DECLINED
    assert stored.payment_reason == "Fondos insuficientes"
    assert stock(svc) == 5 and svc.payments.charges == []
    assert svc.track(order.id)["reject_reason"] == "pago_rechazado"
    assert svc.repo.payment_counts()["DECLINED"] == 1


def test_error_temporal_reintenta_y_luego_se_aprueba():
    svc, co = shop(stock=5)
    order = buy(co, FLAKY)
    first = deliver(svc, order.id)                                   # 1.er intento: el proveedor falla
    (queue, headers), = first.published
    assert queue == config.RETRY_QUEUES[0] and headers["retries"] == 1   # reintento a los 2 s (colas que ya existían)
    assert first.acks == [7]
    pending = svc.get_order(order.id)
    assert pending.status != OrderStatus.REJECTED and pending.payment_status == PaymentStatus.PENDING
    assert pending.payment_failures == 1 and stock(svc) == 5
    assert svc.track(order.id)["payment_retry"] is True               # la tienda avisa que se reintenta
    second = deliver(svc, order.id, retries=1)                       # vuelve de la cola de reintento
    assert second.acks == [7] and second.published == []
    stored = svc.get_order(order.id)
    assert stored.status == OrderStatus.PROCESSED and stored.payment_status == PaymentStatus.APPROVED
    assert stored.payment_failures == 1 and stock(svc) == 4 and svc.payments.charges == [order.id]


def test_el_primer_intento_se_cuenta_en_la_bd_y_sobrevive_a_un_worker_nuevo():
    svc, co = shop()
    order = buy(co, FLAKY)
    deliver(svc, order.id)                                           # falla el 1.er intento
    svc.payments = FakePaymentProvider(sleep=lambda _: None)         # el worker murió y otro proceso lo retoma
    deliver(svc, order.id, retries=1)
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED  # no vuelve a fallar: es el 2.º intento


# --- idempotencia: un solo cobro ---------------------------------------------------------------------------------------
def test_mensaje_duplicado_cobra_una_sola_vez():
    svc, co = shop(stock=5)
    order = buy(co, APPROVED)
    deliver(svc, order.id)
    deliver(svc, order.id)                                           # RabbitMQ lo entrega otra vez
    svc.process_order(order.id)
    assert svc.payments.charges == [order.id] and stock(svc) == 4


def test_ya_aprobado_no_se_vuelve_a_cobrar_aunque_el_proveedor_no_lo_recuerde():
    svc, co = shop(stock=5)
    order = buy(co, APPROVED)
    svc.repo.mark_payment_approved(order.id)                         # cobró y el worker murió antes de descontar
    svc.payments = fresh = FakePaymentProvider(sleep=lambda _: None)
    svc.process_order(order.id)
    assert fresh.charges == []                                       # payment_status APPROVED: no se llama al proveedor
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED and stock(svc) == 4


def test_dos_workers_con_el_mismo_mensaje_a_la_vez_cobran_una_vez():
    provider = FakePaymentProvider(sleep=lambda _: None)
    results = []
    threads = [threading.Thread(target=lambda: results.append(provider.charge("pedido-1", TOKEN_APPROVED, 1)))
               for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert provider.charges == ["pedido-1"] and len(results) == 8
    assert {r.status for r in results} == {PaymentStatus.APPROVED}


# --- stock: sin cobrar o con reembolso ------------------------------------------------------------------------------------
def test_sin_stock_se_rechaza_sin_cobrar():
    svc, co = shop(stock=1)
    order = buy(co, APPROVED, qty=2)
    deliver(svc, order.id)
    stored = svc.get_order(order.id)
    assert stored.status == OrderStatus.REJECTED and stored.payment_status == PaymentStatus.PENDING
    assert svc.payments.charges == [] and stock(svc) == 1
    assert svc.track(order.id)["reject_reason"] == "sin_stock"


class StealingProvider(FakePaymentProvider):
    """Mientras "cobra", otro worker se lleva el último producto (la carrera entre revisar y descontar)."""
    def __init__(self, steal):
        super().__init__(sleep=lambda _: None)
        self.steal = steal

    def charge(self, idempotency_key, token, attempt, latency_s=None):
        result = super().charge(idempotency_key, token, attempt, latency_s)
        steal, self.steal = self.steal, None
        if steal:
            steal()
        return result


def test_si_otro_pedido_se_lleva_el_stock_despues_del_cobro_se_reembolsa():
    svc, co = shop(stock=1)
    late, fast = buy(co, APPROVED), buy(co, APPROVED, email="beto@mail.com")
    svc.payments = StealingProvider(steal=lambda: svc.process_order(fast.id))
    ch = deliver(svc, late.id)
    assert ch.acks == [7] and ch.published == []                     # resultado de negocio: ack, sin DLQ
    stored = svc.get_order(late.id)
    assert stored.status == OrderStatus.REJECTED and stored.payment_status == PaymentStatus.REFUNDED
    assert svc.get_order(fast.id).status == OrderStatus.PROCESSED
    assert stock(svc) == 0                                           # nunca negativo
    assert svc.track(late.id)["reject_reason"] == "sin_stock"
    counts = svc.repo.payment_counts()
    assert (counts["APPROVED"], counts["REFUNDED"]) == (1, 1)


# --- pedidos automáticos y comprobante ---------------------------------------------------------------------------------
def test_los_pedidos_automaticos_usan_la_tarjeta_aprobada():
    svc, _ = make(seed=[make_product("A1", 5)])
    order = svc.create_order("ana123@mail.com", [{"sku": "A1", "quantity": 1, "unit_price": 10}])   # simulador
    assert order.payment_token == TOKEN_APPROVED and order.card_last4 is None
    svc.process_order(order.id)
    assert svc.get_order(order.id).payment_status == PaymentStatus.APPROVED


def test_el_comprobante_incluye_la_tarjeta_y_el_estado_del_pago():
    svc, co = shop()
    # cliente registrado desde la tienda (solo ellos reciben comprobante)
    AuthService(svc.users, PasswordHasher(), svc.outbox).register("Ana Pérez", "ana@mail.com", "secreta123")
    order = buy(co, APPROVED)
    svc.process_order(order.id)
    [ev] = svc.outbox.events(EmailKind.COMPRA_CONFIRMADA.value)
    data = ev["payload"]["data"]
    assert data["card"] == "Visa •••• 1111" and data["payment_status"] == "APPROVED"
    _, html, text = email_templates.render(EmailEvent.from_payload(ev["payload"]), "Inti Tech", "http://x")
    assert "Visa •••• 1111" in html and "Pago aprobado" in html
    assert "Visa •••• 1111 · Pago aprobado" in text
