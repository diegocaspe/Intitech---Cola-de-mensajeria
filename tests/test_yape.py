"""Zona Yape real: parser, montos únicos, webhook, procesamiento del pago, expiración y consumidor de la cola pagos.
SQLite + canal falso: no necesitan PostgreSQL ni RabbitMQ."""
import ast
import importlib
import json
import os
import sys
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import psycopg
import pytest

from app import config
from app.messaging import rabbitmq
from app.messaging.rabbitmq import PagoConsumer
from app.models.order import OrderStatus
from app.models.product import Product
from app.models.yape import EstadoNotificacion, EstadoPedidoYape, PAGO_EVENT, SinMontosYape, SinStockYape
from app.repositories.catalog_repository import InMemoryProductRepository, load_products
from app.repositories.interfaces import OrderPublisher
from app.repositories.sqlite_repository import SqliteOrderRepository, SqliteOutbox, SqliteProductRepository
from app.repositories.yape_sqlite import SqliteYapeRepository
from app.services import yape_parser
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.order_service import OrderService
from app.services.yape_service import YapeService

D = Decimal
SECRET = "k7Qz" * 12                     # 48 caracteres, sin palabras de ejemplo
SETTINGS = {"enabled": True, "problem": None, "secret": SECRET, "titular": "Diego C.", "precio_base": D("1.00"),
            "monto_min": D("1.01"), "monto_max": D("1.99"), "minutos": 10}
T0 = datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.t = T0

    def __call__(self):
        return self.t

    def advance(self, **kw):
        self.t += timedelta(**kw)


class FakePublisher(OrderPublisher):
    def __init__(self):
        self.orders, self.pagos, self.emails = [], [], []

    def publish_order_created(self, order_id):
        self.orders.append(order_id)

    def publish_email(self, payload):
        self.emails.append(payload)

    def publish_pago(self, payload):
        self.pagos.append(payload)


def product(sku="YAPE-CASE", stock=500):
    return Product(sku, "Case Zona Yape", "Zona Yape real", D("1.00"), D("19.00"), 4.5, 10, stock, "case.jpg",
                   "desc", (), pago_yape_real=True)


def make(stock=500, **settings):
    clock = Clock()
    repo = SqliteOrderRepository(":memory:")
    yrepo = SqliteYapeRepository(repo)
    products = SqliteProductRepository(repo, [product(stock=stock),   # el stock vive en la tabla products
                                              Product("NORMAL", "Normal", "Audio", D("10"), D("20"), 4, 1, 5, "n.jpg", "d")])
    svc = YapeService(yrepo, repo, products, {**SETTINGS, **settings}, clock=clock)
    pub = FakePublisher()
    orders = OrderService(repo, pub, SqliteOutbox(repo))
    return svc, orders, pub, clock


def pago(monto: str, nombre="JUAN PEREZ"):
    return "Confirmación de Pago", f"Yape! {nombre} te envió un pago por S/ {monto}"


# --- parser -----------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("titulo,texto,monto,pagador", [
    ("Confirmación de Pago", "Yape! JUAN PEREZ te envió un pago por S/ 1.37", "1.37", "JUAN PEREZ"),
    ("Yape", "María López te envió un pago por S/1,37", "1.37", "María López"),
    ("", "Yape! Ana R. te envió un pago por S/. 1.37. El cód. de seguridad es: 123", "1.37", "Ana R"),
    ("Yape", "Carlos te yapeó S/ 1.37", "1.37", "Carlos"),
    ("Yape", "Recibiste S/   1.37 de Rosa Quispe.", "1.37", "Rosa Quispe"),
    ("Yape", "Te yapearon S/ 5", "5.00", None),
    ("Yape", "PEDRO te envió un pago por S/ 1,250.50", "1250.50", "PEDRO"),
    ("Yape", "Yape!​ LUIS te envió un pago por S/  1.99", "1.99", "LUIS"),
])
def test_parser_formatos(titulo, texto, monto, pagador):
    lectura = yape_parser.leer(titulo, texto)
    assert lectura.es_pago, lectura.motivo
    assert lectura.monto == D(monto)
    assert lectura.pagador == pagador


@pytest.mark.parametrize("texto", [
    "Yapeaste S/ 1.37 a JUAN PEREZ",
    "¡Promo! Gana hasta S/ 100 de cashback con Yape",
    "Recuerda pagar tu recibo de luz con Yape",
    "Te damos la bienvenida a Yape",
    "Te envió un pago",                       # parece un pago, pero sin monto: no se puede asociar
])
def test_parser_ignora_lo_que_no_es_un_pago_recibido(texto):
    assert not yape_parser.leer("Yape", texto).es_pago


# --- montos únicos ----------------------------------------------------------------------------------------------
def test_montos_unicos_sin_colisiones_y_sin_montos_libres_alta_demanda():
    svc, _, _, _ = make()
    montos = [svc.checkout(f"c{i}@mail.com", "C", "YAPE-CASE").monto for i in range(99)]   # 1.01 .. 1.99
    assert len(set(montos)) == 99
    assert min(montos) == D("1.01") and max(montos) == D("1.99")
    with pytest.raises(SinMontosYape):
        svc.checkout("otro@mail.com", "O", "YAPE-CASE")


def test_montos_unicos_con_checkouts_simultaneos():
    svc, _, _, _ = make()
    montos, errors = [], []

    def buy(i):
        try:
            montos.append(svc.checkout(f"c{i}@mail.com", "C", "YAPE-CASE").monto)
        except Exception as exc:   # pragma: no cover - debe fallar la prueba
            errors.append(exc)

    threads = [threading.Thread(target=buy, args=(i,)) for i in range(40)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == [] and len(montos) == 40 and len(set(montos)) == 40


def test_la_bd_rechaza_dos_pendientes_con_el_mismo_monto():
    svc, _, _, _ = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    with pytest.raises(Exception):   # índice único parcial: la segunda red de seguridad
        svc.repo.repo._run("INSERT INTO yape_pedidos (id, customer_email, customer_name, sku, producto, monto, estado, "
                           "creado_en, expira_en) VALUES ('x','b','B','YAPE-CASE','P',?, 'pendiente_pago_yape','a','b')",
                           (str(p.monto),))


def test_el_monto_de_un_pedido_vencido_se_puede_reusar():
    svc, _, _, clock = make(monto_min=D("1.01"), monto_max=D("1.01"))
    first = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    clock.advance(minutes=11)
    second = svc.checkout("b@mail.com", "B", "YAPE-CASE")
    assert first.monto == second.monto == D("1.01")
    assert svc.repo.get_pedido(first.id).estado == EstadoPedidoYape.EXPIRADO


def test_solo_productos_de_la_zona_y_zona_desactivada():
    svc, _, _, _ = make()
    with pytest.raises(Exception):
        svc.checkout("a@mail.com", "A", "NORMAL")
    off, _, _, _ = make(enabled=False)
    with pytest.raises(Exception):
        off.checkout("a@mail.com", "A", "YAPE-CASE")


def test_el_carrito_normal_rechaza_los_productos_yape():
    products = InMemoryProductRepository([product()])
    with pytest.raises(CheckoutError):
        CheckoutService(products, None).checkout("a@b.com", [{"sku": "YAPE-CASE", "quantity": 1}])


def test_catalogo_tiene_la_zona_con_precio_base_y_se_oculta_si_esta_desactivada():
    yape = [p for p in load_products(precio_yape=D("1.00")) if p.pago_yape_real]
    assert 6 <= len(yape) <= 10
    assert all(p.category == "Zona Yape real" and p.price == D("1.00") and p.related == () for p in yape)
    assert all(p.sku.startswith("YAPE-") for p in yape)
    assert not any(p.pago_yape_real for p in load_products(incluir_yape=False))


# --- webhook guardado (servicio) ----------------------------------------------------------------------------------
def test_notificacion_duplicada_no_se_guarda_ni_procesa_dos_veces():
    svc, orders, pub, clock = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    first = svc.recibir(*pago(p.monto))
    clock.advance(seconds=20)                                      # MacroDroid reintenta dentro del mismo minuto
    second = svc.recibir(*pago(p.monto))
    assert first[1] is True and second == (first[0], False)
    assert len(orders.outbox.events(PAGO_EVENT)) == 1             # un solo evento en el outbox
    assert svc.procesar(first[0]) == "asociada"
    assert svc.procesar(first[0]) == "asociada"                   # el mensaje duplicado de RabbitMQ no hace nada
    assert len(orders.outbox.events("OrderCreated")) == 1         # un solo pedido normal


def test_otro_minuto_es_otra_notificacion():
    svc, _, _, clock = make()
    a = svc.recibir(*pago("1.50"))
    clock.advance(minutes=1)
    b = svc.recibir(*pago("1.50"))
    assert a[0] != b[0] and b[1] is True


def test_el_relay_lleva_la_notificacion_a_la_cola_pagos():
    svc, orders, pub, _ = make()
    notif_id, _ = svc.recibir(*pago("1.50"))
    assert orders.relay_outbox() == 1
    assert pub.pagos == [{"notificacion_id": notif_id}] and pub.orders == []
    assert orders.outbox.pending_email_count() == 0               # no se confunde con un correo


# --- procesamiento ------------------------------------------------------------------------------------------------
def test_pago_con_monto_correcto_marca_el_pedido_pagado_y_sigue_el_flujo_normal():
    svc, orders, pub, clock = make()
    p = svc.checkout("a@mail.com", "Ana", "YAPE-CASE")
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "esperando_yapeo"
    notif_id, _ = svc.recibir(*pago(p.monto))
    clock.advance(seconds=3)
    st = svc.estado_cliente(p.id, "a@mail.com")
    assert st["fase"] == "en_cola" and st["posicion"] == 1        # pago recibido, el worker aún no lo procesa
    assert svc.estado_cliente(p.id, "a@mail.com", broker_up=False)["fase"] == "pago_registrado"

    assert svc.procesar(notif_id) == "asociada"
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.PAGADO
    n = svc.repo.get_notificacion(notif_id)
    assert (n.estado, n.pedido_id, n.pagador) == (EstadoNotificacion.ASOCIADA, p.id, "JUAN PEREZ")
    order = orders.get_order(p.id)                                # mismo id, 1 unidad al monto yapeado
    assert order.status == OrderStatus.PENDING and order.total == p.monto and order.items[0].quantity == 1
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "en_cola"

    orders.relay_outbox()                                         # outbox → cola de pedidos → worker
    assert p.id in pub.orders
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "procesando"
    orders.process_order(p.id)
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "confirmado"


def test_monto_distinto_queda_sin_coincidencia():
    svc, orders, _, _ = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    otro = D("1.01") if p.monto != D("1.01") else D("1.02")
    notif_id, _ = svc.recibir(*pago(otro))
    assert svc.procesar(notif_id) == "sin_coincidencia"
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.PENDIENTE
    assert orders.get_order(p.id) is None
    assert svc.repo.get_notificacion(notif_id).texto.endswith(f"S/ {otro}")   # el texto crudo nunca se pierde


def test_notificacion_que_no_es_pago_queda_ignorada_con_su_texto():
    svc, _, _, _ = make()
    notif_id, _ = svc.recibir("Yape", "Yapeaste S/ 1.37 a JUAN")
    assert svc.procesar(notif_id) == "ignorada"
    n = svc.repo.get_notificacion(notif_id)
    assert n.estado == EstadoNotificacion.IGNORADA and n.texto == "Yapeaste S/ 1.37 a JUAN"


def test_segundo_pago_al_mismo_pedido_queda_para_revision():
    svc, _, _, clock = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    a, _ = svc.recibir(*pago(p.monto))
    clock.advance(minutes=1)
    b, _ = svc.recibir(*pago(p.monto))
    assert svc.procesar(a) == "asociada"
    assert svc.procesar(b) == "sin_coincidencia"


def test_asignar_a_mano_y_rechazar():
    svc, orders, _, _ = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    a, _ = svc.recibir(*pago("9.99"))
    svc.procesar(a)
    svc.asignar(a, p.id)
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.PAGADO and orders.get_order(p.id) is not None
    b, _ = svc.recibir("Yape", "Yape! X te envió un pago por S/ 3.00 (repetido)")
    svc.procesar(b)
    svc.rechazar(b)
    assert svc.repo.get_notificacion(b).estado == EstadoNotificacion.RECHAZADA
    with pytest.raises(Exception):
        svc.rechazar(a)                                           # una asociada ya no se rechaza


def test_simular_marca_la_notificacion_y_usa_el_mismo_camino():
    svc, orders, _, _ = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    notif_id, monto = svc.simular(pedido_id=p.id)
    assert monto == p.monto and svc.repo.get_notificacion(notif_id).simulada
    assert len(orders.outbox.events(PAGO_EVENT)) == 1
    assert svc.procesar(notif_id) == "asociada"
    filas = svc.panel()["notificaciones"]
    assert filas[0]["simulada"] and filas[0]["etapa"] == "outbox"   # el evento aún no se publicó (sin relay)


# --- expiración ---------------------------------------------------------------------------------------------------
def test_expiracion_libera_el_stock():
    svc, _, _, clock = make(stock=1)
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    assert svc.stock_disponible()["YAPE-CASE"] == 0
    with pytest.raises(SinStockYape):
        svc.checkout("b@mail.com", "B", "YAPE-CASE")
    clock.advance(minutes=10, seconds=1)
    assert svc.expirar_vencidos() == 1
    assert svc.expirar_vencidos() == 0                            # idempotente
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.EXPIRADO
    assert svc.stock_disponible()["YAPE-CASE"] == 1
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "expirado"
    svc.checkout("b@mail.com", "B", "YAPE-CASE")                  # el stock volvió


def test_pago_despues_de_expirar_queda_sin_coincidencia():
    svc, orders, _, clock = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    clock.advance(minutes=11)
    svc.expirar_vencidos()
    notif_id, _ = svc.recibir(*pago(p.monto))
    assert svc.procesar(notif_id) == "sin_coincidencia"
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.EXPIRADO and orders.get_order(p.id) is None


def test_pago_que_llego_a_tiempo_se_asocia_aunque_el_worker_lo_procese_tarde():
    svc, _, _, clock = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    clock.advance(minutes=9)
    notif_id, _ = svc.recibir(*pago(p.monto))                     # llegó a tiempo...
    clock.advance(minutes=20)                                     # ...pero el worker estuvo muerto 20 minutos
    svc.expirar_vencidos()
    assert svc.estado_cliente(p.id, "a@mail.com")["fase"] == "en_cola"   # el cliente nunca ve "expirado"
    assert svc.procesar(notif_id) == "asociada"
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.PAGADO


# --- consumidor de la cola "pagos" (canal falso) -------------------------------------------------------------------
class FakeChannel:
    def __init__(self):
        self.acks, self.nacks, self.published = [], [], []

    def basic_ack(self, tag):
        self.acks.append(tag)

    def basic_nack(self, tag, requeue):
        self.nacks.append((tag, requeue))

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        self.published.append((routing_key, properties.headers))


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(rabbitmq.time, "sleep", lambda _: None)


def deliver(service, body, retries=0):
    ch = FakeChannel()
    PagoConsumer(service)._on_message(ch, SimpleNamespace(delivery_tag=9), SimpleNamespace(headers={"retries": retries}),
                                      body)
    return ch


def test_consumidor_procesa_y_confirma(no_sleep):
    svc, _, _, _ = make()
    p = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    notif_id, _ = svc.recibir(*pago(p.monto))
    ch = deliver(svc, json.dumps({"notificacion_id": notif_id}).encode())
    assert ch.acks == [9] and ch.nacks == []
    assert svc.repo.get_pedido(p.id).estado == EstadoPedidoYape.PAGADO


def test_consumidor_bd_caida_devuelve_a_la_cola_nunca_a_la_dlq(no_sleep):
    svc = SimpleNamespace(procesar=mock.Mock(side_effect=psycopg.OperationalError("BD caída")))
    ch = deliver(svc, b'{"notificacion_id": 1}', retries=3)
    assert ch.nacks == [(9, True)] and ch.acks == [] and ch.published == []


def test_consumidor_error_de_negocio_reintenta_y_luego_dlq(no_sleep):
    svc, _, _, _ = make()
    ch = deliver(svc, b'{"notificacion_id": 999}')                # no existe → ProcessingError
    assert ch.published[0][0] == config.PAGOS_RETRY_QUEUES[0] and ch.acks == [9]
    ch = deliver(svc, b'{"notificacion_id": 999}', retries=config.MAX_RETRIES)
    assert ch.nacks == [(9, False)]
    assert deliver(svc, b"basura").nacks == [(9, False)]          # ilegible → DLQ


# --- configuración ------------------------------------------------------------------------------------------------
def test_secreto_ausente_corto_o_de_ejemplo_desactiva_la_zona_sin_mostrar_el_valor():
    assert config.yape_settings({})["enabled"] is False
    short = config.yape_settings({"YAPE_WEBHOOK_SECRET": "x9" * 10})
    assert short["enabled"] is False and "x9x9" not in short["problem"] and "32" in short["problem"]
    assert config.yape_settings({"YAPE_WEBHOOK_SECRET": "cambia_este_secreto_" * 3})["enabled"] is False
    ok = config.yape_settings({"YAPE_WEBHOOK_SECRET": SECRET})
    assert ok["enabled"] and ok["monto_min"] == D("1.01") and ok["monto_max"] == D("1.99") and ok["minutos"] == 10
    assert ok["precio_base"] == D("1.00")
    assert config.yape_settings({"YAPE_WEBHOOK_SECRET": SECRET, "YAPE_MONTO_MIN": "2", "YAPE_MONTO_MAX": "1"})["enabled"] is False


# --- webhook por HTTP (FastAPI TestClient, sin PostgreSQL ni Docker) -------------------------------------------------
@pytest.fixture(scope="module")
def api_module():
    """Importa app.routes.api con el pool de PostgreSQL y Docker reemplazados por objetos falsos."""
    env = {"RABBITMQ_USER": "u", "RABBITMQ_PASS": "p", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p",
           "POSTGRES_DB": "d", "SECRET_KEY": "f3a9" * 16, "ADMIN_EMAIL": "jefe@tienda.pe", "ADMIN_PASSWORD": "Tq7#vLm2xW9z"}
    with mock.patch.dict(os.environ, {k: os.environ.get(k) or v for k, v in env.items()}), \
            mock.patch("app.database.connection.create_pool", return_value=mock.MagicMock()), \
            mock.patch("app.utils.docker_control.DockerControl", return_value=mock.MagicMock(available=False)):
        sys.modules.pop("app.routes.api", None)
        yield importlib.import_module("app.routes.api")
    sys.modules.pop("app.routes.api", None)


@pytest.fixture
def client(api_module, monkeypatch):
    from fastapi.testclient import TestClient
    from app.utils.rate_limit import LoginRateLimiter
    svc, orders, _, _ = make()
    monkeypatch.setattr(api_module, "yape", svc)
    monkeypatch.setattr(api_module.config, "YAPE", {**SETTINGS})
    monkeypatch.setattr(api_module, "webhook_limiter", LoginRateLimiter(60, 60))
    return TestClient(api_module.app), svc, orders


URL = "/api/pagos/yape/notificacion"


def test_webhook_sin_secreto_o_con_secreto_incorrecto_401(client):
    c, svc, orders = client
    body = {"titulo": "Confirmación de Pago", "texto": "Yape! JUAN te envió un pago por S/ 1.37"}
    assert c.post(URL, json=body).status_code == 401
    assert c.post(URL, json=body, headers={"X-Yape-Secret": "otro" * 12}).status_code == 401
    assert c.post(URL, json=body, headers={"X-Yape-Secret": SECRET[:-1]}).status_code == 401
    assert orders.outbox.events(PAGO_EVENT) == []                # nada se guardó


def test_webhook_guarda_responde_200_y_no_duplica(client):
    c, svc, orders = client
    body = {"titulo": "Confirmación de Pago", "texto": "Yape! JUAN te envió un pago por S/ 1.37", "app": "Yape",
            "extra": {"x": 1}}                                    # campos extra: se ignoran
    r1 = c.post(URL, json=body, headers={"X-Yape-Secret": SECRET})
    r2 = c.post(URL, json=body, headers={"X-Yape-Secret": SECRET})
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["duplicada"] is False and r2.json() == {**r1.json(), "duplicada": True}
    assert len(orders.outbox.events(PAGO_EVENT)) == 1
    n = svc.repo.get_notificacion(r1.json()["id"])
    assert n.texto == body["texto"] and n.estado == EstadoNotificacion.RECIBIDA   # el request no procesa el pago


def test_webhook_json_roto_guarda_el_cuerpo_como_texto(client):
    c, svc, _ = client
    raw = '{"titulo": "Yape", "texto": "Yape! "JUAN" te envió un pago por S/ 1.37"}'.encode()
    r = c.post(URL, content=raw, headers={"X-Yape-Secret": SECRET, "Content-Type": "application/json"})
    assert r.status_code == 200
    assert "S/ 1.37" in svc.repo.get_notificacion(r.json()["id"]).texto


def test_webhook_acepta_cuerpo_en_latin1(client):
    c, svc, _ = client
    raw = '{"titulo": "Confirmación de Pago", "texto": "Yape! JUAN te envió un pago por S/ 1.37"}'.encode("cp1252")
    r = c.post(URL, content=raw, headers={"X-Yape-Secret": SECRET, "Content-Type": "application/json"})
    assert r.status_code == 200
    n = svc.repo.get_notificacion(r.json()["id"])
    assert n.texto == "Yape! JUAN te envió un pago por S/ 1.37" and svc.procesar(n.id) == "sin_coincidencia"


def test_webhook_bd_caida_503(client, monkeypatch):
    c, svc, _ = client
    monkeypatch.setattr(svc, "recibir", mock.Mock(side_effect=psycopg.OperationalError("BD caída")))
    r = c.post(URL, json={"texto": "Yape! A te envió un pago por S/ 1.10"}, headers={"X-Yape-Secret": SECRET})
    assert r.status_code == 503


def test_webhook_limita_60_por_minuto(client):
    c, _, _ = client
    codes = [c.post(URL, json={"texto": f"Yape! A te envió un pago por S/ 1.{i:02d}"},
                    headers={"X-Yape-Secret": SECRET}).status_code for i in range(61)]
    assert codes[:60] == [200] * 60 and codes[60] == 429


def test_webhook_con_la_zona_desactivada_503(client, monkeypatch):
    c, svc, _ = client
    monkeypatch.setattr(svc, "settings", {**SETTINGS, "enabled": False})
    assert c.post(URL, json={"texto": "x"}, headers={"X-Yape-Secret": SECRET}).status_code == 503


def test_las_rutas_admin_de_yape_exigen_require_admin():
    tree = ast.parse((Path(config.__file__).parent / "routes" / "api.py").read_text(encoding="utf-8"))
    checked = 0
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        for dec in fn.decorator_list:
            if isinstance(dec, ast.Call) and dec.args and str(getattr(dec.args[0], "value", "")).startswith("/api/yape/admin"):
                deps = [d for d in fn.args.defaults if isinstance(d, ast.Call) and getattr(d.func, "id", "") == "Depends"]
                assert any(getattr(d.args[0], "id", "") == "require_admin" for d in deps), fn.name
                checked += 1
    assert checked == 4
