import random
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field

from app import config
from app.services.auth_service import AuthError, AuthService
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.events import EventLog
from app.services.order_service import POISON_SKU, OrderService
from app.services.scenarios import SCENARIOS, TEST_DOMAIN, DbUnavailable, ScenarioError, ScenarioRunner
from app.services.simulator import TrafficSimulator
from app.models.order import OrderStatus
from app.utils.control import ControlStore
from app.utils.docker_control import DockerControl
from app.messaging.management import QueueMonitor
from app.messaging.rabbitmq import (DeadLetterInspector, RabbitOrderPublisher, purge_queues,
                                    remove_legacy_pagos_in_background)
from app.database.connection import create_pool, init_schema
from app.repositories.postgres_repository import (PostgresOrderRepository, PostgresOutbox, PostgresProductRepository,
                                                  PostgresSentEmails, PostgresUserRepository)
from app.models.email import TokenResult
from app.utils.security import PasswordHasher, TokenSigner

# --- composición de dependencias -------------------------------------------
config.require_env("api")   # antes de abrir conexiones: si falta un secreto, error claro en vez de un fallo raro
events = EventLog()
pool = create_pool(config.DATABASE_URL, max_size=15)
control = ControlStore()
# el modo de persistencia (interruptor del panel) decide si cada pedido viaja por la cola durable o la volátil
publisher = RabbitOrderPublisher(persistent_mode=lambda: control.get()["persistent"])
service = OrderService(PostgresOrderRepository(pool), publisher, PostgresOutbox(pool))
# con el outbox, AuthService pide sus correos (bienvenida, inicio de sesión, recuperación) dejando eventos en él
auth = AuthService(PostgresUserRepository(pool), PasswordHasher(), PostgresOutbox(pool),
                   verify_hours=config.VERIFY_TOKEN_HOURS, reset_minutes=config.RESET_TOKEN_MINUTES,
                   reset_throttle_s=config.RESET_THROTTLE_S)
sent_emails = PostgresSentEmails(pool)
tokens = TokenSigner(config.SECRET_KEY)


class DbWatcher:
    """Lee las cifras de la BD en segundo plano cada READ_EVERY segundos. Las páginas responden al instante con el último
    resultado, aunque PostgreSQL esté caído (en ese caso se conservan los últimos valores conocidos).
    Con la BD caída no usa el pool (sus timeouts tardan segundos): la sondea directo cada PROBE_EVERY segundos
    para notar al instante que volvió."""

    PROBE_EVERY = 0.25
    READ_EVERY = 0.25   # el panel muestra un pedido casi al mismo tiempo que el celular de quien compra
    RATE_WINDOW_S = 3
    BACKLOG_NOTICE = 10

    def __init__(self):
        self.snapshot = {"orders": {**{s.value: 0 for s in OrderStatus}, "total": 0}, "revenue": 0.0,
                         "recent": [], "outbox_recent": [], "last_min": 0, "outbox_pending": 0, "stale": 0, "stale_ids": [],
                         "queue_window": {"count": 0, "head": [], "tail": [], "first_event": 1, "numbers": {}},
                         "relay": {"published": 0, "rate": 0.0}, "db_up": True,
                         "emails": {"sent": 0, "outbox_pending": 0},
                         "payments": {"approved": 0, "declined": 0, "refunded": 0, "pending": 0}}
        self._peak = 0   # mayor acumulación del outbox desde la última vez que quedó vacío

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            if self.snapshot["db_up"]:
                t = time.monotonic()
                self._set_up(self._read())
                # con mucho tráfico la lectura tarda más: nunca ocupa la BD más de la mitad del tiempo
                time.sleep(max(self.READ_EVERY, 2 * (time.monotonic() - t)))
            else:
                if self._probe():
                    # el pool aún guarda las conexiones muertas (y espera cada vez más entre reintentos):
                    # check() las prueba y las reemplaza ya, para no esperar sus timeouts de 3 s
                    pool.check()
                    self._set_up(self._read())   # si aún falla, se reintenta en la próxima sonda
                time.sleep(self.PROBE_EVERY)

    def mark_down(self) -> None:
        """El sabotaje avisa que acaba de matar la BD: el panel lo muestra ya, sin esperar a que una consulta falle."""
        self._set_up(False)

    def _set_up(self, up: bool) -> None:
        if up and not self.snapshot["db_up"]:
            events.add("ok", "PostgreSQL responde de nuevo: los datos siguen ahí y los workers retoman la cola", "database")
        elif not up and self.snapshot["db_up"]:
            events.add("bad", "PostgreSQL no responde: la tienda no acepta pedidos nuevos y la cola espera", "database")
        self.snapshot = {**self.snapshot, "db_up": up}

    @staticmethod
    def _probe() -> bool:
        try:
            with psycopg.connect(config.DATABASE_URL, connect_timeout=1, autocommit=True) as conn:
                conn.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False

    def _note_backlog(self, pending: int) -> None:
        self._peak = max(self._peak, pending)
        if pending == 0 and self._peak >= self.BACKLOG_NOTICE:
            events.add("ok", f"El relay publicó los {self._peak} eventos acumulados en el outbox: ningún pedido se perdió", "database")
        if pending == 0:
            self._peak = 0

    def _read(self) -> bool:
        try:
            counts = service.repo.count_by_status()
            stale_ids = service.repo.stale_published(config.LOST_AFTER_S)   # una sola consulta: el conteo y los ids
            published, recent = service.outbox.published_stats(self.RATE_WINDOW_S)
            self.snapshot = {
                **self.snapshot,
                "orders": {**counts, "total": sum(counts.values())},
                "revenue": service.repo.revenue(OrderStatus.PROCESSED),
                "recent": [{"id": o.id, "email": o.customer_email, "status": o.status.value, "total": o.total,
                            "items": [f"{i.quantity}× {i.sku}" for i in o.items], "created_at": o.created_at}
                           for o in service.repo.recent(10)],
                "outbox_recent": service.outbox.recent(10),   # para el detalle de PostgreSQL y los números del almacén en el panel
                "last_min": service.repo.count_since(60),
                "outbox_pending": service.outbox.pending_count(),
                "queue_window": service.outbox.queue_window(24),   # ids reales de los extremos de la fila, para el almacén
                "stale": len(stale_ids),
                "stale_ids": stale_ids,   # para marcar en la pizarra de PostgreSQL cuáles perdió RabbitMQ
                "relay": {"published": published, "rate": round(recent / self.RATE_WINDOW_S, 1)},
                # correos: enviados (tabla de idempotencia del mailer) y eventos aún sin publicar en el outbox
                "emails": {"sent": sent_emails.count(), "outbox_pending": service.outbox.pending_email_count()},
                # pagos con tarjeta simulados: lo que decidió el worker al cobrar
                "payments": {k.lower(): v for k, v in service.repo.payment_counts().items()},
            }
            self._note_backlog(self.snapshot["outbox_pending"])
            return True
        except (psycopg.Error, PoolTimeout):
            return False


db_watch = DbWatcher()
# Catálogo y stock desde la tabla products (catalogo.json solo es la semilla)
products = PostgresProductRepository(pool)
checkout_service = CheckoutService(products, service)
simulator = TrafficSimulator(service, events, products)
docker = DockerControl()
monitor = QueueMonitor()
dlq = DeadLetterInspector()
dlq_emails = DeadLetterInspector(email=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_schema(pool)
    simulator.set_poison(bool(control.get()["poison"]))   # tras reiniciar la API, el simulador sigue al interruptor
    auth.ensure_admin(config.ADMIN_EMAIL, config.ADMIN_PASSWORD)
    db_watch.start()
    events.add("info", "Sistema listo. Todo lo que ves está respaldado por RabbitMQ + PostgreSQL.")
    # la antigua cola "pagos" (Zona Yape, eliminada) se borra del broker si quedó de una versión anterior
    remove_legacy_pagos_in_background()
    yield


TAGS = [
    {"name": "Usuarios", "description": "Registro, inicio y cierre de sesión, verificación del correo y recuperación de contraseña."},
    {"name": "Tienda", "description": "Lo que usa la tienda del cliente: catálogo, compra y seguimiento del pedido."},
    {"name": "Pedidos (admin)", "description": "Consulta y gestión directa de pedidos."},
    {"name": "Panel (admin)", "description": "Métricas, pedidos fallidos (DLQ), reconciliación y reinicio de la demo."},
    {"name": "Simulador (admin)", "description": "Genera tráfico de prueba: goteo, ráfagas o un Black Friday completo."},
    {"name": "Sabotajes (admin)", "description": "Apaga o recupera contenedores reales para demostrar la tolerancia a fallos."},
]
app = FastAPI(title="Tienda Inti Tech - Black Friday", version="3.0", lifespan=lifespan, openapi_tags=TAGS,
              description="API de la tienda: el cliente compra, el pedido se guarda en PostgreSQL y se encola en RabbitMQ, "
                          "y un worker lo procesa. Las rutas marcadas (admin) requieren iniciar sesión como administrador.")
STATIC = Path(__file__).parent.parent / "static"   # app/static
app.mount("/static", StaticFiles(directory=STATIC), name="static")


DB_DOWN_DETAIL = "Servicio no disponible, intenta en unos segundos."


@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(PoolTimeout)
def db_unavailable(_: Request, __: Exception):
    # PostgreSQL caído: el pedido NO se acepta (no hay dónde guardarlo con su evento). Mejor un 503 claro que
    # aceptar algo que se podría perder
    return JSONResponse({"detail": DB_DOWN_DETAIL}, status_code=503)


@app.exception_handler(RequestValidationError)
def invalid_request(_: Request, exc: RequestValidationError):
    # Como el 422 de FastAPI, pero SIN el campo "input": nunca se devuelve lo que envió el cliente (p. ej. el número de
    # una tarjeta o una contraseña mal formados)
    errors = [{k: v for k, v in e.items() if k in ("type", "loc", "msg")} for e in exc.errors()]
    return JSONResponse({"detail": errors}, status_code=422)


# --- sesión -----------------------------------------------------------------
SESSION_COOKIE = "sesion"


def current_user(request: Request) -> dict | None:
    return tokens.verify(request.cookies.get(SESSION_COOKIE))


def require_user(request: Request) -> dict:
    user = current_user(request)
    if not user:
        raise HTTPException(401, "Inicia sesión para continuar")
    return user


def require_admin(request: Request) -> dict:
    user = require_user(request)
    if user["role"] != "admin":
        raise HTTPException(403, "Solo el administrador puede hacer esto")
    return user


def _out(order):
    d = asdict(order)
    d["total"] = order.total
    return d


# --- frontend ---------------------------------------------------------------
@app.get("/", include_in_schema=False)
def store_page():
    return FileResponse(STATIC / "tienda.html")


@app.get("/login", include_in_schema=False)
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/panel", include_in_schema=False)
def panel_page(request: Request):
    user = current_user(request)
    if not user or user["role"] != "admin":
        return RedirectResponse("/login?next=/panel&admin=1")
    return FileResponse(STATIC / "panel.html")


# --- autenticación ----------------------------------------------------------
class RegisterIn(BaseModel):
    name: str = Field(max_length=60)
    email: str = Field(max_length=120)
    password: str = Field(max_length=128)


class LoginIn(BaseModel):
    email: str = Field(max_length=120)
    password: str = Field(max_length=128)


class ForgotIn(BaseModel):
    email: str = Field(max_length=120)


class ResetIn(BaseModel):
    token: str = Field(max_length=200)
    password: str = Field(max_length=128)


def _start_session(user) -> JSONResponse:
    info = {"id": user.id, "name": user.name, "email": user.email, "role": user.role.value}
    resp = JSONResponse(info)
    resp.set_cookie(SESSION_COOKIE, tokens.sign(info, config.SESSION_DAYS * 86400),
                    max_age=config.SESSION_DAYS * 86400, httponly=True, samesite="lax", secure=config.COOKIE_SECURE)
    return resp


@app.post("/api/auth/register", status_code=201, tags=["Usuarios"], summary="Registrar un cliente", description="Crea la cuenta y deja la sesión iniciada (cookie).")
def register(body: RegisterIn):
    try:
        user = auth.register(body.name, body.email, body.password)
    except AuthError as exc:
        raise HTTPException(400, str(exc))
    events.add("info", f"Nuevo cliente registrado: {user.name}", "user")
    resp = _start_session(user)
    resp.status_code = 201
    return resp


@app.post("/api/auth/login", tags=["Usuarios"], summary="Iniciar sesión",
          description="Valida correo y contraseña y entrega la cookie de sesión. Si no coinciden, responde 401.")
def login(body: LoginIn):
    try:
        user = auth.authenticate(body.email, body.password)
    except AuthError as exc:
        raise HTTPException(401, str(exc))
    return _start_session(user)


@app.post("/api/auth/logout", tags=["Usuarios"], summary="Cerrar sesión")
def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, httponly=True, samesite="lax", secure=config.COOKIE_SECURE)
    return resp


@app.get("/api/auth/me", tags=["Usuarios"], summary="Usuario actual", description="Datos del usuario con sesión iniciada, o null si no hay sesión. email_status: pending (falta verificar el correo), verified, legacy (cuenta de prueba anterior a los correos) o null si la base de datos no responde.")
def me(request: Request):
    user = current_user(request)
    if not user:
        raise HTTPException(401, "Sin sesión")
    info = {k: user[k] for k in ("id", "name", "email", "role")}
    try:   # la sesión vale aunque la BD esté caída: solo se pierde el aviso de verificación
        u = auth.users.get_by_email(user["email"])
        info["email_status"] = u.email_status.value if u else None
    except (psycopg.Error, PoolTimeout):
        info["email_status"] = None
    return info


@app.get("/api/auth/verify", tags=["Usuarios"], summary="Verificar el correo", description="Destino del enlace del correo de bienvenida. El token es de un solo uso y vence a las 24 h. Redirige a la tienda con ?verificado=ok, expirado, usado o invalido.")
def verify_email(token: str = ""):
    result = auth.verify_email(token[:200])
    if result == TokenResult.OK:
        events.add("ok", "Un cliente verificó su correo", "user")
    return RedirectResponse(f"/?verificado={result.value}", status_code=303)


@app.post("/api/auth/verify/resend", tags=["Usuarios"], summary="Reenviar el correo de verificación", description="Genera un enlace nuevo para la cuenta con sesión iniciada (como mucho uno por minuto).")
def resend_verification(user: dict = Depends(require_user)):
    return {"sent": auth.resend_verification(user["email"])}


@app.post("/api/auth/forgot", tags=["Usuarios"], summary="Olvidé mi contraseña", description="Si el correo pertenece a un cliente, le envía un enlace para crear una contraseña nueva (vence en 60 min, un solo uso). Responde lo mismo exista o no la cuenta.")
def forgot_password(body: ForgotIn):
    auth.request_password_reset(body.email)
    return {"ok": True, "message": "Si el correo está registrado, te enviamos un enlace para crear una contraseña nueva."}


@app.post("/api/auth/reset", tags=["Usuarios"], summary="Restablecer la contraseña", description="Cambia la contraseña con el token del enlace recibido por correo. El token se gasta al usarlo.")
def reset_password(body: ResetIn):
    try:
        result = auth.reset_password(body.token, body.password)
    except AuthError as exc:
        raise HTTPException(400, str(exc))
    if result != TokenResult.OK:
        raise HTTPException(400, {TokenResult.EXPIRED: "El enlace venció. Pide uno nuevo.",
                                  TokenResult.USED: "Este enlace ya se usó. Pide uno nuevo si lo necesitas."}
                            .get(result, "El enlace no es válido."))
    return {"ok": True}


# --- API de la tienda -------------------------------------------------------
@app.get("/api/products", tags=["Tienda"], summary="Catálogo de productos", description="Los productos con precio, descuento, stock e imagen. El stock sale de la tabla products (el worker lo descuenta al confirmar cada pedido). Si la base de datos cae, se sirve el último stock conocido.")
def list_products():
    return [{**asdict(p), "discount_pct": p.discount_pct, "image": f"/static/img/{p.image}"} for p in products.all()]


class CartLine(BaseModel):
    sku: str
    quantity: int


class CardIn(BaseModel):
    """Tarjeta de PRUEBA (pago simulado). Campos de texto sin restricciones a propósito: los valida CheckoutService y
    responde 400 con un mensaje propio, que nunca repite lo que se escribió. No se guarda ni se registra el número
    completo ni el CVV."""
    holder: str = ""
    number: str = ""
    exp: str = ""
    cvv: str = ""


class CheckoutIn(BaseModel):
    items: list[CartLine] = Field(min_length=1, max_length=30)
    card: CardIn | None = None


@app.post("/api/checkout", status_code=201, tags=["Tienda"], summary="Comprar (checkout)",
          description="Convierte el carrito en un pedido y responde al instante (201): el cobro lo hace el worker después, de "
                      "forma asíncrona. Solo se envían SKU y cantidad (los precios salen del catálogo del servidor) y la "
                      "tarjeta. SOLO se aceptan las tarjetas de prueba 4111 1111 1111 1111 (aprobada), 4000 0000 0000 0002 "
                      "(rechazada) y 4000 0000 0000 0119 (error temporal: se aprueba en el reintento); cualquier otro número "
                      "responde 400 «Usa una tarjeta de prueba». La tarjeta se cambia por un token de prueba: el número "
                      "completo y el CVV no se guardan ni viajan en la cola. Requiere sesión.",
          responses={400: {"description": "Carrito o tarjeta inválidos («Usa una tarjeta de prueba»)"}})
def checkout(body: CheckoutIn, user: dict = Depends(require_user)):
    try:
        order = checkout_service.checkout(user["email"], [l.model_dump() for l in body.items],
                                          body.card.model_dump() if body.card else None)
    except CheckoutError as exc:
        raise HTTPException(400, str(exc))
    events.add("ok", f"Compra real desde la tienda: {user['name']} · S/ {order.total:,.2f}", "cart")
    return service.track(order.id)


@app.get("/api/orders/{order_id}/track", tags=["Tienda"], summary="Seguir un pedido", description="Estado del pedido y su posición en la cola. El cliente lo consulta cada segundo. payment_status: PENDING, APPROVED, DECLINED o REFUNDED; card: «Visa •••• 1111»; reject_reason (si está REJECTED): pago_rechazado (no se descontó stock) o sin_stock (no se cobró, o se reembolsó).")
def track(order_id: str, user: dict = Depends(require_user)):
    order = service.get_order(order_id)
    if not order or (order.customer_email != user["email"] and user["role"] != "admin"):
        raise HTTPException(404, "Pedido no encontrado")
    return service.track(order_id)


@app.get("/api/my/orders", tags=["Tienda"], summary="Mis pedidos", description="Últimos pedidos del cliente con sesión iniciada.")
def my_orders(user: dict = Depends(require_user)):
    return service.orders_of(user["email"])


@app.get("/api/store/live", tags=["Tienda"], summary="Estado en vivo de la tienda", description="Pedidos del último minuto, mensajes en cola y si RabbitMQ y PostgreSQL están arriba.")
def store_live():
    q = monitor.stats()
    snap = db_watch.snapshot
    return {"orders_last_min": snap["last_min"], "in_queue": q and q["ready"] + q["unacked"],
            "broker_up": q is not None, "db_up": snap["db_up"]}


def _store_url(request: Request) -> str:
    return config.PUBLIC_URL or str(request.base_url).rstrip("/")


@app.get("/api/store-url", tags=["Tienda"], summary="Dirección pública de la tienda", description="URL que el público abre desde el celular (la del código QR).")
def store_url(request: Request):
    return {"url": _store_url(request)}


@app.get("/api/qr.svg", include_in_schema=False)
def store_qr(request: Request):
    import qrcode
    import qrcode.image.svg
    img = qrcode.make(_store_url(request), image_factory=qrcode.image.svg.SvgPathFillImage, box_size=12, border=2)
    return Response(img.to_string(), media_type="image/svg+xml")


# --- API de pedidos ---------------------------------------------------------
class ItemIn(BaseModel):
    sku: str
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0, max_digits=10, decimal_places=2)


class OrderIn(BaseModel):
    customer_email: str
    items: list[ItemIn] = Field(min_length=1)


@app.post("/orders", status_code=201, tags=["Pedidos (admin)"], summary="Crear un pedido directo", description="Crea un pedido indicando los productos y precios a mano, sin pasar por el carrito.")
def create_order(body: OrderIn, _: dict = Depends(require_admin)):
    return _out(service.create_order(body.customer_email, [i.model_dump() for i in body.items]))


@app.get("/orders", tags=["Pedidos (admin)"], summary="Listar pedidos", description="Filtra opcionalmente por estado: PENDING, PUBLISHED, PROCESSED, FAILED o REJECTED (rechazado por negocio: sin stock o pago rechazado).")
def list_orders(status: OrderStatus | None = None, limit: int = 100, _: dict = Depends(require_admin)):
    return [_out(o) for o in service.repo.list(status, limit)]


@app.get("/orders/{order_id}", tags=["Pedidos (admin)"], summary="Ver un pedido")
def get_order(order_id: str, _: dict = Depends(require_admin)):
    order = service.get_order(order_id)
    if not order:
        raise HTTPException(404, "Pedido no encontrado")
    return _out(order)


@app.post("/orders/republish-pending", tags=["Pedidos (admin)"], summary="Reenviar pendientes del outbox", description="Herramienta de administración: publica ya los pedidos pendientes sin esperar al relay.")
def republish_pending(_: dict = Depends(require_admin)):
    return {"republished": service.relay_outbox(min_age_s=0)}


# --- API del panel ----------------------------------------------------------
def _services(q) -> dict | None:
    """Estado de cada contenedor. RabbitMQ cuenta como caído si su contenedor corre pero el broker no responde:
    tras «Tirar RabbitMQ» (stop_app) o mientras arranca."""
    st = docker.status()
    if st and st.get("rabbitmq") == "running" and q is None:
        st = {**st, "rabbitmq": "stopped"}   # copia: docker.status() es el estado compartido del hilo de fondo
    return st


@app.get("/api/metrics", tags=["Panel (admin)"], summary="Métricas del panel", description="Todo lo que el panel muestra: pedidos por estado, pagos (approved, declined, refunded, pending), colas, outbox, DLQ, servicios y simulador. lost y lost_ids: pedidos publicados hace rato que RabbitMQ perdió (solo se calculan con la cola vacía); siguen guardados en PostgreSQL.")
def metrics(_: dict = Depends(require_admin)):
    q = monitor.stats()
    ctl = control.get()
    snap = db_watch.snapshot
    in_flight = q["ready"] + q["unacked"] + q["retrying"] if q else None
    return {
        **snap,
        # perdidos = publicados hace rato, sin procesar, y que no están en ninguna cola (el broker los olvidó)
        "lost": snap["stale"] if in_flight == 0 else 0,
        "lost_ids": snap["stale_ids"] if in_flight == 0 else [],   # misma regla: solo con la cola vacía
        "queue": q,
        "broker_up": q is not None,
        "services": _services(q),
        "sim": simulator.snapshot(),
        "control": {"workers": ctl["workers"], "slow": ctl["worker_delay"] >= config.SLOW_DELAY,
                    "persistent": ctl["persistent"], "poison": bool(ctl["poison"])},
        "scenarios": scenarios.state,   # escenario automático en curso y el último resultado de cada uno
        "events": events.latest()[:25],
    }


# --- DLQ: inspección, reprocesamiento y descarte ---------------------------
class DlqAction(BaseModel):
    order_ids: list[str] | None = None   # None = todos
    email_ids: list[str] | None = None   # con queue=correos


DlqQueue = Literal["pedidos", "correos"]


@app.get("/api/dlq", tags=["Panel (admin)"], summary="Ver una DLQ", description="Mensajes que agotaron sus 3 reintentos, con el error y la hora. queue=pedidos (por defecto), o queue=correos (correos que no se pudieron enviar: tipo y destinatario, sin el contenido).")
def dlq_list(queue: DlqQueue = "pedidos", _: dict = Depends(require_admin)):
    try:
        items = (dlq_emails if queue == "correos" else dlq).peek(50)
    except ConnectionError:
        raise HTTPException(503, "RabbitMQ no disponible")
    if queue == "correos":
        return items
    for it in items:                              # se enriquece con los datos del pedido
        try:
            o = service.get_order(it["order_id"])
            it["total"], it["customer"] = (o.total, o.customer_email) if o else (None, None)
        except (psycopg.Error, PoolTimeout):
            it["total"] = it["customer"] = None
    return items


@app.post("/api/dlq/reprocess", tags=["Panel (admin)"], summary="Reprocesar desde la DLQ", description="Devuelve a la cola principal los mensajes elegidos (o todos). queue=correos reenvía correos a la cola del mailer: los que ya salieron no se repiten (idempotencia por email_id).")
def dlq_reprocess(body: DlqAction, queue: DlqQueue = "pedidos", _: dict = Depends(require_admin)):
    if queue == "correos":
        done = dlq_emails.reprocess(set(body.email_ids) if body.email_ids else None)
        events.add("info", f"DLQ de correos: {len(done)} correo(s) devueltos a la cola del mailer", "inbox")
        return {"reprocessed": len(done)}
    # cada mensaje vuelve a la cola principal con retries=0 y sin los x-death anteriores (ver DeadLetterInspector)
    done = dlq.reprocess(set(body.order_ids) if body.order_ids else None)
    for order_id in done:
        service.mark_retrying(order_id)   # FAILED → en cola; uno ya confirmado o rechazado no se toca
    still = " Ojo: «Pedidos venenosos» sigue activo, los de prueba volverán a fallar." if control.get()["poison"] else ""
    events.add("info", f"DLQ: {len(done)} mensaje(s) devueltos a la cola para reprocesar.{still}", "inbox")
    return {"reprocessed": len(done)}


@app.post("/api/dlq/discard", tags=["Panel (admin)"], summary="Descartar de la DLQ", description="Elimina de la DLQ los mensajes elegidos (o todos). queue=pedidos (por defecto) o queue=correos.")
def dlq_discard(body: DlqAction, queue: DlqQueue = "pedidos", _: dict = Depends(require_admin)):
    if queue == "correos":
        done = dlq_emails.discard(set(body.email_ids) if body.email_ids else None)
        events.add("warn", f"DLQ de correos: {len(done)} correo(s) descartados (no se enviarán)", "inbox")
        return {"discarded": len(done)}
    done = dlq.discard(set(body.order_ids) if body.order_ids else None)
    events.add("warn", f"DLQ: {len(done)} mensaje(s) descartados definitivamente (el pedido queda FALLIDO)", "inbox")
    return {"discarded": len(done)}


# --- reconciliación ---------------------------------------------------------
@app.post("/api/reconcile", tags=["Panel (admin)"], summary="Reconciliar desde la BD", description="Reenvía los pedidos publicados que nunca se procesaron (mensajes perdidos en el broker).")
def reconcile(_: dict = Depends(require_admin)):
    n = service.reconcile_lost(config.LOST_AFTER_S)
    events.add("ok" if n else "info", f"Reconciliación: {n} pedido(s) recuperados desde PostgreSQL y reenviados", "database")
    return {"resent": n}


class SimStart(BaseModel):
    mode: Literal["manual", "day"] = "manual"
    rate: float = 10


@app.post("/api/sim/start", tags=["Simulador (admin)"], summary="Iniciar simulación", description="Modo manual (ritmo constante) o modo día (Black Friday con pico a medianoche).")
def sim_start(body: SimStart, _: dict = Depends(require_admin)):
    simulator.start(body.mode, body.rate)
    events.add("info", "¡Arranca el día de Black Friday! (24 h en %d s)" % config.DAY_SECONDS
               if body.mode == "day" else f"Tráfico manual iniciado: {body.rate:g} pedidos/s")
    return simulator.snapshot()


@app.post("/api/sim/stop", tags=["Simulador (admin)"], summary="Detener simulación")
def sim_stop(_: dict = Depends(require_admin)):
    simulator.stop()
    events.add("info", "Tráfico detenido")
    return simulator.snapshot()


class RateIn(BaseModel):
    rate: float


@app.post("/api/sim/rate", tags=["Simulador (admin)"], summary="Cambiar el ritmo", description="Pedidos por segundo de la simulación en curso.")
def sim_rate(body: RateIn, _: dict = Depends(require_admin)):
    simulator.set_rate(body.rate)
    return simulator.snapshot()


class BurstIn(BaseModel):
    count: int = Field(300, ge=1, le=2000)


@app.post("/api/sim/burst", tags=["Simulador (admin)"], summary="Enviar una ráfaga", description="Envía de golpe la cantidad de pedidos indicada (los botones 1 pedido y 5 pedidos). Con count=1 el pedido se crea al instante y la respuesta incluye su order_id.")
def sim_burst(body: BurstIn, _: dict = Depends(require_admin)):
    if body.count == 1:   # un solo pedido: se crea al instante y se devuelve su id para seguirlo en el panel
        return {"ok": True, "order_id": simulator.one()}
    simulator.burst(body.count)
    return {"ok": True}


@app.get("/api/stock", tags=["Panel (admin)"], summary="Stock por producto", description="Stock actual de cada producto (tabla products) junto al stock inicial de catalogo.json.")
def stock_list(_: dict = Depends(require_admin)):
    initial = products.initial_stock()
    return [{"sku": p.sku, "name": p.name, "category": p.category, "stock": p.stock, "initial": initial.get(p.sku)}
            for p in products.all()]


@app.post("/api/stock/reset", tags=["Panel (admin)"], summary="Restablecer stock", description="Vuelve a poner el stock de todos los productos según catalogo.json (para reiniciar la demo antes de la exposición). No toca pedidos ni colas.")
def stock_reset(_: dict = Depends(require_admin)):
    n = products.restore_stock()
    events.add("ok", f"Stock restablecido según catalogo.json ({n} productos)", "package")
    return {"restored": n}


@app.post("/api/reset", tags=["Panel (admin)"], summary="Reiniciar la demo", description="Vacía la base de datos (pedidos y outbox) y todas las colas (pedidos y correos), restablece el stock según catalogo.json y la configuración.")
def reset(_: dict = Depends(require_admin)):
    if scenarios.running:
        raise HTTPException(409, "Hay un escenario automático en curso: detenlo antes de reiniciar la demo")
    simulator.stop()
    time.sleep(1.5)                  # deja terminar lo que estaba en vuelo (simulador y workers)
    try:
        purge_queues()
    except Exception:
        pass
    service.repo.clear()
    products.restore_stock()         # sin pedidos, el stock vuelve al de catalogo.json
    time.sleep(0.5)
    try:
        purge_queues()               # por si algún mensaje en vuelo fue devuelto a la cola
    except Exception:
        pass
    simulator.reset_counters()
    control.reset()
    events.add("info", "Demo reiniciada: base de datos y colas vacías, stock restablecido")
    return {"ok": True}


def _revive_broker_app() -> bool:
    """Si el contenedor sigue en pie pero el broker fue detenido con stop_app, lo vuelve a arrancar."""
    return monitor.stats() is None and docker.broker_app("start_app")


def _need_docker():
    if not docker.available:
        raise HTTPException(503, "Docker no está disponible desde la API (falta montar el socket)")


@app.post("/api/chaos/{action}", tags=["Sabotajes (admin)"], summary="Sabotear o recuperar", description="Valores de action. Caídas: kill-worker, kill-relay, kill-mailer, crash-broker, crash-db. Recuperar: revive-worker, revive-relay, revive-mailer, revive-broker, revive-db, recover (todo). Worker lento: slow-on / slow-off. Pedidos venenosos: poison-on / poison-off. Persistencia: persist-on / persist-off. Escalar: workers-up / workers-down.")
def chaos(action: str, _: dict = Depends(require_admin)):
    if action in ("kill-worker", "kill-relay", "kill-mailer", "crash-broker", "crash-db", "recover", "revive-worker",
                  "revive-relay", "revive-mailer", "revive-broker", "revive-db"):
        _need_docker()
    run_chaos(action)
    return {"ok": True}


def run_chaos(action: str) -> None:
    """Un sabotaje o su recuperación. Lo usan los botones del panel y los escenarios automáticos (el mismo código)."""
    if action == "kill-mailer":
        docker.kill("mailer")
        events.add("bad", "SABOTAJE: el mailer cayó. Los correos esperan en la cola (en disco) y saldrán al recuperarlo", "skull")
    elif action == "kill-worker":
        docker.kill("worker")
        events.add("bad", "SABOTAJE: contenedor del worker eliminado. Los mensajes en proceso vuelven a la cola.", "skull")
    elif action == "kill-relay":
        docker.kill("relay")
        events.add("bad", "SABOTAJE: el relay cayó. Los pedidos se siguen guardando, pero nada llega a RabbitMQ: el outbox acumula", "skull")
    elif action == "crash-broker":
        # stop_app y no SIGKILL: un reinicio completo tarda 20-45 s, esto vuelve en ~4 s (ver DockerControl.broker_app).
        docker.broker_app("stop_app")
        events.add("bad", "SABOTAJE: RabbitMQ cayó. Relay y workers pierden la conexión. ¿Se pierden los mensajes?", "burst")
    elif action == "crash-db":
        docker.kill("postgres")
        events.add("bad", "SABOTAJE: PostgreSQL cayó de golpe (SIGKILL). ¿Se pierden los pedidos guardados?", "database")
        db_watch.mark_down()
    elif action in ("revive-worker", "revive-relay", "revive-mailer", "revive-broker", "revive-db"):   # deshace un sabotaje concreto
        target, label = {"revive-worker": ("worker", "El worker"), "revive-relay": ("relay", "El relay"), "revive-broker": ("rabbitmq", "RabbitMQ"),
                         "revive-mailer": ("mailer", "El mailer"), "revive-db": ("postgres", "PostgreSQL")}[action]
        if docker.start(target) or (target == "rabbitmq" and _revive_broker_app()):
            events.add("ok", f"{label} vuelve a levantarse", "wrench")
    elif action == "recover":
        started = [s for s in ("postgres", "rabbitmq", "relay", "worker", "mailer")
                   if docker.start(s) or (s == "rabbitmq" and _revive_broker_app())]
        control.set(worker_delay=config.WORKER_DELAY, poison=False)
        simulator.set_poison(False)
        events.add("ok", "Recuperación: " + (", ".join(started) + " reiniciado(s)" if started else "todo estaba en pie"), "wrench")
    elif action == "slow-on":
        control.set(worker_delay=config.SLOW_DELAY)
        events.add("warn", "SABOTAJE: workers lentos (dependencia externa degradada)", "snail")
    elif action == "slow-off":
        control.set(worker_delay=config.WORKER_DELAY)
        events.add("ok", "Workers de nuevo a velocidad normal")
    elif action == "poison-on":
        # el simulador marca 1 de cada 5 pedidos de prueba; el worker los hace fallar SOLO mientras el modo siga activo
        control.set(poison=True)
        simulator.set_poison(True)
        events.add("warn", "SABOTAJE: 1 de cada 5 pedidos de prueba fallará (reintentos 2 s → 5 s → 10 s → DLQ)", "flask")
    elif action == "poison-off":
        control.set(poison=False)
        simulator.set_poison(False)
        events.add("ok", "Pedidos venenosos desactivados: al reprocesar la DLQ, esos pedidos se completan")
    elif action == "persist-off":
        control.set(persistent=False)
        events.add("bad", "SABOTAJE: persistencia APAGADA. Los pedidos nuevos viajan en una cola solo en memoria", "disk")
    elif action == "persist-on":
        control.set(persistent=True)
        events.add("ok", "Persistencia activada: los mensajes vuelven a escribirse en disco", "disk")
    elif action in ("workers-up", "workers-down"):
        n = control.get()["workers"] + (1 if action == "workers-up" else -1)
        n = max(0, min(config.MAX_WORKERS, n))
        control.set(workers=n)
        events.add("info", f"Escalado: {n} worker(s) deseados")
    else:
        raise HTTPException(404, "Acción desconocida")


# --- escenarios automáticos -------------------------------------------------
class LiveScenarioOps:
    """Lo que el orquestador de escenarios puede hacer, con los MISMOS sabotajes de los botones (run_chaos), y lo que
    puede leer (colas, contenedores, BD). Solo crea y sigue pedidos de prueba: nunca toca pedidos de clientes."""
    KILL = {"worker": "kill-worker", "rabbitmq": "crash-broker", "postgres": "crash-db"}
    REVIVE = {"worker": "revive-worker", "rabbitmq": "revive-broker", "postgres": "revive-db"}

    def __init__(self):
        # stock que queda para los pedidos de prueba de este escenario: se lee al primer pedido y se descuenta lo enviado,
        # así el escenario nunca pide más de lo que hay (el cuadre espera que todos se procesen)
        self._stock: dict[str, int] | None = None

    def recover(self) -> None:
        simulator.stop()                  # sin tráfico simulado: el cuadre solo cuenta los pedidos del escenario
        run_chaos("recover")              # levanta lo caído, velocidad normal, venenosos apagado
        control.set(persistent=True)
        self._stock = None                # el próximo escenario vuelve a leer el stock

    def kill(self, service_name: str) -> None:
        run_chaos(self.KILL[service_name])

    def revive(self, service_name: str) -> None:
        run_chaos(self.REVIVE[service_name])

    def set_persistent(self, on: bool) -> None:
        run_chaos("persist-on" if on else "persist-off")

    def set_poison(self, on: bool) -> None:
        run_chaos("poison-on" if on else "poison-off")

    def set_worker_delay(self, seconds: float) -> None:
        control.set(worker_delay=seconds)

    def create_test_order(self, email: str, poison: bool = False) -> str:
        """Mismo camino que un pedido de la API (pedido + evento en una transacción). Productos del catálogo que aún
        tienen stock; el venenoso usa el SKU de prueba que falla solo con el modo activo. Sin tarjeta: se paga solo con
        la tarjeta de prueba aprobada (tok_test_approved)."""
        if poison:
            items = [{"sku": POISON_SKU, "quantity": 1, "unit_price": Decimal("9.90")}]
        else:
            catalog = products.all()
            if self._stock is None:
                self._stock = {p.sku: p.stock for p in catalog}
            with_stock = [p for p in catalog if self._stock.get(p.sku, 0) > 0]
            if not with_stock:
                raise ScenarioError("No queda stock para los pedidos de prueba: pulsa «Restablecer stock» en el panel")
            p = random.choice(with_stock)
            self._stock[p.sku] -= 1
            items = [{"sku": p.sku, "quantity": 1, "unit_price": p.price}]
        try:
            return service.create_order(email, items).id
        except (psycopg.OperationalError, PoolTimeout):   # la API respondería 503 «Servicio no disponible»
            raise DbUnavailable()

    def snapshot(self) -> dict:
        q, snap, st = monitor.stats(), db_watch.snapshot, docker.status() or {}
        return {"broker_up": q is not None, "db_up": snap["db_up"], "worker_up": st.get("worker") == "running",
                "consumers": q["consumers"] if q else 0, "depth": q["ready"] + q["unacked"] if q else None,
                "retrying": q["retrying"] if q else None, "retry_stages": q["retry_stages"] if q else None,
                "dlq": q["dlq"] if q else None, "in_disk": q["persistent"] if q else None,
                "in_memory": q["volatile"] if q else None, "outbox": snap["outbox_pending"]}

    def statuses(self, ids: list[str]) -> dict[str, str]:
        try:
            return service.repo.statuses(ids)
        except (psycopg.Error, PoolTimeout):
            raise DbUnavailable()

    def reconcile(self, ids: list[str]) -> int:
        """Como «Reconciliar desde la BD», pero solo con los pedidos del escenario."""
        sent = 0
        for order_id, status in self.statuses(ids).items():
            if status == OrderStatus.PUBLISHED.value:
                publisher.publish_order_created(order_id)
                sent += 1
        events.add("ok", f"Reconciliación: {sent} pedido(s) recuperados desde PostgreSQL y reenviados", "database")
        return sent

    def reprocess_dlq(self, ids: list[str]) -> int:
        done = dlq.reprocess(set(ids))
        for order_id in done:
            service.mark_retrying(order_id)
        events.add("info", f"DLQ: {len(done)} mensaje(s) devueltos a la cola para reprocesar", "inbox")
        return len(done)

    def clean_test_dlq(self) -> int:
        """Descarta de la DLQ solo los mensajes de pedidos de prueba de escenarios anteriores."""
        ids = [it["order_id"] for it in dlq.peek(1000) if it.get("order_id")]
        test = {i for i in ids if (o := service.get_order(i)) and o.customer_email.endswith("@" + TEST_DOMAIN)}
        return len(dlq.discard(test)) if test else 0


scenarios = ScenarioRunner(LiveScenarioOps(), events)


class ScenarioIn(BaseModel):
    speed: Literal["expo", "rapido"] = "expo"   # expo: cada estado se mantiene 6 s; rapido: 2 s
    count: Literal[300, 500] = 300              # solo Black Friday


@app.post("/api/scenarios/{n}/run", tags=["Sabotajes (admin)"], summary="Ejecutar un escenario automático",
          description="Corre solo toda la secuencia del escenario n (1 Black Friday, 2 Matar al worker, 3 Tirar RabbitMQ, "
                      "4 Prueba de persistencia, 5 Pedidos venenosos, 6 Tirar PostgreSQL) con pedidos de prueba. "
                      "Su avance (pasos, narración, resumen) aparece en /api/metrics → scenarios. 409 si ya corre otro.",
          responses={409: {"description": "Ya hay un escenario en curso"}})
def scenario_run(n: int, body: ScenarioIn, _: dict = Depends(require_admin)):
    _need_docker()
    if n not in SCENARIOS:
        raise HTTPException(404, "Escenario desconocido")
    if not scenarios.start(n, body.speed, body.count):
        raise HTTPException(409, "Ya hay un escenario en curso: espera a que termine o pulsa Detener")
    return scenarios.state


@app.post("/api/scenarios/stop", tags=["Sabotajes (admin)"], summary="Detener el escenario en curso",
          description="Corta el escenario y ejecuta «Recuperar todo».")
def scenario_stop(_: dict = Depends(require_admin)):
    return {"stopped": scenarios.stop()}
