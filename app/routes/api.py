import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field

from app import config
from app.services.auth_service import AuthError, AuthService
from app.services.checkout_service import CheckoutError, CheckoutService
from app.services.events import EventLog
from app.services.order_service import OrderService
from app.services.simulator import OutboxRelay, TrafficSimulator
from app.models.order import OrderStatus
from app.utils.control import ControlStore
from app.utils.docker_control import DockerControl
from app.messaging.management import QueueMonitor
from app.messaging.lab import PersistenceLab
from app.messaging.rabbitmq import DeadLetterInspector, RabbitOrderPublisher, purge_queues
from app.repositories.catalog_repository import InMemoryProductRepository
from app.database.connection import create_pool, init_schema
from app.repositories.postgres_repository import PostgresOrderRepository, PostgresOutbox, PostgresUserRepository
from app.utils.security import PasswordHasher, TokenSigner

# --- composición de dependencias -------------------------------------------
events = EventLog()
pool = create_pool(config.DATABASE_URL, max_size=15)
control = ControlStore()
# el modo de persistencia (interruptor del panel) decide si cada pedido viaja por la cola durable o la volátil
publisher = RabbitOrderPublisher(persistent_mode=lambda: control.get()["persistent"])
service = OrderService(PostgresOrderRepository(pool), publisher, PostgresOutbox(pool))
auth = AuthService(PostgresUserRepository(pool), PasswordHasher())
tokens = TokenSigner(config.SECRET_KEY)


class DbWatcher:
    """Lee las cifras de la BD en segundo plano cada segundo. Las páginas responden al instante con el último
    resultado, aunque PostgreSQL esté caído (en ese caso se conservan los últimos valores conocidos)."""

    def __init__(self):
        self.snapshot = {"orders": {**{s.value: 0 for s in OrderStatus}, "total": 0}, "revenue": 0.0,
                         "recent": [], "outbox_recent": [], "last_min": 0, "outbox_pending": 0, "stale": 0,
                         "db_up": True}

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            up = self._read()
            if up and not self.snapshot["db_up"]:
                events.add("ok", "PostgreSQL responde de nuevo: los datos siguen ahí y los workers retoman la cola", "database")
            elif not up and self.snapshot["db_up"]:
                events.add("bad", "PostgreSQL no responde: la tienda no acepta pedidos nuevos y la cola espera", "database")
            self.snapshot = {**self.snapshot, "db_up": up}
            time.sleep(1)

    def _read(self) -> bool:
        try:
            counts = service.repo.count_by_status()
            self.snapshot = {
                **self.snapshot,
                "orders": {**counts, "total": sum(counts.values())},
                "revenue": service.repo.revenue(OrderStatus.PROCESSED),
                "recent": [{"id": o.id, "email": o.customer_email, "status": o.status.value, "total": o.total,
                            "items": [f"{i.quantity}× {i.sku}" for i in o.items], "created_at": o.created_at}
                           for o in service.repo.recent(10)],
                "outbox_recent": service.outbox.recent(6),   # para el detalle de PostgreSQL en el panel
                "last_min": service.repo.count_since(60),
                "outbox_pending": service.outbox.pending_count(),
                "stale": len(service.repo.stale_published(config.LOST_AFTER_S)),
            }
            return True
        except (psycopg.Error, PoolTimeout):
            return False


db_watch = DbWatcher()
products = InMemoryProductRepository()
checkout_service = CheckoutService(products, service)
simulator = TrafficSimulator(service, events, products)
relay = OutboxRelay(service, events)
docker = DockerControl()
monitor = QueueMonitor()
dlq = DeadLetterInspector()
lab = PersistenceLab(docker, events)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_schema(pool)
    auth.ensure_admin(config.ADMIN_EMAIL, config.ADMIN_PASSWORD)
    relay.start()
    db_watch.start()
    events.add("info", "Sistema listo. Todo lo que ves está respaldado por RabbitMQ + PostgreSQL.")
    yield


TAGS = [
    {"name": "Usuarios", "description": "Registro, inicio y cierre de sesión."},
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


@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(PoolTimeout)
def db_unavailable(_: Request, __: Exception):
    return JSONResponse({"detail": "Estamos con alta demanda. Intenta de nuevo en unos segundos."}, status_code=503)


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


def _start_session(user) -> JSONResponse:
    info = {"id": user.id, "name": user.name, "email": user.email, "role": user.role.value}
    resp = JSONResponse(info)
    resp.set_cookie(SESSION_COOKIE, tokens.sign(info, config.SESSION_DAYS * 86400),
                    max_age=config.SESSION_DAYS * 86400, httponly=True, samesite="lax")
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


@app.post("/api/auth/login", tags=["Usuarios"], summary="Iniciar sesión", description="Valida correo y contraseña y entrega la cookie de sesión.")
def login(body: LoginIn):
    try:
        user = auth.authenticate(body.email, body.password)
    except AuthError as exc:
        raise HTTPException(401, str(exc))
    return _start_session(user)


@app.post("/api/auth/logout", tags=["Usuarios"], summary="Cerrar sesión")
def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.get("/api/auth/me", tags=["Usuarios"], summary="Usuario actual", description="Datos del usuario con sesión iniciada, o null si no hay sesión.")
def me(request: Request):
    user = current_user(request)
    if not user:
        raise HTTPException(401, "Sin sesión")
    return {k: user[k] for k in ("id", "name", "email", "role")}


# --- API de la tienda -------------------------------------------------------
@app.get("/api/products", tags=["Tienda"], summary="Catálogo de productos", description="Los 100 productos con precio, descuento, stock e imagen.")
def list_products():
    return [{**asdict(p), "discount_pct": p.discount_pct, "image": f"/static/img/{p.image}"}
            for p in products.all()]


class CartLine(BaseModel):
    sku: str
    quantity: int


class CheckoutIn(BaseModel):
    items: list[CartLine] = Field(min_length=1, max_length=30)


@app.post("/api/checkout", status_code=201, tags=["Tienda"], summary="Comprar (checkout)", description="Convierte el carrito en un pedido. Solo se envían SKU y cantidad: los precios salen del catálogo del servidor. Requiere sesión.")
def checkout(body: CheckoutIn, user: dict = Depends(require_user)):
    try:
        order = checkout_service.checkout(user["email"], [l.model_dump() for l in body.items])
    except CheckoutError as exc:
        raise HTTPException(400, str(exc))
    events.add("ok", f"Compra real desde la tienda: {user['name']} · S/ {order.total:,.2f}", "cart")
    return service.track(order.id)


@app.get("/api/orders/{order_id}/track", tags=["Tienda"], summary="Seguir un pedido", description="Estado del pedido y su posición en la cola. El cliente lo consulta cada segundo.")
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


@app.get("/orders", tags=["Pedidos (admin)"], summary="Listar pedidos", description="Filtra opcionalmente por estado: PENDING, PUBLISHED, PROCESSED o FAILED.")
def list_orders(status: OrderStatus | None = None, limit: int = 100, _: dict = Depends(require_admin)):
    return [_out(o) for o in service.repo.list(status, limit)]


@app.get("/orders/{order_id}", tags=["Pedidos (admin)"], summary="Ver un pedido")
def get_order(order_id: str, _: dict = Depends(require_admin)):
    order = service.get_order(order_id)
    if not order:
        raise HTTPException(404, "Pedido no encontrado")
    return _out(order)


@app.post("/orders/republish-pending", tags=["Pedidos (admin)"], summary="Reenviar pendientes del outbox", description="Publica en RabbitMQ los pedidos que quedaron guardados sin enviar (por ejemplo, con el broker caído).")
def republish_pending(_: dict = Depends(require_admin)):
    return {"republished": service.relay_outbox(min_age_s=0)}


# --- API del panel ----------------------------------------------------------
@app.get("/api/metrics", tags=["Panel (admin)"], summary="Métricas del panel", description="Todo lo que el panel muestra: pedidos por estado, colas, outbox, DLQ, servicios y simulador.")
def metrics(_: dict = Depends(require_admin)):
    q = monitor.stats()
    ctl = control.get()
    snap = db_watch.snapshot
    in_flight = q["ready"] + q["unacked"] + q["retrying"] if q else None
    return {
        **snap,
        # perdidos = publicados hace rato, sin procesar, y que no están en ninguna cola (el broker los olvidó)
        "lost": snap["stale"] if in_flight == 0 else 0,
        "queue": q,
        "broker_up": q is not None,
        "services": docker.status(),
        "sim": simulator.snapshot(),
        "control": {"workers": ctl["workers"], "slow": ctl["worker_delay"] >= config.SLOW_DELAY,
                    "persistent": ctl["persistent"]},
        "lab": lab.state,
        "events": events.latest()[:25],
    }


# --- DLQ: inspección, reprocesamiento y descarte ---------------------------
class DlqAction(BaseModel):
    order_ids: list[str] | None = None   # None = todos


@app.get("/api/dlq", tags=["Panel (admin)"], summary="Ver la DLQ (pedidos fallidos)", description="Mensajes que agotaron sus 3 reintentos, con el error y la hora.")
def dlq_list(_: dict = Depends(require_admin)):
    try:
        items = dlq.peek(50)
    except ConnectionError:
        raise HTTPException(503, "RabbitMQ no disponible")
    for it in items:                              # se enriquece con los datos del pedido
        try:
            o = service.get_order(it["order_id"])
            it["total"], it["customer"] = (o.total, o.customer_email) if o else (None, None)
        except (psycopg.Error, PoolTimeout):
            it["total"] = it["customer"] = None
    return items


@app.post("/api/dlq/reprocess", tags=["Panel (admin)"], summary="Reprocesar desde la DLQ", description="Devuelve a la cola principal los pedidos elegidos (o todos).")
def dlq_reprocess(body: DlqAction, _: dict = Depends(require_admin)):
    done = dlq.reprocess(set(body.order_ids) if body.order_ids else None)
    for order_id in done:
        service.mark_retrying(order_id)
    events.add("info", f"DLQ: {len(done)} mensaje(s) devueltos a la cola para reprocesar", "inbox")
    return {"reprocessed": len(done)}


@app.post("/api/dlq/discard", tags=["Panel (admin)"], summary="Descartar de la DLQ", description="Elimina de la DLQ los pedidos elegidos (o todos).")
def dlq_discard(body: DlqAction, _: dict = Depends(require_admin)):
    done = dlq.discard(set(body.order_ids) if body.order_ids else None)
    events.add("warn", f"DLQ: {len(done)} mensaje(s) descartados definitivamente (el pedido queda FALLIDO)", "inbox")
    return {"discarded": len(done)}


# --- experimento de persistencia y reconciliación -------------------------
@app.post("/api/lab/run", tags=["Panel (admin)"], summary="Experimento de persistencia", description="Compara una cola en disco contra una en memoria reiniciando RabbitMQ.")
def lab_run(_: dict = Depends(require_admin)):
    if not docker.available:
        raise HTTPException(503, "Docker no está disponible desde la API (falta montar el socket)")
    if not lab.start():
        raise HTTPException(409, "El experimento ya está en curso")
    return lab.state


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


@app.post("/api/reset", tags=["Panel (admin)"], summary="Reiniciar la demo", description="Vacía la base de datos y las colas, y restablece la configuración.")
def reset(_: dict = Depends(require_admin)):
    simulator.stop()
    time.sleep(1.5)                  # deja terminar lo que estaba en vuelo (simulador y workers)
    try:
        purge_queues()
    except Exception:
        pass
    service.repo.clear()
    time.sleep(0.5)
    try:
        purge_queues()               # por si algún mensaje en vuelo fue devuelto a la cola
    except Exception:
        pass
    simulator.reset_counters()
    control.reset()
    events.add("info", "Demo reiniciada: base de datos y colas vacías")
    return {"ok": True}


def _need_docker():
    if not docker.available:
        raise HTTPException(503, "Docker no está disponible desde la API (falta montar el socket)")


@app.post("/api/chaos/{action}", tags=["Sabotajes (admin)"], summary="Sabotear o recuperar", description="Valores de action. Caídas: kill-worker, crash-broker, crash-db. Recuperar: revive-worker, revive-broker, revive-db, recover (todo). Worker lento: slow-on / slow-off. Pedidos con error: poison (uno), poison-on / poison-off. Persistencia: persist-on / persist-off. Escalar: workers-up / workers-down.")
def chaos(action: str, _: dict = Depends(require_admin)):
    if action in ("kill-worker", "crash-broker", "crash-db", "recover", "revive-worker", "revive-broker", "revive-db"):
        _need_docker()
    if action == "kill-worker":
        docker.kill("worker")
        events.add("bad", "SABOTAJE: contenedor del worker eliminado. Los mensajes en proceso vuelven a la cola.", "skull")
    elif action == "crash-broker":
        docker.kill("rabbitmq")
        events.add("bad", "SABOTAJE: RabbitMQ cayó de golpe (SIGKILL). ¿Se pierden los mensajes?", "burst")
    elif action == "crash-db":
        docker.kill("postgres")
        events.add("bad", "SABOTAJE: PostgreSQL cayó de golpe (SIGKILL). ¿Se pierden los pedidos guardados?", "database")
    elif action in ("revive-worker", "revive-broker", "revive-db"):   # deshace un sabotaje concreto
        target, label = {"revive-worker": ("worker", "El worker"), "revive-broker": ("rabbitmq", "RabbitMQ"),
                         "revive-db": ("postgres", "PostgreSQL")}[action]
        if docker.start(target):
            events.add("ok", f"{label} vuelve a levantarse", "wrench")
    elif action == "recover":
        started = [s for s in ("postgres", "rabbitmq", "worker") if docker.start(s)]
        control.set(worker_delay=config.WORKER_DELAY)
        simulator.set_poison(False)
        events.add("ok", "Recuperación: " + (", ".join(started) + " reiniciado(s)" if started else "todo estaba en pie"), "wrench")
    elif action == "slow-on":
        control.set(worker_delay=config.SLOW_DELAY)
        events.add("warn", "SABOTAJE: workers lentos (dependencia externa degradada)", "snail")
    elif action == "slow-off":
        control.set(worker_delay=config.WORKER_DELAY)
        events.add("ok", "Workers de nuevo a velocidad normal")
    elif action == "poison-on":
        simulator.set_poison(True)
        events.add("warn", "SABOTAJE: 1 de cada 5 pedidos simulados será venenoso (falla siempre → reintentos → DLQ)", "flask")
    elif action == "poison-off":
        simulator.set_poison(False)
        events.add("ok", "Pedidos venenosos desactivados")
    elif action == "poison":
        for _ in range(5):
            service.create_order("saboteador@mail.com", [{"sku": "FAIL", "quantity": 1, "unit_price": 9.9}])
        events.add("warn", "SABOTAJE: 5 pedidos venenosos enviados (siempre fallan)", "flask")
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
    return {"ok": True}
