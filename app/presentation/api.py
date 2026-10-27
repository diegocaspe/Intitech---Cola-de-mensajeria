import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field

from app import config
from app.application.auth_service import AuthError, AuthService
from app.application.checkout_service import CheckoutError, CheckoutService
from app.application.events import EventLog
from app.application.order_service import OrderService
from app.application.simulator import OutboxRelay, TrafficSimulator
from app.domain.order import OrderStatus
from app.infrastructure.control import ControlStore
from app.infrastructure.docker_control import DockerControl
from app.infrastructure.messaging.management import QueueMonitor
from app.infrastructure.messaging.lab import PersistenceLab
from app.infrastructure.messaging.rabbitmq import DeadLetterInspector, RabbitOrderPublisher, purge_queues
from app.infrastructure.persistence.catalog import InMemoryProductRepository
from app.infrastructure.persistence.postgres import (PostgresOrderRepository, PostgresOutbox, PostgresUserRepository,
                                                     create_pool, init_schema)
from app.infrastructure.security import PasswordHasher, TokenSigner

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
                         "recent": [], "last_min": 0, "outbox_pending": 0, "stale": 0, "db_up": True}

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
                            "items": [f"{i.quantity}× {i.sku}" for i in o.items]} for o in service.repo.recent(10)],
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


app = FastAPI(title="Tienda E-commerce - Black Friday", version="3.0", lifespan=lifespan)
STATIC = Path(__file__).parent / "static"
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


@app.post("/api/auth/register", status_code=201)
def register(body: RegisterIn):
    try:
        user = auth.register(body.name, body.email, body.password)
    except AuthError as exc:
        raise HTTPException(400, str(exc))
    events.add("info", f"Nuevo cliente registrado: {user.name}", "user")
    resp = _start_session(user)
    resp.status_code = 201
    return resp


@app.post("/api/auth/login")
def login(body: LoginIn):
    try:
        user = auth.authenticate(body.email, body.password)
    except AuthError as exc:
        raise HTTPException(401, str(exc))
    return _start_session(user)


@app.post("/api/auth/logout")
def logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.get("/api/auth/me")
def me(request: Request):
    user = current_user(request)
    if not user:
        raise HTTPException(401, "Sin sesión")
    return {k: user[k] for k in ("id", "name", "email", "role")}


# --- API de la tienda -------------------------------------------------------
@app.get("/api/products")
def list_products():
    return [{**asdict(p), "discount_pct": p.discount_pct, "image": f"/static/img/{p.image}"}
            for p in products.all()]


class CartLine(BaseModel):
    sku: str
    quantity: int


class CheckoutIn(BaseModel):
    items: list[CartLine] = Field(min_length=1, max_length=30)


@app.post("/api/checkout", status_code=201)
def checkout(body: CheckoutIn, user: dict = Depends(require_user)):
    try:
        order = checkout_service.checkout(user["email"], [l.model_dump() for l in body.items])
    except CheckoutError as exc:
        raise HTTPException(400, str(exc))
    events.add("ok", f"Compra real desde la tienda: {user['name']} · S/ {order.total:,.2f}", "cart")
    return service.track(order.id)


@app.get("/api/orders/{order_id}/track")
def track(order_id: str, user: dict = Depends(require_user)):
    order = service.get_order(order_id)
    if not order or (order.customer_email != user["email"] and user["role"] != "admin"):
        raise HTTPException(404, "Pedido no encontrado")
    return service.track(order_id)


@app.get("/api/my/orders")
def my_orders(user: dict = Depends(require_user)):
    return service.orders_of(user["email"])


@app.get("/api/store/live")
def store_live():
    q = monitor.stats()
    snap = db_watch.snapshot
    return {"orders_last_min": snap["last_min"], "in_queue": q and q["ready"] + q["unacked"],
            "broker_up": q is not None, "db_up": snap["db_up"]}


def _store_url(request: Request) -> str:
    return config.PUBLIC_URL or str(request.base_url).rstrip("/")


@app.get("/api/store-url")
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
    unit_price: float = Field(ge=0)


class OrderIn(BaseModel):
    customer_email: str
    items: list[ItemIn] = Field(min_length=1)


@app.post("/orders", status_code=201)
def create_order(body: OrderIn, _: dict = Depends(require_admin)):
    return _out(service.create_order(body.customer_email, [i.model_dump() for i in body.items]))


@app.get("/orders")
def list_orders(status: OrderStatus | None = None, limit: int = 100, _: dict = Depends(require_admin)):
    return [_out(o) for o in service.repo.list(status, limit)]


@app.get("/orders/{order_id}")
def get_order(order_id: str, _: dict = Depends(require_admin)):
    order = service.get_order(order_id)
    if not order:
        raise HTTPException(404, "Pedido no encontrado")
    return _out(order)


@app.post("/orders/republish-pending")
def republish_pending(_: dict = Depends(require_admin)):
    return {"republished": service.relay_outbox(min_age_s=0)}


# --- API del panel ----------------------------------------------------------
@app.get("/api/metrics")
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


@app.get("/api/dlq")
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


@app.post("/api/dlq/reprocess")
def dlq_reprocess(body: DlqAction, _: dict = Depends(require_admin)):
    done = dlq.reprocess(set(body.order_ids) if body.order_ids else None)
    for order_id in done:
        service.mark_retrying(order_id)
    events.add("info", f"DLQ: {len(done)} mensaje(s) devueltos a la cola para reprocesar", "inbox")
    return {"reprocessed": len(done)}


@app.post("/api/dlq/discard")
def dlq_discard(body: DlqAction, _: dict = Depends(require_admin)):
    done = dlq.discard(set(body.order_ids) if body.order_ids else None)
    events.add("warn", f"DLQ: {len(done)} mensaje(s) descartados definitivamente (el pedido queda FALLIDO)", "inbox")
    return {"discarded": len(done)}


# --- experimento de persistencia y reconciliación -------------------------
@app.post("/api/lab/run")
def lab_run(_: dict = Depends(require_admin)):
    if not docker.available:
        raise HTTPException(503, "Docker no está disponible desde la API (falta montar el socket)")
    if not lab.start():
        raise HTTPException(409, "El experimento ya está en curso")
    return lab.state


@app.post("/api/reconcile")
def reconcile(_: dict = Depends(require_admin)):
    n = service.reconcile_lost(config.LOST_AFTER_S)
    events.add("ok" if n else "info", f"Reconciliación: {n} pedido(s) recuperados desde PostgreSQL y reenviados", "database")
    return {"resent": n}


class SimStart(BaseModel):
    mode: Literal["manual", "day"] = "manual"
    rate: float = 10


@app.post("/api/sim/start")
def sim_start(body: SimStart, _: dict = Depends(require_admin)):
    simulator.start(body.mode, body.rate)
    events.add("info", "¡Arranca el día de Black Friday! (24 h en %d s)" % config.DAY_SECONDS
               if body.mode == "day" else f"Tráfico manual iniciado: {body.rate:g} pedidos/s")
    return simulator.snapshot()


@app.post("/api/sim/stop")
def sim_stop(_: dict = Depends(require_admin)):
    simulator.stop()
    events.add("info", "Tráfico detenido")
    return simulator.snapshot()


class RateIn(BaseModel):
    rate: float


@app.post("/api/sim/rate")
def sim_rate(body: RateIn, _: dict = Depends(require_admin)):
    simulator.set_rate(body.rate)
    return simulator.snapshot()


class BurstIn(BaseModel):
    count: int = Field(300, ge=1, le=2000)


@app.post("/api/sim/burst")
def sim_burst(body: BurstIn, _: dict = Depends(require_admin)):
    simulator.burst(body.count)
    return {"ok": True}


@app.post("/api/reset")
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


@app.post("/api/chaos/{action}")
def chaos(action: str, _: dict = Depends(require_admin)):
    if action in ("kill-worker", "crash-broker", "crash-db", "recover"):
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
    elif action == "recover":
        started = [s for s in ("postgres", "rabbitmq", "worker") if docker.start(s)]
        control.set(worker_delay=config.WORKER_DELAY)
        events.add("ok", "Recuperación: " + (", ".join(started) + " reiniciado(s)" if started else "todo estaba en pie"), "wrench")
    elif action == "slow-on":
        control.set(worker_delay=config.SLOW_DELAY)
        events.add("warn", "SABOTAJE: workers lentos (dependencia externa degradada)", "snail")
    elif action == "slow-off":
        control.set(worker_delay=config.WORKER_DELAY)
        events.add("ok", "Workers de nuevo a velocidad normal")
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
