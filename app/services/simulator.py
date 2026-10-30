"""Generador de tráfico: simula clientes comprando durante un día de Black Friday."""
import math
import random
import threading
import time

from app import config
from app.services.events import EventLog
from app.services.order_service import OrderService
from app.repositories.interfaces import ProductRepository

POISON_EVERY = 5
NAMES = ["ana", "luis", "marta", "pedro", "sofia", "juan", "carla", "diego", "elena", "raul", "paula", "nico"]


def _gap(a: float, b: float) -> float:
    d = abs(a - b) % 24
    return min(d, 24 - d)


def traffic_shape(hour: float) -> float:
    """Intensidad 0..1 según la hora del día: pico a medianoche (apertura de ofertas), almuerzo y noche."""
    v = (1.0 * math.exp(-(_gap(hour, 0.0) / 1.2) ** 2)
         + 0.45 * math.exp(-(_gap(hour, 13.0) / 2.0) ** 2)
         + 0.70 * math.exp(-(_gap(hour, 20.0) / 2.0) ** 2))
    return max(0.06, min(1.0, v))


class TrafficSimulator:
    def __init__(self, service: OrderService, events: EventLog, products: ProductRepository):
        self.service = service
        self.events = events
        self.products = products
        self.running = False
        self.mode = "manual"          # manual | day
        self.rate = 10.0              # pedidos/seg (en modo day: el pico del día)
        self.sent = 0
        self._t0 = 0.0
        self._failing = False
        self.poison = False           # modo venenoso: 1 de cada POISON_EVERY pedidos simulados siempre falla
        self._poison_slots: list[bool] = []
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    # --- control ----------------------------------------------------------
    def start(self, mode: str, rate: float) -> None:
        self.mode, self.rate = mode, max(0.5, min(150.0, rate))
        self._t0 = time.monotonic()
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.running = False

    def set_rate(self, rate: float) -> None:
        self.rate = max(0.5, min(150.0, rate))

    def one(self) -> str:
        """Un solo pedido, al instante: devuelve su id para que el panel lo siga en cámara lenta."""
        self.events.add("info", "Un cliente hace un pedido")
        return self._new_order()

    def burst(self, count: int) -> None:
        threading.Thread(target=self._burst, args=(min(count, 2000),), daemon=True).start()

    def reset_counters(self) -> None:
        self.sent = 0
        self.set_poison(False)

    def set_poison(self, on: bool) -> None:
        with self._lock:
            self.poison, self._poison_slots = on, []

    def _next_is_poison(self) -> bool:
        """En cada bloque de POISON_EVERY pedidos, uno al azar es venenoso: siempre 1 de cada 5, sin patrón fijo."""
        with self._lock:
            if not self.poison:
                return False
            if not self._poison_slots:
                self._poison_slots = [True] + [False] * (POISON_EVERY - 1)
                random.shuffle(self._poison_slots)
            return self._poison_slots.pop()

    # --- estado -----------------------------------------------------------
    def virtual_hour(self) -> float | None:
        if not (self.running and self.mode == "day"):
            return None
        return (23.0 + (time.monotonic() - self._t0) / config.DAY_SECONDS * 24) % 24

    def current_rate(self) -> float:
        if not self.running:
            return 0.0
        h = self.virtual_hour()
        return self.rate if h is None else self.rate * traffic_shape(h)

    def snapshot(self) -> dict:
        h = self.virtual_hour()
        return {"running": self.running, "mode": self.mode, "rate": self.rate,
                "current_rate": round(self.current_rate(), 1), "sent": self.sent, "poison": self.poison,
                "virtual_hour": None if h is None else round(h, 2)}

    # --- internos ---------------------------------------------------------
    def _new_order(self) -> str:
        if self._next_is_poison():   # producto inexistente: el worker falla siempre → reintentos → DLQ
            order = self.service.create_order(f"{random.choice(NAMES)}{random.randint(1, 999)}@mail.com",
                                              [{"sku": "FAIL", "quantity": 1, "unit_price": 9.9}])
            with self._lock:
                self.sent += 1
            return order.id
        # los clientes simulados también hacen cross-selling: un producto y a veces sus relacionados
        main = random.choice(self.products.all())
        picks = [main] + [self.products.get(s) for s in random.sample(main.related, random.choice([0, 0, 1, 2]))]
        items = [{"sku": p.sku, "quantity": random.choice([1, 1, 1, 2]), "unit_price": p.price} for p in picks]
        email = f"{random.choice(NAMES)}{random.randint(1, 999)}@mail.com"
        order = self.service.create_order(email, items)
        with self._lock:
            self.sent += 1
        return order.id

    def _burst(self, n: int) -> None:
        self.events.add("info", "Un cliente hace un pedido" if n == 1 else f"Oleada de {n} clientes comprando a la vez")
        for _ in range(n):
            try:
                self._new_order()
            except Exception as exc:
                self.events.add("bad", f"Oleada interrumpida: {exc.__class__.__name__}")
                return

    def _loop(self) -> None:
        next_t = time.monotonic()
        while self.running:
            if self.mode == "day" and time.monotonic() - self._t0 >= config.DAY_SECONDS:
                self.running = False
                self.events.add("info", "Terminó el día de Black Friday (24 h simuladas)")
                break
            try:
                self._new_order()
                if self._failing:
                    self._failing = False
                    self.events.add("ok", "Los clientes vuelven a poder comprar")
            except Exception as exc:
                if not self._failing:     # un solo aviso por caída, no uno por pedido
                    self._failing = True
                    self.events.add("bad", f"Los pedidos se rechazan: {exc.__class__.__name__} (¿base de datos caída?)")
                time.sleep(0.5)
            next_t += 1.0 / max(self.current_rate(), 0.5)
            now = time.monotonic()
            if next_t < now - 1.0:   # nos atrasamos (BD lenta): no acumular ráfagas
                next_t = now
            time.sleep(max(0.0, next_t - now))


class OutboxRelay:
    """Relay del Transactional Outbox: publica los eventos que quedaron sin enviar en cuanto el broker vuelve."""

    def __init__(self, service: OrderService, events: EventLog, interval: float = 1.5):
        self.service, self.events, self.interval = service, events, interval
        self._was_healthy = True

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            time.sleep(self.interval)
            try:
                n = self.service.relay_outbox(min_age_s=3)
                healthy = self.service.publisher.is_healthy()
                if self._was_healthy and not healthy:
                    self.events.add("bad", "RabbitMQ no responde: los eventos de los pedidos nuevos esperan en el outbox", "octagon")
                if n:
                    self.events.add("ok", f"Relay del outbox: {n} eventos pendientes publicados en RabbitMQ", "database")
                self._was_healthy = healthy
            except Exception:
                pass                      # BD caída: se reintenta en la siguiente vuelta
