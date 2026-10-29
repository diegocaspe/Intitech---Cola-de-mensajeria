import threading
import time
from collections import deque


class EventLog:
    """Bitácora en memoria que el panel muestra en vivo. kind: info | ok | warn | bad"""

    def __init__(self, size: int = 40):
        self._events = deque(maxlen=size)
        self._lock = threading.Lock()

    def add(self, kind: str, text: str, icon: str | None = None) -> None:
        """icon: nombre de un ícono de static/icons.svg (p. ej. "database")."""
        with self._lock:
            self._events.append({"t": time.time(), "kind": kind, "text": text, "icon": icon})

    def latest(self) -> list[dict]:
        with self._lock:
            return list(reversed(self._events))
