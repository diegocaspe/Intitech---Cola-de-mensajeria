"""Límite de intentos fallidos de login por IP, contra la fuerza bruta.

Cuenta solo los intentos FALLIDOS: en la presentación muchos celulares con datos móviles salen a internet por la
misma IP pública (CGNAT del operador) y no deben bloquearse entre sí por iniciar sesión bien. Vive en memoria de la
API (un solo proceso): si la API se reinicia, los contadores empiezan de cero, suficiente para la demo.
"""
import threading
import time
from collections import deque


class LoginRateLimiter:
    def __init__(self, max_fails: int = 5, window_s: float = 60, clock=time.monotonic):
        self.max_fails = max_fails
        self.window_s = window_s
        self._clock = clock
        self._fails: dict[str, deque] = {}
        self._lock = threading.Lock()

    def _recent(self, key: str, now: float) -> deque:
        q = self._fails.get(key)
        if q is None:
            return deque()
        while q and now - q[0] >= self.window_s:
            q.popleft()
        if not q:
            del self._fails[key]   # no acumula IPs viejas en memoria
        return q

    def retry_after(self, key: str) -> int:
        """0 si la IP puede intentar; si no, los segundos que le faltan esperar."""
        with self._lock:
            now = self._clock()
            q = self._recent(key, now)
            if len(q) < self.max_fails:
                return 0
            return max(1, int(self.window_s - (now - q[0])) + 1)

    def fail(self, key: str) -> None:
        with self._lock:
            now = self._clock()
            self._recent(key, now)
            self._fails.setdefault(key, deque()).append(now)
    # Sin reset al acertar: si no, un atacante con su propia cuenta válida podría intercalar logins correctos para
    # vaciar su contador y seguir probando contraseñas ajenas sin límite.
