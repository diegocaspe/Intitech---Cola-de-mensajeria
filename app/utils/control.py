"""Configuración en caliente compartida entre la API y el worker (archivo JSON en el volumen)."""
import json
import os
import threading
import time

from app import config


class ControlStore:
    CACHE_S = 0.5   # se lee en cada mensaje/publicación: una caché corta evita leer el archivo cientos de veces

    def __init__(self, path: str = config.CONTROL_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._cache: tuple[float, dict] | None = None

    def _defaults(self) -> dict:
        return {"workers": config.WORKERS, "worker_delay": config.WORKER_DELAY, "persistent": True}

    def get(self) -> dict:
        cached = self._cache
        if cached and time.monotonic() - cached[0] < self.CACHE_S:
            return cached[1]
        try:
            with open(self.path, encoding="utf-8") as f:
                data = {**self._defaults(), **json.load(f)}
        except (OSError, ValueError):
            data = self._defaults()
        self._cache = (time.monotonic(), data)
        return data

    def set(self, **values) -> dict:
        with self._lock:
            self._cache = None
            data = {**self.get(), **values}
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, self.path)  # escritura atómica
            self._cache = None
            return data

    def reset(self) -> dict:
        return self.set(**self._defaults())
