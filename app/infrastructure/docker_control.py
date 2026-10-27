"""Sabotaje real: la API puede matar/levantar contenedores hermanos a través del socket de Docker."""
import logging
import socket

log = logging.getLogger(__name__)


class DockerControl:
    SERVICES = ("api", "worker", "rabbitmq", "postgres")

    def __init__(self):
        self.client = None
        self.project = None
        try:
            import docker
            self.client = docker.from_env()
            self.client.ping()
            own = self.client.containers.get(socket.gethostname())
            self.project = own.labels.get("com.docker.compose.project")
        except Exception as exc:
            log.warning("Docker no disponible, sabotajes reales desactivados: %s", exc)
            self.client = None

    @property
    def available(self) -> bool:
        return self.client is not None

    def _find(self, service: str):
        labels = [f"com.docker.compose.service={service}"]
        if self.project:
            labels.append(f"com.docker.compose.project={self.project}")
        found = self.client.containers.list(all=True, filters={"label": labels})
        return found[0] if found else None

    def status(self) -> dict[str, str] | None:
        if not self.available:
            return None
        try:
            return {s: (c.status if (c := self._find(s)) else "missing") for s in self.SERVICES}
        except Exception:
            return None

    def kill(self, service: str) -> bool:
        c = self._find(service)
        if c and c.status == "running":
            c.kill()  # SIGKILL: caída brusca, sin apagado ordenado
            return True
        return False

    def start(self, service: str) -> bool:
        c = self._find(service)
        if c and c.status != "running":
            c.start()
            return True
        return False
