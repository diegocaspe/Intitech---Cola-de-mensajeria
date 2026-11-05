"""Sabotaje real: la API puede matar/levantar contenedores hermanos a través del socket de Docker."""
import logging
import socket
import threading
import time

log = logging.getLogger(__name__)


class DockerControl:
    SERVICES = ("api", "relay", "worker", "mailer", "rabbitmq", "postgres")
    REFRESH_S = 0.3

    def __init__(self):
        self.client = None
        self.project = None
        self._status = None
        try:
            import docker
            self.client = docker.from_env()
            self.client.ping()
            own = self.client.containers.get(socket.gethostname())
            self.project = own.labels.get("com.docker.compose.project")
        except Exception as exc:
            log.warning("Docker no disponible, sabotajes reales desactivados: %s", exc)
            self.client = None
        if self.client:
            self._status = self._read_status()
            threading.Thread(target=self._watch, daemon=True, name="docker-status").start()

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
        """Último estado leído por el hilo de fondo: el socket de Docker Desktop tarda 70-600 ms por consulta y el
        panel pide /api/metrics 4 veces por segundo; si esperara a Docker, se atrasaría respecto al celular."""
        return self._status if self.available else None

    def _watch(self) -> None:
        while True:
            time.sleep(self.REFRESH_S)
            self._status = self._read_status() or self._status   # un fallo puntual no borra el último estado

    def _read_status(self) -> dict[str, str] | None:
        # una sola consulta "sparse": no inspecciona cada contenedor por separado
        label = f"com.docker.compose.project={self.project}" if self.project else "com.docker.compose.service"
        try:
            found = self.client.containers.list(all=True, sparse=True, filters={"label": label})
            by_service = {c.attrs.get("Labels", {}).get("com.docker.compose.service"): c.attrs.get("State") for c in found}
            return {s: by_service.get(s) or "missing" for s in self.SERVICES}
        except Exception:
            return None

    def kill(self, service: str) -> bool:
        c = self._find(service)
        if c and c.status == "running":
            c.kill()  # SIGKILL: caída brusca, sin apagado ordenado
            self._status = self._read_status()   # el panel lo ve en la siguiente consulta
            return True
        return False

    def start(self, service: str) -> bool:
        c = self._find(service)
        if c and c.status != "running":
            c.start()
            self._status = self._read_status()
            return True
        return False

    # Caída "rápida" del broker para la demo: tras un SIGKILL, RabbitMQ tarda 20-45 s en volver a arrancar
    # (Erlang carga y compila cientos de módulos con un solo núcleo). stop_app detiene solo la aplicación:
    # el puerto 5672 se cierra y los clientes reciben "conexión rechazada", pero la VM de Erlang sigue viva y
    # start_app la levanta en ~4 s. Las colas durables se recuperan desde disco igual que en un reinicio.
    # detach: el CLI de rabbitmqctl tarda ~3 s en arrancar; la petición del panel no lo espera.
    def broker_app(self, action: str, service: str = "rabbitmq") -> bool:
        c = self._find(service)
        if not c or c.status != "running":
            return False
        c.exec_run(["rabbitmqctl", "-q", action], detach=True)
        return True
