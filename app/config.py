import os
import sys
from urllib.parse import quote

# Los secretos NO tienen valor por defecto: vienen del archivo .env (ver .env.example y README → Configuración).
# Cada proceso llama a require_env() al arrancar para fallar con un mensaje claro si falta alguno.

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_MGMT_PORT = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "")

# DATABASE_URL completa si se define; si no, se arma con las mismas variables que usa el contenedor de PostgreSQL
# (quote() escapa caracteres como @ o : que romperían la URL si aparecen en la contraseña)
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DATABASE_URL = os.getenv("DATABASE_URL") or (
    f"postgresql://{quote(POSTGRES_USER, safe='')}:{quote(POSTGRES_PASSWORD, safe='')}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{quote(POSTGRES_DB, safe='')}"
)
# Archivo compartido (volumen) con el que la API controla al worker en caliente
CONTROL_PATH = os.getenv("CONTROL_PATH", "data/control.json")

# Seguridad: firma de sesiones y cuenta de administrador (se crea/actualiza al arrancar la API)
SECRET_KEY = os.getenv("SECRET_KEY", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
SESSION_DAYS = 7

STORE_NAME = "Inti Tech"

# Correos: el mailer los envía por Gmail (SMTP_SSL). Con EMAIL_ENABLED=false solo escribe "correo simulado a ..." en el log
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").strip().lower() in ("1", "true", "yes", "si", "sí")
GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "")   # Google la muestra en grupos de 4
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
VERIFY_TOKEN_HOURS = 24     # enlace de verificación de la cuenta
RESET_TOKEN_MINUTES = 60    # enlace para restablecer la contraseña
RESET_THROTTLE_S = 60       # como mucho un correo de recuperación por minuto y cuenta

# Variables obligatorias según el proceso (la API además firma sesiones y crea la cuenta de administrador)
_REQUIRED_COMMON = ("RABBITMQ_USER", "RABBITMQ_PASS")
_REQUIRED_DB = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")   # no hacen falta si se da DATABASE_URL
_REQUIRED_API = ("SECRET_KEY", "ADMIN_EMAIL", "ADMIN_PASSWORD")
_REQUIRED_MAILER = ("GMAIL_USER", "GMAIL_APP_PASSWORD")                # solo si EMAIL_ENABLED=true


def require_env(process: str) -> None:
    """Corta el arranque con un mensaje claro si falta alguna variable obligatoria. Solo nombra las variables: nunca
    imprime sus valores."""
    names = list(_REQUIRED_COMMON)
    if not os.getenv("DATABASE_URL"):
        names += _REQUIRED_DB
    if process == "api":
        names += _REQUIRED_API
    if process == "mailer" and EMAIL_ENABLED:
        names += _REQUIRED_MAILER
    missing = [n for n in names if not os.getenv(n, "").strip()]
    if missing:
        sys.exit(
            f"[{process}] Falta configuración obligatoria: {', '.join(missing)}.\n"
            f"  Copia .env.example a .env, rellena esas variables y vuelve a levantar con: docker compose up --build"
        )

RELAY_POLL_S = float(os.getenv("RELAY_POLL_S", "0.25"))   # cada cuánto revisa el relay el outbox (rápido: el panel va a la par de la tienda)

# Topología de RabbitMQ
EXCHANGE = "tienda.orders"
DLX = "tienda.orders.dlx"
QUEUE = "orders.created"
DLQ = "orders.created.dlq"
ROUTING_KEY = "order.created"

# Correos: mismo esquema que los pedidos (cola durable, reintentos con TTL y DLQ), en su propio exchange
EMAIL_EXCHANGE = "tienda.correos"
EMAIL_QUEUE = "correos"
EMAIL_DLQ = "correos.dlq"
EMAIL_ROUTING_KEY = "email"

# Modo "sin persistencia" (para comparar): exchange y cola que viven solo en memoria
VOLATILE_EXCHANGE = "tienda.orders.volatil"
VOLATILE_QUEUE = "orders.created.volatil"

# Reintentos con espera creciente: cada intento fallido espera en una cola con TTL y luego vuelve a la principal
RETRY_DELAYS = (2, 5, 10)                       # segundos de espera antes del 1er, 2º y 3er reintento
RETRY_QUEUES = tuple(f"orders.retry.{d}s" for d in RETRY_DELAYS)
MAX_RETRIES = len(RETRY_DELAYS)
EMAIL_RETRY_QUEUES = tuple(f"correos.retry.{d}s" for d in RETRY_DELAYS)   # mismas esperas para los correos

# Pedidos PUBLISHED sin procesar tras este tiempo (con la cola vacía) se consideran mensajes perdidos
LOST_AFTER_S = 15

# Experimento de persistencia (colas propias, no tocan los pedidos reales)
LAB_MESSAGES = int(os.getenv("LAB_MESSAGES", "500"))
LAB_DURABLE_QUEUE = "lab.persistente"
LAB_VOLATILE_QUEUE = "lab.volatil"

# Worker
WORKERS = int(os.getenv("WORKERS", "1"))
WORKER_DELAY = float(os.getenv("WORKER_DELAY", "0.15"))   # segundos que tarda "procesar" un pedido
SLOW_DELAY = float(os.getenv("SLOW_DELAY", "1.5"))        # delay cuando se sabotea al worker
MAX_WORKERS = 10

# Simulación de un día de Black Friday comprimido en estos segundos
DAY_SECONDS = int(os.getenv("DAY_SECONDS", "150"))

# URL con la que el público entra a la tienda desde sus celulares (se muestra como QR en el panel)
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
