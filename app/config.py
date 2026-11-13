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
# API HTTP de management (solo la usa la api para leer los mensajes "en proceso"). En la laptop: el mismo usuario de
# AMQP y sin prefijo. En el servidor (docker-compose.prod.yml) RABBITMQ_USER no tiene tags y no puede entrar a esa API:
# se usa el usuario de monitoreo (solo lectura) y el prefijo /rabbitmq (management.path_prefix, detrás de Caddy).
RABBITMQ_MGMT_USER = os.getenv("RABBITMQ_MGMT_USER") or RABBITMQ_USER
RABBITMQ_MGMT_PASS = os.getenv("RABBITMQ_MGMT_PASS") or RABBITMQ_PASS
RABBITMQ_MGMT_PATH_PREFIX = os.getenv("RABBITMQ_MGMT_PATH_PREFIX", "").strip().rstrip("/")

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
    if process == "api":
        weak = weak_secrets(os.environ)
        if weak:
            sys.exit(f"[api] Secretos inseguros en .env:\n  - " + "\n  - ".join(weak) +
                     "\n  Genera uno aleatorio con: python -c \"import secrets; print(secrets.token_hex(32))\"")


# Largo mínimo y valores de ejemplo que nunca deben llegar a un servidor público
MIN_SECRET_KEY = 32
MIN_ADMIN_PASSWORD = 10
_PLACEHOLDERS = ("cambia", "change", "example", "ejemplo", "secret", "password", "contrasena", "contraseña",
                 "admin", "123456", "qwerty", "tu_clave")


def weak_secrets(env) -> list[str]:
    """Describe los problemas de SECRET_KEY y ADMIN_PASSWORD. Solo nombra la variable: nunca muestra su valor."""
    problems = []
    for name, minimum in (("SECRET_KEY", MIN_SECRET_KEY), ("ADMIN_PASSWORD", MIN_ADMIN_PASSWORD)):
        value = env.get(name, "").strip()
        if any(p in value.lower() for p in _PLACEHOLDERS):
            problems.append(f"{name} parece un valor de ejemplo (contiene una palabra como 'cambia', 'admin' o 'password')")
        elif len(value) < minimum:
            problems.append(f"{name} es muy corta: mínimo {minimum} caracteres")
    return problems

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

# URL con la que el público entra a la tienda desde sus celulares (QR del panel y enlaces de los correos).
# PUBLIC_BASE_URL (en .env, p. ej. la IP del servidor en la nube) tiene prioridad; si no está, se usa PUBLIC_URL,
# que iniciar.ps1 rellena con la IP de la laptop en la red Wi-Fi.
def public_url(env=os.environ) -> str:
    return (env.get("PUBLIC_BASE_URL", "").strip() or env.get("PUBLIC_URL", "").strip()).rstrip("/")


PUBLIC_URL = public_url()


# Con la tienda servida por HTTPS (Caddy en el servidor) la cookie de sesión lleva Secure: el navegador nunca la envía
# por HTTP sin cifrar. En la laptop (http://...) va sin Secure, o el navegador no la guardaría.
def cookie_secure(env=os.environ) -> bool:
    return env.get("PUBLIC_BASE_URL", "").strip().lower().startswith("https://")


COOKIE_SECURE = cookie_secure()

# Fuerza bruta en el login: como mucho LOGIN_MAX_FAILS intentos fallidos por IP en LOGIN_WINDOW_S segundos
LOGIN_MAX_FAILS = int(os.getenv("LOGIN_MAX_FAILS", "5"))
LOGIN_WINDOW_S = int(os.getenv("LOGIN_WINDOW_S", "60"))


# --- Zona Yape real: productos baratos que se pagan con Yape de verdad -----------------------------------------------
# La app MacroDroid del celular del dueño reenvía cada notificación de Yape al webhook POST /api/pagos/yape/notificacion
# con la cabecera X-Yape-Secret. Sin un secreto válido (32+ caracteres, sin palabras de ejemplo) la zona se desactiva
# sola y el resto de la tienda sigue igual.
MIN_YAPE_SECRET = 32
YAPE_WEBHOOK_PER_MIN = 60          # peticiones por minuto al webhook (todas, válidas o no)
YAPE_EXPIRE_EVERY_S = 5            # cada cuánto el relay vence los pedidos Yape sin pagar


def _money_env(env, name: str, default: str):
    from decimal import Decimal, InvalidOperation
    raw = env.get(name, "").strip() or default
    try:
        value = Decimal(raw.replace(",", "."))
    except InvalidOperation:
        return None
    return value.quantize(Decimal("0.01")) if value.is_finite() else None


def yape_settings(env=os.environ) -> dict:
    """Configuración de la Zona Yape real. problema: por qué está desactivada (None si está activa). Nunca incluye el
    valor del secreto, solo su nombre."""
    secret = env.get("YAPE_WEBHOOK_SECRET", "").strip()
    base = _money_env(env, "YAPE_PRECIO_BASE", "1.00")
    lo = _money_env(env, "YAPE_MONTO_MIN", "1.01")
    hi = _money_env(env, "YAPE_MONTO_MAX", "1.99")
    try:
        minutes = int(env.get("YAPE_MINUTOS_EXPIRA", "").strip() or "10")
    except ValueError:
        minutes = 0
    problem = None
    if not secret:
        problem = "Falta YAPE_WEBHOOK_SECRET en el .env"
    elif len(secret) < MIN_YAPE_SECRET:
        problem = f"YAPE_WEBHOOK_SECRET es muy corto: mínimo {MIN_YAPE_SECRET} caracteres"
    elif any(p in secret.lower() for p in _PLACEHOLDERS):
        problem = "YAPE_WEBHOOK_SECRET parece un valor de ejemplo"
    elif base is None or base <= 0:
        problem = "YAPE_PRECIO_BASE no es un monto válido"
    elif lo is None or hi is None or lo <= 0 or hi < lo:
        problem = "YAPE_MONTO_MIN / YAPE_MONTO_MAX no forman un rango válido"
    elif (hi - lo) * 100 > 5000:
        problem = "El rango YAPE_MONTO_MIN..YAPE_MONTO_MAX tiene más de 5000 montos: redúcelo"
    elif minutes < 1 or minutes > 120:
        problem = "YAPE_MINUTOS_EXPIRA debe estar entre 1 y 120"
    return {"enabled": problem is None, "problem": problem, "secret": secret,
            "titular": env.get("YAPE_TITULAR", "").strip() or "Titular no configurado",
            "precio_base": base, "monto_min": lo, "monto_max": hi, "minutos": minutes}


YAPE = yape_settings()

# Cola de pagos: el mismo esquema que pedidos y correos (durable, reintentos con TTL y DLQ), en su propio exchange
PAGOS_EXCHANGE = "tienda.pagos"
PAGOS_QUEUE = "pagos"
PAGOS_DLQ = "pagos.dlq"
PAGOS_ROUTING_KEY = "pago"
PAGOS_RETRY_QUEUES = tuple(f"pagos.retry.{d}s" for d in RETRY_DELAYS)
