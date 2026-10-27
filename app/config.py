import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_MGMT_PORT = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://tienda:tienda@localhost:5432/tienda")
# Archivo compartido (volumen) con el que la API controla al worker en caliente
CONTROL_PATH = os.getenv("CONTROL_PATH", "data/control.json")

# Seguridad: firma de sesiones y cuenta de administrador (se crea/actualiza al arrancar la API)
SECRET_KEY = os.getenv("SECRET_KEY", "cambia-esta-clave-en-produccion")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@tienda.com")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Admin2025!")
SESSION_DAYS = 7

# Topología de RabbitMQ
EXCHANGE = "tienda.orders"
DLX = "tienda.orders.dlx"
QUEUE = "orders.created"
DLQ = "orders.created.dlq"
ROUTING_KEY = "order.created"

# Modo "sin persistencia" (para comparar): exchange y cola que viven solo en memoria
VOLATILE_EXCHANGE = "tienda.orders.volatil"
VOLATILE_QUEUE = "orders.created.volatil"

# Reintentos con espera creciente: cada intento fallido espera en una cola con TTL y luego vuelve a la principal
RETRY_DELAYS = (2, 5, 10)                       # segundos de espera antes del 1er, 2º y 3er reintento
RETRY_QUEUES = tuple(f"orders.retry.{d}s" for d in RETRY_DELAYS)
MAX_RETRIES = len(RETRY_DELAYS)

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
