# Black Friday: persistencia de datos en colas de mensajería con RabbitMQ

Simulación de una tienda online en un día de alto tráfico. Un panel interactivo muestra en vivo cómo una cola de
mensajes absorbe los picos y cómo **no se pierde ningún pedido** aunque el sistema falle (hay botones de autosabotaje).

## Ejecutar (solo requiere Docker Desktop encendido)

El día de la presentación, en PowerShell dentro de la carpeta del proyecto:
```powershell
powershell -ExecutionPolicy Bypass -File .\iniciar.ps1
```
El script detecta la IP de la laptop en la red Wi-Fi, levanta los contenedores y abre el panel. El QR del panel
apunta a la tienda, para que el público compre desde su celular (deben estar en la misma red Wi-Fi).

Sin el script también funciona (`docker compose up --build`), pero el QR apuntará a `localhost`.

- **Tienda (público): http://localhost:8000** o `http://<IP-de-la-laptop>:8000` desde los celulares
- **Panel de control (proyector): http://localhost:8000/panel** — solo administrador
- Panel de RabbitMQ: http://localhost:15672 (guest / guest)
- Documentación de la API: http://localhost:8000/docs
- PostgreSQL: `localhost:5432`, base `tienda`, usuario `tienda`, contraseña `tienda` (para mostrar las tablas con DBeaver o pgAdmin)
- Apagar: `docker compose down` (los datos se conservan). Empezar de cero: `docker compose down -v`.

## Usuarios y administrador

- Los clientes se registran desde la tienda (botón **Ingresar → Crear cuenta**). Para pagar hay que tener sesión.
- La cuenta de **administrador** se crea sola al arrancar y es la **única** que puede abrir `/panel`, ver las métricas
  y usar el simulador y los sabotajes (la API responde 401/403 a cualquier otro usuario):
  - Correo: `admin@tienda.com`
  - Contraseña: `Admin2025!`
- Para cambiarlas, define `ADMIN_EMAIL` y `ADMIN_PASSWORD` antes de levantar (se aplican en cada arranque), por ejemplo:
  `$env:ADMIN_PASSWORD="OtraClave123"; docker compose up -d`. Cambia también `SECRET_KEY`, que firma las sesiones.
- Las contraseñas se guardan con hash PBKDF2-SHA256 y sal aleatoria; la sesión es una cookie `HttpOnly` firmada con
  HMAC, así que no se puede editar para darse permisos de administrador.

## Arquitectura en capas

```
app/
├── presentation/    API REST + tienda, login y panel (FastAPI, static/*.html)
├── application/     OrderService, CheckoutService, AuthService, simulador, relay, puertos (interfaces)
├── domain/          Order, Product, User (reglas de negocio, sin dependencias)
├── infrastructure/
│   ├── persistence/ PostgresOrderRepository, PostgresUserRepository, catálogo (JSON)
│   ├── security.py  hash de contraseñas y firma de sesiones
│   ├── messaging/   RabbitMQ: publisher, consumer, monitor de colas
│   ├── control.py   configuración en caliente (nº de workers, velocidad)
│   └── docker_control.py  sabotajes reales vía socket de Docker
└── worker.py        proceso consumidor (escalable en caliente con hilos)
```
Cada capa solo depende de las inferiores y la aplicación usa interfaces (`ports.py`). Prueba de ello: el proyecto
empezó con SQLite y se pasó a PostgreSQL cambiando solo la capa de infraestructura. El adaptador de SQLite se conserva
(`sqlite_order_repository.py`) y lo usan las pruebas unitarias, que corren sin levantar ningún servidor.

Contenedores: `api` (API + tienda + panel + simulador), `worker` (consumidores), `rabbitmq` (broker) y `postgres` (BD).

## Mecanismos de persistencia demostrados

| Mecanismo | Dónde | Qué evita |
|---|---|---|
| Exchange y colas `durable` | `declare_topology` | perder la cola si RabbitMQ se reinicia |
| Mensajes `delivery_mode=Persistent` | `RabbitOrderPublisher` | perder mensajes en memoria al caer el broker |
| Publisher confirms + `mandatory` | `RabbitOrderPublisher` | creer que se envió algo que el broker no recibió |
| Volumen Docker `rabbit_data` | `docker-compose.yml` | perder datos al recrear el contenedor |
| **Transactional Outbox**: pedido + evento en la misma transacción | `PostgresOrderRepository.save` | un pedido guardado sin su mensaje (o un mensaje sin pedido) |
| **Relay del outbox** con `FOR UPDATE SKIP LOCKED` | `PostgresOutbox.relay`, `OutboxRelay` | perder eventos si el broker está caído; que dos relays envíen el mismo evento |
| **Reconciliación** de mensajes perdidos | `OrderService.reconcile_lost` | que un mensaje que el broker olvidó deje el pedido sin procesar para siempre |
| ACK manual tras procesar | `OrderConsumer` | perder un mensaje si el worker muere a mitad |
| **Reintentos con espera creciente** (2 s, 5 s, 10 s) en colas con TTL | `declare_topology`, `OrderConsumer` | reintentar al instante algo que va a volver a fallar |
| **Dead Letter Queue** + inspector (reprocesar / descartar) | `DeadLetterInspector` | que un mensaje "venenoso" bloquee la cola o se pierda sin rastro |
| Idempotencia en `process_order` | `OrderService` | procesar dos veces un mensaje duplicado (RabbitMQ garantiza "al menos una vez") |
| Reconexión automática | `OrderConsumer` | que los workers queden muertos tras una caída del broker |
| PostgreSQL con WAL + volumen `pg_data` | `docker-compose.yml` | perder pedidos o usuarios si la BD cae de golpe |
| Errores de infraestructura vuelven a la cola (`nack` + requeue) | `OrderConsumer` | que una caída de la BD mande pedidos sanos a la DLQ |
| Pool de conexiones con verificación | `create_pool` | que la API y los workers queden rotos tras reiniciar la BD |

## Laboratorio: ¿cuánto vale la persistencia?

- **Interruptor "Apagar persistencia"** (panel, sección Autosabotaje): los pedidos nuevos viajan por un exchange y una
  cola **no durables** con mensajes **transitorios** (solo en memoria). Si en ese modo se tira RabbitMQ, los mensajes
  se pierden de verdad: el KPI **Mensajes perdidos** lo muestra y el botón **Reconciliar desde la BD** los recupera
  gracias a que PostgreSQL conserva los pedidos.
- **Experimento automático**: publica 500 mensajes con persistencia y 500 sin ella (en colas de laboratorio que no
  tocan los pedidos reales), mide la velocidad de cada modo, mata RabbitMQ y cuenta los sobrevivientes. Resultado de
  referencia en esta laptop:

  | Modo | Velocidad al publicar | Sobrevivieron | Perdidos |
  |---|---|---|---|
  | Con persistencia | 369 msg/s | 500 | 0 |
  | Sin persistencia | 1 532 msg/s | 0 | 500 |

  Conclusión: escribir en disco hace la publicación ~76% más lenta, pero es lo que evita perder los mensajes.

## Guion para la presentación

1. Pulsa **Iniciar Black Friday**: en 2,5 min pasa un día completo con picos a medianoche, al mediodía y a la noche.
   El tráfico (puntos azules) entra, la cola (barril central) se llena y los workers la drenan.
2. **Pico de tráfico**: con ritmo alto la cola crece. Sube con **＋ workers** y mira cómo se vacía (escalado horizontal).
3. **Tirar RabbitMQ**: el broker muere de golpe. Los clientes siguen comprando y los eventos se acumulan en el
   **outbox** de PostgreSQL. Pulsa **Recuperar todo**: el relay los publica solo y la cola recupera sus mensajes desde disco.
4. **Matar al worker**: los mensajes esperan en la cola (en disco). Al recuperar, se procesan todos.
5. **Pedidos venenosos**: se reintentan esperando 2 s, 5 s y 10 s; al tercer fallo van a la DLQ sin bloquear a los
   demás. En **Mensajes fallidos (DLQ)** se ve cada uno con su error e intentos, y se puede reprocesar o descartar.
6. **Workers lentos**: la cola absorbe la diferencia entre lo que entra y lo que sale.
7. **Tirar PostgreSQL**: la base de datos muere en plena venta. La tienda responde "alta demanda, reintenta"
   (no acepta pedidos que no puede guardar), y los mensajes que ya estaban en la cola **no** se pierden ni van a la DLQ:
   los workers los devuelven a la cola. Al **Recuperar todo**, los pedidos y usuarios siguen ahí y la cola se procesa.

8. **Apagar persistencia** + **Tirar RabbitMQ**: esta vez sí se pierden mensajes (KPI en rojo). **Reconciliar desde la BD**
   los recupera. Luego **Ejecutar experimento** para mostrar la comparación con números.

Al final los **Procesados** deben igualar a los **Recibidos** (menos los fallidos a propósito): cero pérdidas.

## Pruebas

```bash
docker compose run --rm --no-deps api pytest -q
```

## La tienda

- Catálogo de 100 productos (celulares, televisores, cómputo, audio, electrohogar, cocina y hogar) con precios en soles.
  Los datos están en `app/infrastructure/persistence/catalogo.json`: nombre, precios, descripción, stock y
  productos relacionados (cross-selling). Se puede editar a mano; las imágenes están en `static/img/<sku>.jpg`.
- **Cross-selling**: "Frecuentemente comprados juntos", "Quienes vieron esto también compraron", combos,
  sugerencias al agregar al carrito y "Completa tu compra" dentro del carrito, además de una barra de envío gratis.
- Los precios se validan en el servidor (`CheckoutService`): el navegador solo envía SKU y cantidad.
- Para pagar hay que iniciar sesión; si no la tiene, la tienda lo lleva al registro y luego vuelve al pago con su carrito.
- "Mi cuenta" muestra los pedidos del cliente leídos desde PostgreSQL, con su estado en vivo.
- Tras pagar, el cliente ve **su pedido viajar por la cola en vivo**: guardado en BD, en la cola con su posición,
  procesándose y confirmado. Si RabbitMQ está caído, se le avisa que su pedido está guardado y no se perderá.
- Para cambiar el nombre de la empresa: constante `STORE_NAME` en `static/tienda.html`.

## Si los celulares no cargan la tienda

1. Comprueba que estén en la misma red Wi-Fi que la laptop (redes de invitados suelen aislar dispositivos).
2. Abre el puerto 8000 en el Firewall de Windows (PowerShell como administrador):
   `New-NetFirewallRule -DisplayName "Demo tienda 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow`
3. Plan B: comparte internet desde el celular del presentador y conecta la laptop a ese punto de acceso.

## Notas

- Los botones de sabotaje real necesitan que Docker esté disponible para la API: `docker-compose.yml` monta
  `/var/run/docker.sock` en el contenedor `api`. Es solo para la demo; no se haría en producción.
- "Reiniciar demo" (enlace pequeño bajo los botones) vacía la BD y las colas para repetir la presentación.
