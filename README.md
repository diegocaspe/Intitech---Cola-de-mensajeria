# Black Friday: persistencia de datos en colas de mensajería con RabbitMQ

Simulación de una tienda online en un día de alto tráfico. Un panel interactivo muestra en vivo cómo una cola de
mensajes absorbe los picos y cómo **no se pierde ningún pedido** aunque el sistema falle (hay botones de autosabotaje).

## Configuración

Las contraseñas y claves no están en el código: se leen de un archivo `.env` que **no se sube a git**.

1. Copia la plantilla: `cp .env.example .env` (en PowerShell: `Copy-Item .env.example .env`).
2. Abre `.env` y reemplaza todos los valores de ejemplo. Para generar claves aleatorias:
   `python -c "import secrets; print(secrets.token_hex(32))"`
3. Levanta todo: `docker compose up --build` (o `iniciar.ps1`, ver abajo).

Si falta una variable obligatoria, `docker compose` o el propio servicio se detienen diciendo cuál falta.

**Correos (opcional).** Con `EMAIL_ENABLED=true` el contenedor `mailer` envía correos reales por Gmail; con `false`
solo escribe "correo simulado a ..." en su log. Para el envío real:

1. Activa la verificación en 2 pasos de la cuenta de Google que enviará los correos.
2. Crea una contraseña de aplicación en https://myaccount.google.com/apppasswords (16 letras).
3. En `.env`: `GMAIL_USER=<esa cuenta>`, `GMAIL_APP_PASSWORD=<las 16 letras>` y `EMAIL_ENABLED=true`.
4. Reinicia el mailer: `docker compose up -d mailer`.

Los enlaces de los correos y el QR del panel usan `PUBLIC_BASE_URL` si está en el `.env` (en el servidor:
`https://3-214-91-208.sslip.io`; si empieza con `https://`, la cookie de sesión lleva además `Secure`). Si no, usan `PUBLIC_URL`, que pone `iniciar.ps1` con la IP de la laptop; sin ninguna de las
dos apuntan a `http://localhost:8000`, que solo abre en la misma laptop.

La API no arranca si `SECRET_KEY` tiene menos de 32 caracteres, si `ADMIN_PASSWORD` tiene menos de 10 o si alguna
sigue siendo un valor de ejemplo (contiene "cambia", "admin", "password", etc.). El error nombra la variable, nunca
su valor.

> PostgreSQL y RabbitMQ solo leen sus credenciales al crear sus volúmenes. Si cambias `POSTGRES_PASSWORD`,
> `RABBITMQ_USER` o `RABBITMQ_PASS` con los volúmenes ya creados, empieza de cero con `docker compose down -v`
> (borra los datos) o aplica el cambio dentro de cada servicio (`ALTER ROLE ... PASSWORD` / `rabbitmqctl`).
> En el servidor, las claves de RabbitMQ sí se reaplican solas en cada arranque (`deploy/rabbitmq/arranque.sh`).

## Ejecutar (solo requiere Docker Desktop encendido y el `.env` configurado)

**Presentación con el servidor en la nube (recomendado):** el público entra a **https://3-214-91-208.sslip.io**
desde su celular con datos móviles, sin estar en la misma Wi-Fi que la laptop; el QR del panel ya apunta ahí. Es
HTTPS con certificado válido, así que Chrome y Brave (que fuerzan HTTPS) la abren sin avisos. Quien escriba
`http://3.214.91.208` es redirigido solo. El panel se abre en **https://3-214-91-208.sslip.io/panel** con la cuenta de
administrador, y las colas de RabbitMQ (solo lectura) en **https://3-214-91-208.sslip.io/rabbitmq/** o con el botón
**Ver colas en RabbitMQ** del panel. Ver [Despliegue en AWS EC2](#despliegue-en-aws-ec2).

**Presentación en local (plan B, sin internet):** en PowerShell dentro de la carpeta del proyecto:
```powershell
powershell -ExecutionPolicy Bypass -File .\iniciar.ps1
```
El script detecta la IP de la laptop en la red Wi-Fi, levanta los contenedores y abre el panel. El QR del panel
apunta a la tienda, para que el público compre desde su celular (deben estar en la misma red Wi-Fi).

Sin el script también funciona (`docker compose up --build`), pero el QR apuntará a `localhost`.

- **Tienda (público): http://localhost:8000** o `http://<IP-de-la-laptop>:8000` desde los celulares
- **Panel de control (proyector): http://localhost:8000/panel** — solo administrador
- Panel de RabbitMQ: http://localhost:15672 (`RABBITMQ_USER` / `RABBITMQ_PASS` del `.env`), también con el botón
  **Ver colas en RabbitMQ** del panel
- Documentación de la API: http://localhost:8000/docs
- PostgreSQL: `localhost:5433`, con `POSTGRES_DB`, `POSTGRES_USER` y `POSTGRES_PASSWORD` del `.env` (para mostrar las tablas con DBeaver o pgAdmin)
- Apagar: `docker compose down` (los datos se conservan). Empezar de cero: `docker compose down -v`.

## Usuarios y administrador

- Los clientes se registran desde la tienda (botón **Ingresar → Crear cuenta**). Para pagar hay que tener sesión.
- La cuenta de **administrador** se crea sola al arrancar y es la **única** que puede abrir `/panel`, ver las métricas
  y usar el simulador y los sabotajes (la API responde 401/403 a cualquier otro usuario):
  correo y contraseña son `ADMIN_EMAIL` y `ADMIN_PASSWORD` del `.env`.
- Para cambiarlas, edita el `.env` y reinicia la API (`docker compose up -d`): se aplican en cada arranque.
  `SECRET_KEY` firma las sesiones; si la cambias, todos tienen que volver a iniciar sesión.
- Las contraseñas se guardan con hash PBKDF2-SHA256 y sal aleatoria; la sesión es una cookie `HttpOnly` firmada con
  HMAC, así que no se puede editar para darse permisos de administrador.
- **Verificación del correo**: al registrarse, el cliente recibe un enlace (vence en 24 h, un solo uso). Sin verificar
  puede ingresar y comprar igual; "Mi cuenta" le muestra un aviso con la opción de reenviar el enlace.
- **Sin límite de intentos de login**: en la expo muchas personas comparten la misma IP pública (Wi-Fi de la
  universidad), así que equivocarse de contraseña nunca bloquea a nadie; la API solo responde 401.
- **¿Olvidaste tu contraseña?** (en `/login`): envía un enlace para crear una contraseña nueva (vence en 60 min, un
  solo uso, como mucho uno por minuto). La respuesta es la misma exista o no la cuenta, para no revelar qué correos
  están registrados. De los tokens solo se guarda el hash SHA-256.

## Arquitectura en capas

```
app/
├── routes/          Rutas: los endpoints de la API (FastAPI). Reciben la petición y llaman a un service
├── services/        Servicios: la lógica de negocio (OrderService, CheckoutService, AuthService, simulador,
│                    NotificationService, las plantillas de los correos y payments.py, el proveedor de pagos
│                    simulado)
├── models/          Modelos: Order (con su PaymentStatus), Product, User, EmailEvent y sus reglas (sin
│                    dependencias de tecnología)
├── repositories/    Repositorios: acceso a los datos
│   ├── interfaces.py          contratos que usan los services (OrderRepository, ProductRepository, OrderPublisher...)
│   ├── postgres_repository.py implementación con PostgreSQL (incluye PostgresProductRepository: tabla products)
│   ├── sqlite_repository.py   implementación con SQLite (la usan las pruebas; incluye SqliteProductRepository)
│   └── catalog_repository.py  lectura de catalogo.json (semilla, pruebas y nombres en los correos)
├── database/        Base de datos: conexión (pool), creación de tablas, seed.py y catalogo.json (solo semilla)
├── messaging/       Mensajería con RabbitMQ: publisher, consumers (pedidos y correos), monitor de colas,
│                    y los EmailSender (Gmail por SMTP, solo log y falso para pruebas)
├── utils/           Utilidades: seguridad (contraseñas y sesiones), límite de intentos de login, configuración en
│                    caliente, sabotajes vía Docker
├── static/          Páginas web: tienda, login y panel (HTML, CSS, imágenes)
├── config.py        Variables de configuración
├── relay.py         Proceso que lee el outbox y publica en RabbitMQ con publisher confirms (pedidos y correos)
├── worker.py        Proceso que consume la cola de pedidos (escalable en caliente con hilos): cobra y descuenta stock
└── mailer.py        Proceso que consume la cola de correos y los envía por Gmail
```
Flujo de una petición: **routes → services → models → repositories → database**. Los services solo conocen las
interfaces de `repositories/interfaces.py`, no PostgreSQL ni RabbitMQ. Prueba de ello: el proyecto empezó con SQLite
y se pasó a PostgreSQL cambiando solo el repositorio. El de SQLite se conserva (`sqlite_repository.py`) y lo usan las
pruebas unitarias, que corren sin levantar ningún servidor.

Fuera de `app/`: `deploy/` (scripts para subir al servidor de AWS), `docker-compose.yml` (local) y
`docker-compose.prod.yml` (lo que el servidor agrega encima).

Contenedores: `api` (API + tienda + panel + simulador), `relay` (outbox → RabbitMQ), `worker` (consumidores de pedidos:
cobran con el proveedor simulado y descuentan stock), `mailer` (consumidor de correos), `rabbitmq` (broker) y
`postgres` (BD).

Un pedido nuevo nunca va directo a la cola: la API solo escribe `orders` + `outbox` en una transacción y el relay es quien publica.

## Stock real: tabla `products`

El catálogo y su stock viven en PostgreSQL. `app/database/catalogo.json` es **solo la semilla**: al arrancar, si la
tabla `products` está vacía, se llena desde ese archivo (`seed.py`, con `INSERT ... ON CONFLICT (sku) DO NOTHING`).
api, relay, worker y mailer crean el esquema y siembran al arrancar; un candado de PostgreSQL
(`pg_advisory_xact_lock`) los pone en fila dentro de la misma transacción, así nunca siembran dos a la vez.

| Tabla | Columnas principales | Para qué |
|---|---|---|
| `products` | `sku` (PK), `name`, `category` (con índice), `price` y `list_price` `NUMERIC(10,2)`, `rating`, `reviews`, `stock INTEGER CHECK (stock >= 0)`, `image`, `description`, `related` (JSONB), `updated_at` | el catálogo con el stock de verdad |
| `orders` | … + `stock_descontado` (BOOLEAN) | marca que el stock de ese pedido ya se restó (lo resta una sola vez) |
| `orders` | … + `payment_token`, `card_brand`, `card_last4`, `payment_status`, `payment_reason`, `payment_failures` | el pago con tarjeta simulado (ver [Pago con tarjeta](#pago-con-tarjeta-simulado-y-asíncrono)); nunca el número completo ni el CVV |

**Flujo del stock**

```
tienda ──checkout──> orders + outbox (el stock aún NO se toca: el pedido solo espera en la cola)
worker ──¿alcanza? (solo mira)──> no: REJECTED sin cobrar
       ──cobra la tarjeta (simulado)──> rechazada: REJECTED, el stock no se toca
       ──una transacción──> descuenta cada ítem (en orden de SKU) ─┬─ todos alcanzan: PROCESSED + comprobante en el outbox
                                                                   └─ uno no alcanza: se deshace todo → REJECTED + REFUNDED (ack, sin DLQ)
```

**El query del descuento, en español simple**

```sql
UPDATE products SET stock = stock - :qty, updated_at = now()
WHERE sku = :sku AND stock >= :qty
```

"Réstale `qty` unidades a este producto, **pero solo si tiene al menos `qty`**". La comprobación y la resta son una
sola operación: no hay un momento entre "miro cuántas quedan" y "las resto" en el que otro worker pueda colarse. Si
dos workers quieren la última unidad a la vez, PostgreSQL bloquea la fila: el segundo espera, vuelve a mirar el stock
(ya en 0) y su `UPDATE` no cambia ninguna fila. "0 filas afectadas" significa "no alcanza": el worker deshace los
descuentos que ya hizo en ese pedido (punto de guardado) y lo marca `REJECTED`. Además, el `CHECK (stock >= 0)` hace
que la BD rechace cualquier stock negativo aunque el código se equivocara.

- **Todo o nada**: los ítems de un pedido se descuentan en la misma transacción que `mark_processed` y el comprobante.
  Si falta uno, no se descuenta ninguno.
- **Sin bloqueos cruzados**: cada worker descuenta los ítems ordenados por SKU, así dos pedidos con los mismos
  productos nunca se esperan mutuamente (deadlock).
- **Duplicados**: el worker bloquea la fila del pedido (`FOR UPDATE`) y solo descuenta si aún no tiene resultado final;
  un mensaje repetido de RabbitMQ no resta dos veces.
- **Cobro antes del descuento**: el worker mira el stock (sin restar), cobra y recién entonces descuenta. Entre la mirada y
  el descuento otro pedido puede llevarse lo último: el `UPDATE` no alcanza, el pedido queda `REJECTED` y su pago pasa a
  `REFUNDED` en esa misma transacción (reembolso simulado).
- `SIN-STOCK` y `FAIL` siguen siendo SKU de prueba de los escenarios: `SIN-STOCK` se rechaza siempre; `FAIL` no es un
  producto y no descuenta nada. Cualquier otro SKU que no esté en `products` se rechaza (no se vende lo que no existe).
- **Panel → Stock por producto** (solo admin): unidades en stock, vendidas y agotados, la lista con los menos
  disponibles primero y el botón **Restablecer stock** (vuelve a los valores de `catalogo.json`; no toca pedidos ni
  colas). **Reiniciar demo** también restablece el stock.

## Correos automáticos

Los correos usan **los mismos mecanismos que los pedidos**. Ningún route ni service envía correos: cada service deja
un evento `email.*` en el outbox, en la misma transacción que su operación. El relay lo publica en la cola durable
`correos` y el contenedor `mailer` lo envía.

```
service ──(misma transacción)──> outbox ──relay──> cola "correos" ──mailer──> Gmail (SMTP_SSL :465)
                                                     │  falla SMTP/red: correos.retry.2s → 5s → 10s
                                                     └─ 3er fallo: correos.dlq (se ve y reprocesa en el panel)
```

| Correo | Lo pide | En la misma transacción que |
|---|---|---|
| `email.registro`: bienvenida + enlace de verificación | `AuthService.register` | crear el usuario y su token |
| `email.login`: "Nuevo inicio de sesión" con fecha y hora de Lima | `AuthService.authenticate` | (no escribe nada más) |
| `email.compra_confirmada`: comprobante con código, productos, cantidades, precios, total, fecha y hora | `OrderService.process_order` | marcar el pedido como confirmado |
| `email.recuperar`: enlace para crear una contraseña nueva | `AuthService.request_password_reset` | crear el token |

Solo reciben correos los **clientes registrados desde la tienda**: nunca el administrador ni los compradores
inventados por el simulador (no tienen cuenta). Las cuentas que ya existían antes de los correos se marcaron como
`legacy` (eran de prueba) y tampoco reciben.

Cada evento lleva un `email_id` único. El mailer lo guarda en `sent_emails` al enviar y, si RabbitMQ le entrega el
mismo mensaje otra vez, no lo repite. Límite honesto: si el mailer muriera justo entre "Gmail aceptó el correo" y
"guardé el email_id", ese correo llegaría dos veces. Gmail no participa de la transacción de la base de datos, así
que "al menos una vez" no se puede convertir en "exactamente una vez".

## Pago con tarjeta (simulado y asíncrono)

No hay pasarela real: un proveedor de pagos **simulado** vive en el código (`app/services/payments.py`). Aun así el
cobro sigue las reglas de uno de verdad, y lo hace el **worker**, no la API: así el worker verifica de verdad
"pago y stock".

```
tienda ──checkout + tarjeta──> API: valida y cambia la tarjeta por un token ─> orders + outbox ─> 201 al instante
worker: ¿hay stock? (solo mira) ─ no ─> REJECTED «sin stock», sin cobrar
          │ sí
          └─> cobra (FakePaymentProvider, clave de idempotencia = order_id, 300-800 ms)
                 ├─ rechazada ──────────────> payment_status DECLINED, REJECTED «pago rechazado» (ack, sin reintentos ni DLQ)
                 ├─ error temporal ─────────> ProcessingError: reintento a los 2 s / 5 s / 10 s (las colas de siempre)
                 └─ aprobada ─> APPROVED ─> descuenta stock + PROCESSED + comprobante (UNA transacción)
                                               └─ el stock ya no alcanza (otro pedido se adelantó) ─> REJECTED + REFUNDED
```

**Tarjetas de prueba** (las únicas que se aceptan; el modal de la tienda tiene botones que rellenan el formulario con un
vencimiento futuro y un CVV de 3 dígitos):

| Número | Token | Qué pasa |
|---|---|---|
| `4111 1111 1111 1111` | `tok_test_approved` | aprobada |
| `4000 0000 0000 0002` | `tok_test_declined` | rechazada (fondos insuficientes): no se descuenta stock |
| `4000 0000 0000 0119` | `tok_test_flaky` | error temporal del proveedor: falla el 1.er intento y se aprueba en el reintento (2 s) |

- **Solo tarjetas de prueba**: cualquier otro número responde **400 «Usa una tarjeta de prueba»**, así nadie del público
  escribe su tarjeta real. Los campos tienen `autocomplete="off"` para que el navegador no ofrezca tarjetas guardadas.
- **Nunca se guarda ni se registra el número completo ni el CVV**: `payments.tokenize()` es el único lugar que los ve. En
  `orders` quedan `payment_token`, `card_brand`, `card_last4`, `payment_status` (`PENDING`, `APPROVED`, `DECLINED`,
  `REFUNDED`), `payment_reason` y `payment_failures`. El mensaje de RabbitMQ sigue llevando solo el `order_id`. Los
  errores 400 nunca repiten lo que se escribió, y los 422 de la API se devuelven sin el campo `input` de FastAPI.
- **Asíncrono**: `POST /api/checkout` responde 201 al instante. El seguimiento del pedido (paso 3) dice «Cobrando tu
  tarjeta y verificando stock» y, si sale `REJECTED`, distingue «Pago rechazado: no se descontó stock» de «Sin stock: no
  se te cobró nada» (o, si se cobró y luego se acabó el stock, «te devolvimos el pago»).
- **Un solo cobro por pedido**: si el pedido ya tiene `payment_status = APPROVED` (RabbitMQ entregó el mensaje dos veces,
  o el worker murió entre el cobro y el descuento), no se vuelve a cobrar. Además el proveedor recuerda cada `order_id`:
  dos workers con el mismo mensaje a la vez reciben el mismo resultado.
- **Error temporal**: los fallos se cuentan en `payment_failures` (en la BD), así el reintento sabe que ya falló aunque lo
  tome otro worker o el worker se haya reiniciado.
- **Latencia**: las compras de la tienda esperan 300-800 ms al proveedor (`PAYMENT_LATENCY_MIN` / `PAYMENT_LATENCY_MAX`).
  Los pedidos automáticos (simulador, los 6 escenarios y `POST /orders`) pagan solos con `tok_test_approved` y casi sin
  espera (0-50 ms), para que una ráfaga de 500 no tarde minutos. El sabotaje «worker lento» se suma, como antes.
- El comprobante por correo incluye «Pagado con: Visa •••• 1111 · Pago aprobado».
- **Panel → Pagos con tarjeta**: aprobados, rechazados y reembolsados (también en `/api/metrics` → `payments`).


## Mecanismos de persistencia demostrados

| Mecanismo | Dónde | Qué evita |
|---|---|---|
| Exchange y colas `durable` | `declare_topology` | perder la cola si RabbitMQ se reinicia |
| Mensajes `delivery_mode=Persistent` | `RabbitOrderPublisher` | perder mensajes en memoria al caer el broker |
| Publisher confirms + `mandatory` | `RabbitOrderPublisher` | creer que se envió algo que el broker no recibió |
| Volumen Docker `rabbit_data` | `docker-compose.yml` | perder datos al recrear el contenedor |
| **Transactional Outbox**: pedido + evento en la misma transacción | `PostgresOrderRepository.save` | un pedido guardado sin su mensaje (o un mensaje sin pedido) |
| **Relay del outbox** con `FOR UPDATE SKIP LOCKED` | `PostgresOutbox.relay`, `app/relay.py` | perder eventos si el broker o el relay están caídos; que dos relays envíen el mismo evento |
| **Reconciliación** de mensajes perdidos | `OrderService.reconcile_lost` | que un mensaje que el broker olvidó deje el pedido sin procesar para siempre |
| ACK manual tras procesar | `OrderConsumer` | perder un mensaje si el worker muere a mitad |
| **Reintentos con espera creciente** (2 s, 5 s, 10 s) en colas con TTL | `declare_topology`, `OrderConsumer` | reintentar al instante algo que va a volver a fallar |
| **Dead Letter Queue** + inspector (reprocesar / descartar) | `DeadLetterInspector` | que un mensaje "venenoso" bloquee la cola o se pierda sin rastro |
| Idempotencia en `process_order` | `OrderService` | procesar dos veces un mensaje duplicado (RabbitMQ garantiza "al menos una vez") |
| Reconexión automática | `OrderConsumer` | que los workers queden muertos tras una caída del broker |
| PostgreSQL con WAL + volumen `pg_data` | `docker-compose.yml` | perder pedidos o usuarios si la BD cae de golpe |
| Errores de infraestructura vuelven a la cola (`nack` + requeue) | `OrderConsumer` | que una caída de la BD mande pedidos sanos a la DLQ |
| Rechazos de negocio como resultado final (`REJECTED` + ack) | `OrderService.process_order`, `OrderConsumer` | llenar la DLQ de pedidos sin stock que no tienen ningún error |
| Reprocesar solo pedidos `FAILED` (`requeue_failed`) | `OrderService.mark_retrying` | que un duplicado en la DLQ haga procesar dos veces un pedido ya confirmado |
| Pool de conexiones con verificación | `create_pool` | que la API y los workers queden rotos tras reiniciar la BD |
| **Correos por el outbox**: evento `email.*` en la misma transacción que su operación | `add_customer_with_email`, `mark_processed` | un usuario o pedido confirmado sin su correo (o un correo de algo que no se guardó) |
| Cola `correos` durable + mensajes persistentes + publisher confirms | `declare_topology`, `publish_email` | perder correos si RabbitMQ se reinicia o el mailer está caído |
| ACK manual tras enviar + reintentos 2 s / 5 s / 10 s + `correos.dlq` | `EmailConsumer` | perder un correo si Gmail o la red fallan, o que uno roto bloquee al resto |
| Idempotencia por `email_id` (tabla `sent_emails`) | `NotificationService.deliver` | enviar dos veces el mismo correo ante un mensaje duplicado |
| Tokens de un solo uso con vencimiento, guardados como hash | `email_tokens`, `consume_token` | reutilizar un enlace de verificación o de recuperación |
| **Cobro idempotente**: `payment_status = APPROVED` guardado antes de descontar + clave de idempotencia `order_id` en el proveedor | `OrderService._charge`, `FakePaymentProvider` | cobrar dos veces un pedido por un mensaje duplicado o por un worker que murió tras cobrar |
| Error temporal del proveedor como `ProcessingError` (reintentos 2 s / 5 s / 10 s) y pago rechazado como resultado de negocio | `OrderService._charge` | reintentar algo que no va a cambiar (tarjeta rechazada) o rendirse ante un fallo pasajero |
| Reembolso en la misma transacción que el rechazo por stock | `mark_processed` | un pedido rechazado con el pago aún cobrado |
| Tokenización: solo token + marca + últimos 4 dígitos; el número y el CVV nunca se guardan, registran ni encolan | `payments.tokenize`, `CheckoutService` | filtrar datos de tarjeta en la BD, los logs o RabbitMQ |
| **Descuento atómico de stock** (`UPDATE ... WHERE stock >= qty`) + `CHECK (stock >= 0)` | `mark_processed`, tabla `products` | vender más unidades de las que hay, aunque varios workers compren lo último a la vez |
| Descuento + confirmación + comprobante en UNA transacción, con punto de guardado | `mark_processed` | un pedido a medio descontar (todo o nada) |
| Ítems en orden de SKU | `mark_processed` | que dos workers se bloqueen en cruz (deadlock) |
| `FOR UPDATE` del pedido + `stock_descontado` | `mark_processed` | descontar dos veces por un mensaje duplicado |
| Siembra con candado (`pg_advisory_xact_lock`) + `ON CONFLICT DO NOTHING` | `init_schema`, `seed_products` | que dos contenedores siembren el catálogo a la vez al arrancar |

## Laboratorio: ¿cuánto vale la persistencia?

- **Interruptor de persistencia** (panel, tarjeta 4 «Prueba de persistencia» de Autosabotaje): los pedidos nuevos viajan por un exchange y una
  cola **no durables** con mensajes **transitorios** (solo en memoria). Si en ese modo se tira RabbitMQ, los mensajes
  se pierden de verdad: el KPI **Mensajes perdidos** lo muestra, la pizarra de PostgreSQL los marca en rojo y el botón
  **Reconciliar desde la BD** los recupera gracias a que PostgreSQL conserva los pedidos.
- El antiguo **Experimento de persistencia** (500 mensajes en colas `lab.persistente` / `lab.volatil`) se eliminó: el
  escenario 4 ya lo demuestra con pedidos reales. Si esas colas quedaron en el broker, la API y el worker las borran
  al arrancar, junto con los restos de la antigua cola «pagos».

## Autosabotaje: 6 escenarios automáticos

Arriba del panel, a la izquierda el diagrama **Recorrido de un pedido** (~65 %) y a la derecha la columna **Escenarios**
(~35 %; en pantallas angostas pasa debajo del diagrama), con dos pestañas: **Controles manuales** (la de siempre al
abrir el panel; se recuerda la última elegida) y **Autosabotaje**. Si corre un escenario, el panel cambia solo a
**Autosabotaje**. Cada escenario es una tarjeta compacta con su
número, estado, el botón **?** (ventana con **Qué hace**, **Qué vas a ver** y **Qué demuestra**: al pasar el mouse, al
enfocarla con el teclado o al tocar el **?**) y **▶ Ejecutar**: un solo botón corre toda la secuencia, sola.
El 1, **Black Friday**, no es un sabotaje sino una **prueba de carga** (no rompe nada): va con acento azul y la etiqueta
«Prueba de carga», separado de los 5 sabotajes (en rojo) por dos rótulos, «Prueba de carga · no rompe nada» y
«Sabotajes · rompemos algo a propósito». Cada rótulo aparece donde empieza un tramo de otro tipo, así que siguen
siendo correctos cuando un escenario sube al primer lugar.

Mientras corre, la tarjeta se expande con la lista de pasos (✔ terminado · ● en curso, con una frase que narra lo que
pasa · ○ pendiente) y el diagrama resalta el componente afectado (el nodo caído además se pone en rojo). Al terminar
muestra el **resumen** con el cuadre *enviados = procesados + rechazados + DLQ*. El escenario en curso (o el último
ejecutado) sube al primer lugar de la lista, así su recorrido se ve completo sin hacer scroll; vuelve a su lugar
cuando se ejecuta otro.

Reglas del orquestador (`app/services/scenarios.py`, en la API; avance en `/api/metrics` → `scenarios`):

- **Uno a la vez**: los demás botones se desactivan (la API responde 409 si se intenta otro).
- **Empieza limpio**: «Recuperar todo», persistencia encendida, venenosos apagado, sin tráfico simulado, cola vacía y
  sin mensajes de prueba en la DLQ (solo se descartan los de prueba: nunca se purgan colas).
- **■ Detener** (el mismo botón mientras corre, o **Recuperar todo**): corta el escenario y recupera todo.
- **Velocidad**: *Expo* mantiene cada estado 6 s para que el público lo vea; *Rápido*, 2 s.
- **Tiempo máximo**: cada «esperar hasta que…» falla si en 60 s no se cumple (o, al vaciar la cola, si en 60 s no
  avanza). El error queda en la tarjeta y se recupera todo.
- **Termina en pie**: pase lo que pase (bien, error o Detener) ejecuta «Recuperar todo» al final.
- **Solo pedidos de prueba**: correo `escenarioN-…@prueba.intitech`, productos del catálogo **que aún tienen stock**,
  1 unidad cada uno, pagados solos con la tarjeta de prueba aprobada (`tok_test_approved`): así el cuadre espera que
  todos se procesen. Los pedidos de prueba
  descuentan stock de verdad; si ya no queda, el escenario se detiene y pide pulsar **Restablecer stock**. El cuadre y
  la reconciliación usan solo los pedidos del escenario.
- Usa los **mismos sabotajes** que los botones (`run_chaos`): matar/revivir, persistencia, venenosos, reconciliar,
  reprocesar la DLQ. Los botones sueltos siguen en la pestaña **Controles manuales**.

| # | Escenario | Lo que hace solo | Resultado esperado |
|---|---|---|---|
| 1 | **Black Friday** (300 o 500) | envía la ráfaga → narra que la API respondió al instante → espera cola 0 mostrando la velocidad | N enviados = N procesados, con el tiempo total |
| 2 | **Matar al worker** | envía 20 → espera a que el worker esté a mitad de un pedido y lo mata → pausa ("Worker caído. N pedidos esperan…") → lo revive → espera cola 0 | 20 = 20 procesados. El worker queda `exited` hasta revivirlo (Docker no reinicia un contenedor matado a mano, ni con `unless-stopped`) |
| 3 | **Tirar RabbitMQ** | lo tira → envía 50 (la tienda sigue vendiendo) → pausa mostrando "outbox: N pendientes" → lo revive → espera outbox 0 y cola 0 | 50 = 50 procesados |
| 4 | **Prueba de persistencia** | A: persistencia apagada, worker muerto, 10 pedidos, tira y revive RabbitMQ → "la cola volvió vacía: se perdieron 10". B: persistencia encendida, 10 pedidos, tira y revive → "volvió con los 10". Cierre: reconcilia desde la BD los 10 perdidos y revive el worker | 20 = 20 procesados (10 rescatados por reconciliación) |
| 5 | **Pedidos venenosos** | activa venenosos, pone el worker lento (0,6 s por pedido) y envía 20 → pausa en el primer reintento (2 s) → pausa cuando el primero llega a la DLQ tras 2 s → 5 s → 10 s → espera cola 0 (4 en la DLQ) → desactiva y reprocesa la DLQ → espera DLQ 0 | 20 = 20 procesados |
| 6 | **Tirar PostgreSQL** | mata al worker y envía 20 → tira PostgreSQL y revive el worker → intenta 5 nuevos → pausa → revive PostgreSQL → espera cola 0 | 20 procesados, 5 rechazados con aviso (HTTP 503 "Servicio no disponible, intenta en unos segundos."), 0 en DLQ |

Medido en la laptop (modo Rápido, uno detrás de otro): 1 → 57 s, 2 → 20 s, 3 → 20 s, 4 → 29 s, 5 → 56 s, 6 → 40 s.

### Cómo trata el worker cada problema

| Problema | Ejemplo | Qué hace el worker | ¿DLQ? |
|---|---|---|---|
| Fallo del pedido | modo venenoso activo; error temporal del proveedor de pagos (tarjeta `…0119`) | reintento a los 2 s, 5 s y 10 s; al tercero, `FAILED` | sí |
| Resultado de negocio | sin stock (antes de cobrar, al descontar, o el SKU de prueba `SIN-STOCK`), pago rechazado (tarjeta `…0002`) | marca `REJECTED` (y `DECLINED` o `REFUNDED` según el caso) y hace ack | **no** |
| Infraestructura caída | PostgreSQL no responde | `nack` + requeue con espera, sin gastar reintentos | **no** |
| Duplicado | el pedido ya está `PROCESSED` o `REJECTED` | ack y lo salta (idempotencia) | no |

**Reprocesar la DLQ** republica cada mensaje en la cola principal con `retries=0` y sin los `x-death` anteriores, y
solo devuelve a "en cola" los pedidos `FAILED` (uno ya confirmado no se toca, así no se procesa dos veces). El veneno
se decide **al procesar**, según el interruptor de ese momento: no queda marcado en el pedido.

> Antes, el veneno era el SKU `FAIL` guardado dentro del pedido y el worker lo hacía fallar **siempre**: al reprocesar,
> esos pedidos volvían a fallar 3 veces y regresaban todos a la DLQ. Ahora, con el modo apagado, se procesan.

## Guion para la presentación

0. Antes de empezar: **Restablecer stock** (tarjeta **Stock por producto**) o **Reiniciar demo**.
1. **Calentamiento**: pocos pedidos (goteo o **1 pedido** en cámara lenta) para explicar el recorrido de una caja.
2. Ejecuta los 6 escenarios de **Autosabotaje** en orden con **▶ Ejecutar** (velocidad *Expo*).
3. **Iniciar Black Friday** (botón de la cabecera): en 2,5 min pasa un día completo con picos; escala con **＋ workers**.
   Los clientes simulados compran al azar, así que los productos con poco stock se agotan de verdad: sus pedidos
   siguientes salen `REJECTED` y la tarjeta **Stock por producto** nunca baja de 0.
4. Si el profesor pide repetir algo a mano: pestaña **Controles manuales**.

5. **Pago con tarjeta**: alguien del público compra desde su celular con las tarjetas de prueba del modal. Con la
   `…1111` el pedido se confirma; con la `…0002` sale «Pago rechazado: no se descontó stock»; con la `…0119` el paso 3
   avisa «reintentando el cobro» y a los ~2 s se confirma. La tarjeta **Pagos con tarjeta** del panel lo cuenta en vivo.
   Variante: **Matar al worker** justo después de pagar; al **Recuperar todo**, el pedido se cobra una sola vez.

Al final, el cuadre de la tarjeta 5 debe cerrar: aceptados = procesados + rechazados + DLQ. Cero pérdidas.

## Contrato de la API

El contrato (OpenAPI 3) está en [`docs/openapi.yaml`](docs/openapi.yaml), con los endpoints agrupados en Usuarios
(incluye verificación del correo y recuperación de contraseña), Tienda (el checkout recibe la tarjeta de prueba),
Pedidos, Panel (incluye la DLQ de correos con `?queue=correos` y los contadores de pagos), Simulador y Sabotajes. Con el sistema encendido también se puede ver y probar en
http://localhost:8000/docs (Swagger) o http://localhost:8000/redoc. FastAPI lo genera desde `app/routes/api.py`;
si cambias una ruta, vuelve a exportarlo desde http://localhost:8000/openapi.json.

## Pruebas

```bash
docker compose run --rm --no-deps api pytest -q
```
O sin Docker, en un entorno virtual (las pruebas usan SQLite y un canal de RabbitMQ falso):
```powershell
python -m venv .venv; .\.venv\Scripts\python -m pip install -r requirements.txt; .\.venv\Scripts\python -m pytest -q
```

## La tienda

- Catálogo de 100 productos (celulares, televisores, cómputo, audio, electrohogar, cocina y hogar) con precios en soles.
  Los datos viven en la tabla `products` de PostgreSQL (ver [Stock real](#stock-real-tabla-products)).
  `app/database/catalogo.json` (nombre, precios, descripción, stock inicial y productos relacionados) es la semilla: si
  lo editas, los cambios entran con una base nueva (`docker compose down -v`), y su stock con **Restablecer stock**. Las
  imágenes están en `static/img/<sku>.jpg`.
- El stock que muestra la tienda sale de la tabla (con hasta 1 s de atraso). Si PostgreSQL cae, la tienda sigue
  mostrando el catálogo con el último stock conocido.
- **Cross-selling**: "Frecuentemente comprados juntos", "Quienes vieron esto también compraron", combos,
  sugerencias al agregar al carrito y "Completa tu compra" dentro del carrito, además de una barra de envío gratis.
- Los precios se validan en el servidor (`CheckoutService`): el navegador solo envía SKU y cantidad.
- Para pagar hay que iniciar sesión; si no la tiene, la tienda lo lleva al registro y luego vuelve al pago con su carrito.
  El pago es con tarjeta de prueba (ver [Pago con tarjeta](#pago-con-tarjeta-simulado-y-asíncrono)).
- "Mi cuenta" muestra los pedidos del cliente leídos desde PostgreSQL, con su estado en vivo.
- Tras pagar, el cliente ve **su pedido viajar por la cola en vivo**: guardado en BD, en la cola con su posición,
  cobrando su tarjeta y verificando stock, y confirmado. Si RabbitMQ está caído, se le avisa que su pedido está guardado y no se perderá.
- Para cambiar el nombre de la empresa: constante `STORE_NAME` en `static/tienda.html`.

## Despliegue en AWS EC2

Servidor: Ubuntu 24.04, t3.small (2 GB de RAM + 2 GB de swap), Docker y Docker Compose instalados, usuario
`ubuntu`, IP fija `3.214.91.208`. El security group abre 80, 443 y 8000 al público y el 22 solo a la IP del
presentador. Los scripts se ejecutan en PowerShell desde la laptop, dentro de la carpeta del proyecto, y usan la clave
`$HOME\.ssh\intitech-key.pem` (otra: `-KeyPath C:\ruta\clave.pem`; otra IP: `-ServerIp 1.2.3.4`).

En el servidor se usa `docker-compose.yml` + `docker-compose.prod.yml`, que agrega:
- **Caddy** (contenedor `caddy`, configurado en `deploy/Caddyfile`) delante de la api, en los puertos 80 y 443:
  - sirve la tienda en **https://3-214-91-208.sslip.io**;
  - pide y renueva solo el certificado gratis de Let's Encrypt y lo guarda en el volumen `caddy_data`, así no lo
    vuelve a pedir en cada reinicio;
  - redirige `http://…` (también `http://3.214.91.208`) a https;
- la api arranca con `--proxy-headers`, aceptadas solo de la IP fija de Caddy. Así ve la IP real de cada celular (la
  usa el límite de intentos de login) y sabe que la petición original fue HTTPS;
- la api sigue publicada en el 8000 como respaldo directo por HTTP. Como la cookie es `Secure`, por ahí se puede
  navegar pero no iniciar sesión;
- el **panel de RabbitMQ** en https://3-214-91-208.sslip.io/rabbitmq/, de solo lectura y con doble candado (ver
  [Panel de RabbitMQ en el servidor](#panel-de-rabbitmq-en-el-servidor));
- `restart: unless-stopped` en todos los servicios: si la instancia se reinicia, todo vuelve solo;
- rotación de logs (3 archivos de 10 MB por contenedor), para no llenar el disco;
- límites de memoria por contenedor (postgres 384 MB, rabbitmq 512 MB, api 448 MB, worker 256 MB, relay, mailer y
  caddy 128 MB).

En la laptop nada de esto se levanta: `iniciar.ps1` y `docker compose up` usan solo `docker-compose.yml` (HTTP en el
8000, sin Caddy).

**Por qué sslip.io:** Let's Encrypt no emite certificados para una IP sola y no hay dominio propio. El servicio
gratuito sslip.io responde a `3-214-91-208.sslip.io` con la IP `3.214.91.208`, así que Caddy obtiene un certificado
válido para ese nombre. La primera vez tarda unos segundos; requiere que los puertos 80 y 443 estén abiertos.
`https://3.214.91.208` (la IP con https) seguirá dando error de certificado: comparte siempre el dominio (o el QR).

PostgreSQL y RabbitMQ siguen publicados solo en `127.0.0.1` del servidor: desde internet no se ven. El panel de
RabbitMQ llega al público solo a través de Caddy (siguiente apartado).

### Panel de RabbitMQ en el servidor

**https://3-214-91-208.sslip.io/rabbitmq/** (o el botón **Ver colas en RabbitMQ** del panel), sin túnel, desde
cualquier navegador. Pasa por dos candados:

1. **Caddy**: el navegador pide usuario y clave (`CADDY_RABBITMQ_USER` y la clave cuyo hash bcrypt es
   `CADDY_RABBITMQ_HASH`). Al acertar, Caddy entrega una cookie `HttpOnly` + `Secure` válida 12 h y abre el panel.
2. **RabbitMQ**: su formulario de login, con `RABBITMQ_MONITOR_USER` / `RABBITMQ_MONITOR_PASS`.

Es de **solo lectura** por partida doble: el usuario de monitoreo no puede configurar ni escribir, y Caddy además
rechaza (403) todo lo que no sea `GET` en la API de RabbitMQ (purgar, publicar, borrar, "Get messages") y oculta
usuarios, permisos y definiciones, aunque alguien entrara con el administrador.

> Por qué una "puerta" con cookie y no `basic_auth` en todo `/rabbitmq/`: el panel de RabbitMQ manda su propio
> usuario en la cabecera `Authorization` de cada consulta, la misma que usa `basic_auth`. Caddy la vería, no la
> reconocería y rechazaría todo. Por eso Caddy pide su clave solo en `/rabbitmq/acceso` y luego reconoce la cookie.

Usuarios de RabbitMQ en el servidor (`deploy/rabbitmq/arranque.sh` los crea o corrige en **cada arranque**: el `.env`
manda, también con el volumen ya creado):

| Usuario | Tags | Permisos (configure / write / read) | Para qué |
|---|---|---|---|
| `RABBITMQ_USER` | ninguno | `.*` / `.*` / `.*` | api, relay, worker y mailer por AMQP. **No** entra al panel web ni a su API HTTP |
| `RABBITMQ_MONITOR_USER` | `monitoring` | `^$` / `^$` / `.*` | panel público de solo lectura; la api lo usa para leer "en proceso" |
| `RABBITMQ_ADMIN_USER` | `administrator` | `.*` / `.*` / `.*` | administrar de verdad (purgar, publicar, mover de la DLQ), **solo por túnel SSH** |

El panel vive bajo `/rabbitmq/` (`management.path_prefix`, en `deploy/rabbitmq/20-produccion.conf`), también por el
túnel. **Administrar** (desde la laptop):
```powershell
ssh -i $HOME\.ssh\intitech-key.pem -L 15672:127.0.0.1:15672 ubuntu@3.214.91.208
```
y, con esa ventana abierta, http://localhost:15672/rabbitmq/ con `RABBITMQ_ADMIN_USER` / `RABBITMQ_ADMIN_PASS`.

**Generar el hash de Caddy, paso a paso** (en la laptop con Docker Desktop encendido, o por SSH en el servidor):

1. Inventa la clave de Caddy (20+ caracteres), p. ej. `python -c "import secrets; print(secrets.token_hex(12))"`.
   Guárdala en tu gestor de contraseñas: es la que escribirás en el navegador.
2. Genera su hash (pide la clave dos veces sin mostrarla, y no queda en el historial):
   ```powershell
   docker run --rm -it caddy:2 caddy hash-password
   ```
   Imprime algo como `$2a$14$Zk3...` (60 caracteres, bcrypt con costo 14).
3. En `.env.production`, **entre comillas simples** (el hash tiene `$` y docker compose lo interpretaría):
   `CADDY_RABBITMQ_HASH='$2a$14$Zk3...'`. `subir_env.ps1` se niega a subir si faltan las comillas.

Variables nuevas en `.env.production` (las claves, de 20 caracteres o más; `subir_env.ps1` lo valida y no sube nada
si alguna falla; el contenedor de RabbitMQ además avisa en su log):

```
CADDY_RABBITMQ_USER=puerta
CADDY_RABBITMQ_HASH='$2a$14$...'
CADDY_RABBITMQ_COOKIE=<salida de: python -c "import secrets; print(secrets.token_hex(32))">
RABBITMQ_MONITOR_USER=monitor
RABBITMQ_MONITOR_PASS=<salida de: python -c "import secrets; print(secrets.token_hex(24))">
RABBITMQ_ADMIN_USER=jefe
RABBITMQ_ADMIN_PASS=<salida de: python -c "import secrets; print(secrets.token_hex(24))">
```
`RABBITMQ_PASS` también debe tener 20 o más. Los tres usuarios de RabbitMQ deben ser distintos.

Para cambiar una clave: edítala en `.env.production` y ejecuta `subir_env.ps1` y luego `subir.ps1`. Para cerrar todas las sesiones
de la puerta, cambia `CADDY_RABBITMQ_COOKIE`. Si cambias el **nombre** de un usuario de RabbitMQ, el viejo sigue
existiendo: bórralo por el túnel (Admin → Users) o con
`docker compose -f docker-compose.yml -f docker-compose.prod.yml exec rabbitmq rabbitmqctl delete_user <viejo>`.
Si cambias algo en `deploy/rabbitmq/`, aplícalo con `... restart rabbitmq` en el servidor.

**Comprobar** (después de subir):
- https://3-214-91-208.sslip.io/rabbitmq/ → el navegador pide la clave de Caddy; con una incorrecta vuelve a pedirla.
- Luego el login de RabbitMQ: con `RABBITMQ_MONITOR_USER` se ven colas y mensajes por segundo; los botones de
  purgar o publicar responden error (solo lectura). Con `RABBITMQ_USER`: "Not management user".
- En el servidor: `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs rabbitmq | grep usuarios`
  muestra los tres usuarios con sus tags y `usuarios listos`.
- En `/panel`, la tarjeta de la cola sigue mostrando los mensajes "en proceso" (la api los lee con el usuario de
  monitoreo).

**1. Subir los secretos (la primera vez o cuando cambie alguno).** Crea `.env.production` en la carpeta del proyecto
(copia de `.env.example` con claves **distintas** a las de la laptop, `PUBLIC_DOMAIN=3-214-91-208.sslip.io`,
`PUBLIC_BASE_URL=https://3-214-91-208.sslip.io` y las variables del [panel de RabbitMQ](#panel-de-rabbitmq-en-el-servidor)).
Está en `.gitignore`. Luego:
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\subir_env.ps1
```
Revisa las variables sin mostrar sus valores (si alguna del panel de RabbitMQ falta o es corta, no sube nada) y queda
en el servidor como `~/intitech/.env` con permisos 600.

**2. Subir el código y levantar todo (cada vez que cambies algo).**
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\subir.ps1
```
Empaqueta el proyecto con `tar`, sin `.env`, `.env.*`, `.git`, entornos virtuales, cachés, logs ni datos locales.
Lo copia con `scp` a `~/intitech` (sin tocar el `.env` ni los volúmenes con pedidos y usuarios) y ejecuta
`docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build`. Al final muestra los contenedores y
la URL https. Si falta el `.env` en el servidor, se detiene y te dice cómo subirlo. Si falta `PUBLIC_DOMAIN`,
`docker compose` se niega a arrancar y lo nombra.

Si la tienda no abre por https: revisa con `estado.ps1` que `caddy` esté arriba y mira su log
(`docker compose -f docker-compose.yml -f docker-compose.prod.yml logs caddy` en el servidor). Lo habitual es que el
puerto 80 o 443 esté cerrado en el security group, y entonces Let's Encrypt no puede validar.

**3. Ver el estado.**
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\estado.ps1
```
Muestra los contenedores, la memoria de cada uno, `free -h`, `df -h` y las últimas 50 líneas del log de la api.

**4. Apagar y encender la instancia para ahorrar créditos.** En la consola de AWS: EC2 → Instancias → seleccionar →
*Estado de la instancia* → **Detener instancia** (o `aws ec2 stop-instances --instance-ids <id>`). Detenida no
cobra cómputo, solo el disco. Si la IP es una Elastic IP, se cobra mientras la instancia está detenida. Para volver,
usa **Iniciar instancia**: Docker arranca solo y los contenedores vuelven por `restart: unless-stopped`, con sus datos.
Espera ~1 minuto y revisa con `estado.ps1`. No uses **Terminar**: borra el servidor y su disco.

Si `ssh` rechaza la clave por "permisos demasiado abiertos", restríngela una vez:
`icacls $HOME\.ssh\intitech-key.pem /inheritance:r /grant:r "$($env:USERNAME):R"`.

## Si los celulares no cargan la tienda

1. Comprueba que estén en la misma red Wi-Fi que la laptop (redes de invitados suelen aislar dispositivos).
2. Abre el puerto 8000 en el Firewall de Windows (PowerShell como administrador):
   `New-NetFirewallRule -DisplayName "Demo tienda 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow`
3. Plan B: comparte internet desde el celular del presentador y conecta la laptop a ese punto de acceso.

## Limitaciones conocidas (decisiones conscientes para la demo)

| Decisión | Por qué en la demo | Qué se haría en producción |
|---|---|---|
| La API monta `/var/run/docker.sock` | los botones de sabotaje apagan contenedores reales | nunca: acceso al socket equivale a control total del host; la tolerancia a fallos se probaría con herramientas de caos aparte |
| Secretos en un archivo `.env` local (fuera de git) | simple y suficiente para una laptop | Docker secrets o un gestor de secretos (Vault, AWS Secrets Manager), con rotación |
| PostgreSQL y RabbitMQ publicados en `127.0.0.1` | inspeccionar tablas y colas desde la laptop (en el servidor, por túnel SSH); el público solo ve los puertos 80/443 (Caddy) y 8000 | sin puertos publicados: solo red interna entre contenedores |
| Panel de RabbitMQ público en `/rabbitmq/` (solo lectura, doble candado) | mostrar las colas en la exposición sin túnel | no exponerlo: VPN o túnel, o un SSO delante (oauth2-proxy) |
| La puerta de Caddy usa una cookie con un token fijo del `.env` (12 h en el navegador) | Caddy no tiene sesiones propias; cambiar `CADDY_RABBITMQ_COOKIE` las cierra todas | sesiones firmadas con vencimiento en el servidor (SSO / oauth2-proxy) |
| Sin límite de intentos en `/rabbitmq/acceso` | con claves aleatorias de 20+ caracteres la fuerza bruta es inútil; bcrypt costo 14 la hace lenta | límite por IP (fail2ban o el WAF) |
| En la nube, HTTPS con Caddy + Let's Encrypt sobre un nombre de **sslip.io** (`3-214-91-208.sslip.io`) | no hay dominio propio y Let's Encrypt no emite para una IP sola; sslip.io es gratis y resuelve el nombre a la IP. La cookie de sesión va con `Secure` | un dominio propio (no depender de un servicio DNS de terceros), cabecera HSTS y, con más tráfico, un **Application Load Balancer** de AWS con certificado de ACM |
| En la nube, el puerto 8000 sigue abierto por HTTP sin TLS (respaldo directo a la api) | si Caddy fallara durante la presentación, la tienda sigue visible; con la cookie `Secure` no se puede iniciar sesión por ahí, así que no viajan contraseñas sin cifrar desde la tienda | cerrar el 8000 en el security group: solo 80 (redirección) y 443 |
| En la laptop, HTTP sin TLS y cookie sin `Secure` | red Wi-Fi local, sin certificado (plan B sin internet) | servir siempre por HTTPS |
| El servidor público también monta `docker.sock` | los sabotajes funcionan igual que en la laptop; solo el administrador puede usarlos (la API responde 401/403 a cualquier otro) | ningún contenedor expuesto a internet con el socket de Docker |
| Sin límite de intentos de login en la tienda | en la expo muchas personas comparten la IP pública del Wi-Fi de la universidad: un límite por IP bloquearía a todos por el error de uno | límite por cuenta y por IP en un contador compartido (Redis) o el rate limit del balanceador / WAF |
| Pago con tarjeta **simulado** (`FakePaymentProvider`): no hay pasarela ni dinero real, y solo se aceptan 3 tarjetas de prueba | demo académica: muestra el cobro asíncrono, la idempotencia y los reintentos sin tocar datos de tarjeta reales | una pasarela con tokenización en el navegador (Culqi, Niubiz, Mercado Pago, Stripe): el número nunca llega a nuestro servidor (PCI DSS) |
| El número de la tarjeta de prueba sí pasa por la API (por HTTPS) antes de tokenizarse | sin pasarela no hay quien tokenice en el navegador; la API solo acepta las 3 de prueba y nunca las guarda ni las registra | el formulario de la pasarela (iframe/SDK) devuelve el token directamente |
| La memoria de idempotencia del proveedor vive en el proceso del worker | la mitad que importa está en la BD (`payment_status = APPROVED`): solo un worker que muera justo entre «el proveedor aprobó» y «guardé APPROVED» podría cobrar dos veces | la pasarela guarda la clave de idempotencia de su lado |
| Reembolso simulado (solo cambia `payment_status` a `REFUNDED`) | no hay dinero que devolver | la llamada de reembolso de la pasarela, por el outbox para no perderla |

## Notas

- "Reiniciar demo" (enlace pequeño bajo los botones) vacía la BD y las colas para repetir la presentación y restablece
  el stock según `catalogo.json` (los usuarios y la tabla `products` se conservan).
- **Migración desde la versión con Zona Yape**: al arrancar, el esquema borra las tablas `yape_pedidos` y
  `yape_notificaciones`, los productos `YAPE-...` y la columna `pago_yape_real`; la API y el worker borran del broker las
  colas `pagos`, `pagos.retry.*`, `pagos.dlq` y el exchange `tienda.pagos` (reintentan hasta que RabbitMQ responda). Las
  variables `YAPE_*` que queden en un `.env` ya no se usan.
