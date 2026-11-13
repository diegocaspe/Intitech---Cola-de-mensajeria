#!/usr/bin/env bash
# Arranque de RabbitMQ SOLO en el servidor (docker-compose.prod.yml lo usa como entrypoint). En la laptop no se usa.
#
# 1. Levanta RabbitMQ igual que la imagen oficial (docker-entrypoint.sh rabbitmq-server), en segundo plano.
# 2. Cuando responde, deja los usuarios como dice el .env. Es idempotente: corre en cada arranque, crea lo que falta
#    y vuelve a aplicar claves, tags y permisos (el .env manda, aunque el volumen rabbit_data ya exista).
#      RABBITMQ_DEFAULT_USER (= RABBITMQ_USER: api, relay, worker, mailer): permisos completos por AMQP y SIN tags,
#                             así no puede iniciar sesión en el panel web ni en su API HTTP.
#      RABBITMQ_MONITOR_USER: tag "monitoring", solo lectura (configure "^$", write "^$", read ".*"): el panel
#                             público https://<dominio>/rabbitmq/ y las estadísticas que lee la api.
#      RABBITMQ_ADMIN_USER:   tag "administrator": purgar, publicar, mover de la DLQ... solo por túnel SSH
#                             (Caddy deja el panel público en solo lectura aunque alguien entre con este usuario).
# 3. Se queda esperando al servidor y le pasa la señal de apagado (docker stop) para que cierre en orden.
# Nunca imprime claves: solo nombres de usuario y avisos.
set -u

avisar() { echo "[usuarios] $*"; }

docker-entrypoint.sh rabbitmq-server &
pid=$!
trap 'kill -TERM "$pid" 2>/dev/null' TERM INT

# El servidor corre como el usuario "rabbitmq" (la imagen hace gosu). rabbitmqctl debe correr igual: como root
# crearía /var/lib/rabbitmq/.erlang.cookie con dueño root antes que el servidor, y este no podría leerla.
ctl() {
    if [ "$(id -u)" = 0 ]; then gosu rabbitmq rabbitmqctl -q "$@"; else rabbitmqctl -q "$@"; fi
}

# usuario <nombre> <clave> <tags ("" = ninguno)> <configure> <write> <read>
usuario() {
    local nombre=$1 clave=$2 tags=$3
    if [ "${#clave}" -lt 20 ]; then
        avisar "AVISO: la clave de '$nombre' tiene menos de 20 caracteres. Usa una larga y aleatoria."
    fi
    # add_user falla si ya existe: entonces solo se actualiza la clave
    if ! ctl add_user "$nombre" "$clave" >/dev/null 2>&1; then
        ctl change_password "$nombre" "$clave" >/dev/null || return 1
    fi
    # sin comillas a propósito: con tags vacío, set_user_tags quita todos los tags
    # shellcheck disable=SC2086
    ctl set_user_tags "$nombre" $tags >/dev/null || return 1
    ctl set_permissions -p / "$nombre" "$4" "$5" "$6" >/dev/null || return 1
    avisar "'$nombre': tags=[${tags:-ninguno}] configure=$4 write=$5 read=$6"
}

configurar() {
    [ -f /var/lib/rabbitmq/.erlang.cookie ] || return 1   # la crea el servidor al arrancar
    ctl await_startup >/dev/null 2>&1 || return 1
    usuario "$RABBITMQ_DEFAULT_USER" "$RABBITMQ_DEFAULT_PASS" "" ".*" ".*" ".*" &&
    usuario "$RABBITMQ_MONITOR_USER" "$RABBITMQ_MONITOR_PASS" "monitoring" "^$" "^$" ".*" &&
    usuario "$RABBITMQ_ADMIN_USER" "$RABBITMQ_ADMIN_PASS" "administrator" ".*" ".*" ".*"
}

if [ "$RABBITMQ_DEFAULT_USER" = "$RABBITMQ_MONITOR_USER" ] || [ "$RABBITMQ_DEFAULT_USER" = "$RABBITMQ_ADMIN_USER" ] ||
   [ "$RABBITMQ_MONITOR_USER" = "$RABBITMQ_ADMIN_USER" ]; then
    avisar "ERROR: RABBITMQ_USER, RABBITMQ_MONITOR_USER y RABBITMQ_ADMIN_USER deben ser distintos. No se tocan los usuarios."
else
    listo=0
    for _ in $(seq 1 60); do               # hasta ~6 min: en un t3.small RabbitMQ tarda 20-45 s en arrancar
        kill -0 "$pid" 2>/dev/null || break  # el servidor se cayó o lo apagaron
        if configurar; then listo=1; break; fi
        sleep 3
    done
    if [ "$listo" = 1 ]; then avisar "usuarios listos"; else avisar "ERROR: no pude configurar los usuarios (ver arriba)"; fi
fi

# Espera al servidor. Si llega docker stop, el trap le pasa la señal y aquí se espera a que termine de cerrar.
wait "$pid"
codigo=$?
while kill -0 "$pid" 2>/dev/null; do
    wait "$pid"
    codigo=$?
done
exit "$codigo"
