"""Seguridad para el servidor público: secretos débiles, URL pública, límite de login y rutas solo de admin."""
import ast
from pathlib import Path

import pytest

from app import config

STRONG_KEY = "f3a9" * 16          # 64 caracteres, como los de secrets.token_hex(32)
STRONG_PASS = "Tq7#vLm2xW9z"


# --- SECRET_KEY y ADMIN_PASSWORD ---------------------------------------------
def test_secretos_fuertes_pasan():
    assert config.weak_secrets({"SECRET_KEY": STRONG_KEY, "ADMIN_PASSWORD": STRONG_PASS}) == []


def test_valores_de_ejemplo_se_rechazan():
    problems = config.weak_secrets({"SECRET_KEY": "cambia_esto_por_una_clave_larga_y_aleatoria",
                                    "ADMIN_PASSWORD": "cambia_esta_clave_de_admin"})
    assert len(problems) == 2
    assert all("ejemplo" in p for p in problems)


def test_secretos_cortos_se_rechazan():
    problems = config.weak_secrets({"SECRET_KEY": "x9" * 10, "ADMIN_PASSWORD": "Qz7#k"})
    assert any(p.startswith("SECRET_KEY es muy corta") for p in problems)
    assert any(p.startswith("ADMIN_PASSWORD es muy corta") for p in problems)


def test_el_mensaje_nunca_incluye_el_valor():
    secret = "Admin-supersecreto-777"
    problems = config.weak_secrets({"SECRET_KEY": STRONG_KEY, "ADMIN_PASSWORD": secret})
    assert problems and all(secret not in p for p in problems)


# --- PUBLIC_BASE_URL ----------------------------------------------------------
def test_public_base_url_tiene_prioridad():
    env = {"PUBLIC_BASE_URL": "http://3.214.91.208/", "PUBLIC_URL": "http://192.168.1.5:8000"}
    assert config.public_url(env) == "http://3.214.91.208"


def test_sin_public_base_url_se_usa_la_de_iniciar_ps1():
    assert config.public_url({"PUBLIC_URL": "http://192.168.1.5:8000"}) == "http://192.168.1.5:8000"
    assert config.public_url({"PUBLIC_BASE_URL": "  ", "PUBLIC_URL": "http://192.168.1.5:8000"}) == "http://192.168.1.5:8000"
    assert config.public_url({}) == ""


# --- HTTPS: cookie Secure y Caddy solo en el servidor --------------------------------
def test_cookie_secure_solo_con_https():
    assert config.cookie_secure({"PUBLIC_BASE_URL": "https://3-214-91-208.sslip.io"}) is True
    assert config.cookie_secure({"PUBLIC_BASE_URL": "http://3.214.91.208"}) is False
    # en la laptop: solo PUBLIC_URL (http) o nada -> sin Secure, o el navegador no guardaría la sesión
    assert config.cookie_secure({"PUBLIC_URL": "http://192.168.1.5:8000"}) is False
    assert config.cookie_secure({}) is False


ROOT = Path(config.__file__).parent.parent
# La imagen Docker solo copia app/ y tests/: dentro del contenedor estas dos pruebas se saltan; corren con
# "python -m pytest" en la laptop, donde están docker-compose*.yml y deploy/.
needs_repo_files = pytest.mark.skipif(not (ROOT / "deploy" / "Caddyfile").exists(),
                                      reason="docker-compose*.yml y deploy/ no están dentro de la imagen")


@needs_repo_files
def test_caddy_solo_en_produccion_y_la_api_ya_no_usa_el_80():
    base = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    prod = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
    assert "caddy" not in base                       # en la laptop no se levanta
    assert "image: caddy:2" in prod and '"443:443"' in prod and '"80:80"' in prod
    assert '"80:8000"' not in prod                   # el 80 ahora es de Caddy
    assert "--proxy-headers" in prod and "--forwarded-allow-ips" in prod


@needs_repo_files
def test_caddyfile_hace_proxy_a_la_api_y_redirige_http():
    caddyfile = (ROOT / "deploy" / "Caddyfile").read_text(encoding="utf-8")
    assert "{$PUBLIC_DOMAIN} {" in caddyfile
    assert "reverse_proxy api:8000" in caddyfile
    assert "redir https://{$PUBLIC_DOMAIN}{uri}" in caddyfile


# --- panel de RabbitMQ público (/rabbitmq/) ------------------------------------------
def _caddy_block(caddyfile: str, start: str) -> str:
    """Texto del bloque { ... } que abre la línea que contiene `start`."""
    i = caddyfile.index(start)
    depth, j = 0, caddyfile.index("{", i)
    for k in range(j, len(caddyfile)):
        depth += {"{": 1, "}": -1}.get(caddyfile[k], 0)
        if depth == 0:
            return caddyfile[j:k + 1]
    raise AssertionError("bloque sin cerrar")


@needs_repo_files
def test_caddy_puerta_de_rabbitmq_pide_basic_auth_bcrypt_antes_de_la_cookie():
    caddyfile = (ROOT / "deploy" / "Caddyfile").read_text(encoding="utf-8")
    puerta = _caddy_block(caddyfile, "handle /rabbitmq/acceso")
    # route: en este orden. Sin route, Caddy ejecutaría header y redir antes que basic_auth
    assert "route {" in puerta
    auth, cookie, redir = (puerta.index("basic_auth bcrypt"), puerta.index("header Set-Cookie"),
                           puerta.index("redir * /rabbitmq/ 302"))
    assert auth < cookie < redir
    assert "{$CADDY_RABBITMQ_USER} {$CADDY_RABBITMQ_HASH}" in puerta
    for flag in ("HttpOnly", "Secure", "SameSite=Strict", "Path=/rabbitmq/"):
        assert flag in puerta
    # todo lo demás de /rabbitmq exige la cookie exacta; sin ella, vuelve a la puerta
    assert '@puerta_abierta expression `{http.request.cookie.rmq_acceso} == "{$CADDY_RABBITMQ_COOKIE}"`' in caddyfile
    assert "redir * /rabbitmq/acceso 302" in caddyfile
    # "redir /ruta 302" sin "*" toma la ruta como filtro y no redirige (bug real que encontramos al probar)
    assert "redir /rabbitmq" not in caddyfile


@needs_repo_files
def test_caddy_deja_el_panel_de_rabbitmq_en_solo_lectura():
    caddyfile = (ROOT / "deploy" / "Caddyfile").read_text(encoding="utf-8")
    abierta = _caddy_block(caddyfile, "handle @puerta_abierta")
    assert "not method GET HEAD" in abierta and "path /rabbitmq/api/*" in abierta
    assert "/rabbitmq/api/definitions*" in abierta and "/rabbitmq/api/users*" in abierta
    assert "reverse_proxy rabbitmq:15672" in abierta
    assert abierta.index("respond @escritura") < abierta.index("reverse_proxy rabbitmq:15672")
    assert caddyfile.count("reverse_proxy rabbitmq:15672") == 1   # solo detrás de la puerta


@needs_repo_files
def test_rabbitmq_en_produccion_path_prefix_y_usuarios_separados():
    base = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    prod = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
    # la laptop no cambia: sin script de arranque ni prefijo
    assert "arranque.sh" not in base and "path_prefix" not in base and "RABBITMQ_MONITOR" not in base
    # el 15672 sigue solo en 127.0.0.1: el override no publica puertos de RabbitMQ
    assert '"127.0.0.1:15672:15672"' in base and "15672:15672" not in prod
    assert 'entrypoint: ["bash", "/opt/intitech/arranque.sh"]' in prod
    assert "./deploy/rabbitmq/20-produccion.conf:/etc/rabbitmq/conf.d/" in prod
    for var in ("RABBITMQ_MONITOR_USER", "RABBITMQ_MONITOR_PASS", "RABBITMQ_ADMIN_USER", "RABBITMQ_ADMIN_PASS",
                "CADDY_RABBITMQ_USER", "CADDY_RABBITMQ_HASH", "CADDY_RABBITMQ_COOKIE"):
        assert "${" + var + ":?" in prod, f"{var} debe ser obligatoria en producción"
    assert "RABBITMQ_MGMT_PATH_PREFIX: /rabbitmq" in prod
    conf = (ROOT / "deploy" / "rabbitmq" / "20-produccion.conf").read_text(encoding="utf-8")
    assert "management.path_prefix = /rabbitmq" in conf


@needs_repo_files
def test_script_de_arranque_deja_cada_usuario_con_sus_tags_y_permisos():
    raw = (ROOT / "deploy" / "rabbitmq" / "arranque.sh").read_bytes()
    assert b"\r" not in raw   # bash no ejecuta un script con CRLF
    sh = raw.decode("utf-8")
    assert 'usuario "$RABBITMQ_DEFAULT_USER" "$RABBITMQ_DEFAULT_PASS" "" ".*" ".*" ".*"' in sh       # sin tags
    assert 'usuario "$RABBITMQ_MONITOR_USER" "$RABBITMQ_MONITOR_PASS" "monitoring" "^$" "^$" ".*"' in sh
    assert 'usuario "$RABBITMQ_ADMIN_USER" "$RABBITMQ_ADMIN_PASS" "administrator" ".*" ".*" ".*"' in sh
    assert "gosu rabbitmq rabbitmqctl" in sh          # como root rompería la cookie de Erlang
    assert '-lt 20' in sh                              # avisa claves cortas
    assert "trap " in sh and 'wait "$pid"' in sh       # docker stop sigue cerrando en orden


@needs_repo_files
def test_subir_env_valida_las_claves_nuevas():
    ps1 = (ROOT / "deploy" / "subir_env.ps1").read_text(encoding="utf-8")
    for var in ("RABBITMQ_PASS", "RABBITMQ_MONITOR_PASS", "RABBITMQ_ADMIN_PASS", "CADDY_RABBITMQ_COOKIE",
                "CADDY_RABBITMQ_HASH", "CADDY_RABBITMQ_USER"):
        assert var in ps1
    assert "-lt 20" in ps1 and "No se subio nada" in ps1
    assert ps1.isascii()   # PowerShell 5.1 lee los .ps1 sin BOM como ANSI


def test_monitor_de_colas_usa_usuario_y_prefijo_de_management(monkeypatch):
    import base64
    from app.messaging import management

    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"messages_unacknowledged": 3}'

    def fake_urlopen(req, timeout):
        seen["url"], seen["auth"] = req.full_url, req.get_header("Authorization")
        return Resp()

    monkeypatch.setattr(management, "urlopen", fake_urlopen)
    monkeypatch.setattr(config, "RABBITMQ_MGMT_USER", "monitor")
    monkeypatch.setattr(config, "RABBITMQ_MGMT_PASS", "clave-del-monitor-123456")
    monkeypatch.setattr(config, "RABBITMQ_MGMT_PATH_PREFIX", "/rabbitmq")
    assert management._mgmt_unacked("pedidos") == 3
    assert seen["url"].endswith(":15672/rabbitmq/api/queues/%2F/pedidos")
    assert seen["auth"] == "Basic " + base64.b64encode(b"monitor:clave-del-monitor-123456").decode()


def test_el_panel_tiene_el_enlace_a_rabbitmq():
    html = (Path(config.__file__).parent / "static" / "panel.html").read_text(encoding="utf-8")
    assert 'id="rmqLink" href="/rabbitmq/" target="_blank" rel="noopener"' in html
    assert "Ver colas en RabbitMQ" in html


# --- rutas del panel, simulador, sabotajes y reinicio: solo admin -------------------
ADMIN_PREFIXES = ("/api/sim", "/api/chaos", "/api/reset", "/api/metrics", "/api/dlq", "/api/reconcile",
                  "/api/stock", "/orders")


def _routes():
    tree = ast.parse((Path(config.__file__).parent / "routes" / "api.py").read_text(encoding="utf-8"))
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        for dec in fn.decorator_list:
            if isinstance(dec, ast.Call) and getattr(dec.func, "attr", "") in ("get", "post", "put", "delete"):
                yield dec.args[0].value, fn


def test_las_rutas_de_admin_exigen_require_admin():
    checked = 0
    for path, fn in _routes():
        if path.startswith(ADMIN_PREFIXES):
            deps = [d for d in fn.args.defaults if isinstance(d, ast.Call) and getattr(d.func, "id", "") == "Depends"]
            assert any(getattr(d.args[0], "id", "") == "require_admin" for d in deps), f"{path} no exige admin"
            checked += 1
    assert checked >= 14   # sim (4), chaos, reset, metrics, dlq (3), reconcile, orders (4)


def test_la_pagina_del_panel_verifica_el_rol_admin():
    src = next(ast.unparse(fn) for path, fn in _routes() if path == "/panel")
    assert "'admin'" in src and "RedirectResponse" in src
