# Sube los secretos del servidor: .env.production (local) -> ~/intitech/.env (servidor), con permisos 600.
# Solo hace falta la primera vez o cuando cambie algun secreto. Despues ejecuta deploy\subir.ps1.
# .env.production esta en .gitignore: nunca se sube a git. Este script no muestra su contenido.
# Uso: powershell -ExecutionPolicy Bypass -File .\deploy\subir_env.ps1 [-ServerIp 1.2.3.4] [-KeyPath "C:\ruta\clave.pem"]

param(
    [string]$ServerIp = "3.214.91.208",
    [string]$KeyPath = "$HOME\.ssh\intitech-key.pem"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent "$PSScriptRoot"
$envFile = Join-Path "$root" ".env.production"
$target = "ubuntu@$ServerIp"
$domain = ($ServerIp -replace '\.', '-') + ".sslip.io"
# BatchMode: nunca pregunta nada; si falta algo, falla al instante. -n (solo ssh): no lee del teclado/stdin.
$common = @("-i", "$KeyPath", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")
$sshOpts = @("-n") + $common

function Fail([string]$msg) { Write-Host $msg -ForegroundColor Red; exit 1 }

# Muestra el paso, lo ejecuta y, si falla, detiene el script con el codigo de salida.
function Invoke-Required([string]$desc, [string]$exe, [string[]]$argList) {
    Write-Host ">> $desc" -ForegroundColor Cyan
    Write-Host "   $exe $($argList -join ' ')" -ForegroundColor DarkGray
    & $exe @argList
    $code = $LASTEXITCODE
    if ($code -eq 255) { Fail "ERROR: no pude conectarme por SSH a $target (codigo 255). Revisa la instancia, la IP, la clave y el puerto 22." }
    if ($code -ne 0) { Fail "ERROR: '$desc' fallo con codigo de salida $code." }
}

if (-not (Test-Path -LiteralPath "$KeyPath")) { Fail "No encuentro la clave SSH en `"$KeyPath`". Pasala con -KeyPath `"C:\ruta\clave.pem`"" }
if (-not (Test-Path -LiteralPath "$envFile")) {
    Fail "No existe `"$envFile`". Copia .env.example a .env.production, pon valores reales, PUBLIC_DOMAIN=$domain y PUBLIC_BASE_URL=https://$domain"
}

# Comprobaciones sin imprimir valores: los mensajes solo nombran variables
Write-Host ">> Revisar .env.production (sin mostrar valores)" -ForegroundColor Cyan
$vals = @{}   # nombre -> valor sin comillas (si se repite, gana la ultima linea, como en docker compose)
$raw = @{}    # nombre -> valor tal cual esta escrito
foreach ($line in Get-Content -LiteralPath "$envFile") {
    if ($line -match '^\s*([A-Z_][A-Z0-9_]*)\s*=(.*)$') {
        $v = $Matches[2].Trim()
        $raw[$Matches[1]] = $v
        if ($v.Length -ge 2 -and (($v.StartsWith("'") -and $v.EndsWith("'")) -or ($v.StartsWith('"') -and $v.EndsWith('"')))) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        $vals[$Matches[1]] = $v
    }
}
foreach ($n in "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "SECRET_KEY", "ADMIN_EMAIL", "ADMIN_PASSWORD",
                "PUBLIC_BASE_URL", "PUBLIC_DOMAIN") {
    if (-not $vals[$n]) { Write-Host "   Aviso: .env.production no define $n" -ForegroundColor Yellow }
}

# Panel de RabbitMQ publico (https://<dominio>/rabbitmq/): estas son obligatorias y se validan; si algo falla no se sube.
$problemas = @()
foreach ($n in "RABBITMQ_USER", "RABBITMQ_MONITOR_USER", "RABBITMQ_ADMIN_USER", "CADDY_RABBITMQ_USER") {
    if (-not $vals[$n]) { $problemas += "falta $n" }
}
# claves largas: minimo 20 caracteres; sin "-" al inicio (rabbitmqctl la tomaria como opcion)
foreach ($n in "RABBITMQ_PASS", "RABBITMQ_MONITOR_PASS", "RABBITMQ_ADMIN_PASS", "CADDY_RABBITMQ_COOKIE") {
    $v = $vals[$n]
    if (-not $v) { $problemas += "falta $n" }
    elseif ($v.Length -lt 20) { $problemas += "$n tiene $($v.Length) caracteres: minimo 20 (genera una con: python -c `"import secrets; print(secrets.token_hex(24))`")" }
    elseif ($v.StartsWith("-")) { $problemas += "$n no puede empezar con '-'" }
}
# el token de la cookie va dentro de la configuracion de Caddy: solo letras, numeros, "_" y "-", y 32 o mas
if ($vals["CADDY_RABBITMQ_COOKIE"] -and $vals["CADDY_RABBITMQ_COOKIE"] -notmatch '^[A-Za-z0-9_-]{32,}$') {
    $problemas += "CADDY_RABBITMQ_COOKIE: usa 32 o mas caracteres de solo letras y numeros (python -c `"import secrets; print(secrets.token_hex(32))`")"
}
# hash bcrypt de caddy hash-password, entre comillas simples (tiene "$" y docker compose lo interpretaria)
$h = $vals["CADDY_RABBITMQ_HASH"]
if (-not $h) { $problemas += "falta CADDY_RABBITMQ_HASH (ver README: caddy hash-password)" }
elseif ($h -notmatch '^\$2[aby]\$\d\d\$[./A-Za-z0-9]{53}$') { $problemas += "CADDY_RABBITMQ_HASH no parece un hash bcrypt de 'caddy hash-password' (empieza con `$2a`$14`$)" }
elseif (-not ($raw["CADDY_RABBITMQ_HASH"].StartsWith("'") -and $raw["CADDY_RABBITMQ_HASH"].EndsWith("'"))) {
    $problemas += "CADDY_RABBITMQ_HASH debe ir entre comillas simples: CADDY_RABBITMQ_HASH='`$2a`$14`$...'"
}
$usuarios = @("RABBITMQ_USER", "RABBITMQ_MONITOR_USER", "RABBITMQ_ADMIN_USER") | ForEach-Object { $vals[$_] } | Where-Object { $_ }
if (@($usuarios | Select-Object -Unique).Count -ne @($usuarios).Count) {
    $problemas += "RABBITMQ_USER, RABBITMQ_MONITOR_USER y RABBITMQ_ADMIN_USER deben ser distintos"
}
if ($problemas) {
    foreach ($p in $problemas) { Write-Host "   ERROR: $p" -ForegroundColor Red }
    Fail "Corrige .env.production y vuelve a ejecutar. No se subio nada."
}
Write-Host "   OK: variables del panel de RabbitMQ presentes, claves de 20 caracteres o mas." -ForegroundColor Green

# Se sube a un archivo temporal, se le pone chmod 600 y recien entonces reemplaza al .env
Invoke-Required "Crear ~/intitech en el servidor" "ssh" ($sshOpts + @($target, "mkdir -p ~/intitech"))
Invoke-Required "Copiar .env.production con scp" "scp" ($common + @("$envFile", "${target}:~/intitech/.env.subiendo"))
Invoke-Required "Instalar como ~/intitech/.env con permisos 600" "ssh" ($sshOpts + @($target,
    "chmod 600 ~/intitech/.env.subiendo && mv -f ~/intitech/.env.subiendo ~/intitech/.env && sed -i 's/\r$//' ~/intitech/.env && ls -l ~/intitech/.env"))

Write-Host ""
Write-Host "  .env instalado en el servidor (permisos 600)." -ForegroundColor Green
Write-Host "  Si los contenedores ya estaban corriendo, aplicalo con: .\deploy\subir.ps1"
