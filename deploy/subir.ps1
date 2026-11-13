# Sube el proyecto al servidor AWS EC2 y lo (re)levanta con Docker Compose.
# Uso (PowerShell, desde cualquier carpeta):
#   powershell -ExecutionPolicy Bypass -File .\deploy\subir.ps1
#   powershell -ExecutionPolicy Bypass -File .\deploy\subir.ps1 -ServerIp 1.2.3.4 -KeyPath "C:\ruta\clave.pem"
# Requisito previo (una sola vez): el .env del servidor, con deploy\subir_env.ps1.
# Nunca envia el .env local ni .env.production: los secretos solo viajan con subir_env.ps1.

param(
    [string]$ServerIp = "3.214.91.208",
    [string]$KeyPath = "$HOME\.ssh\intitech-key.pem"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent "$PSScriptRoot"          # carpeta del proyecto (deploy\..); puede tener espacios
$target = "ubuntu@$ServerIp"
$domain = ($ServerIp -replace '\.', '-') + ".sslip.io"   # sslip.io: nombre que resuelve a la IP (para el HTTPS de Caddy)
# BatchMode: nunca pregunta nada (clave, host nuevo, contrasena): si algo falta, falla al instante en vez de esperar.
# ServerAlive: si la conexion se corta a mitad, ssh/scp terminan en ~1 min en vez de quedarse colgados.
$common = @("-i", "$KeyPath", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")
$sshOpts = @("-n") + $common        # -n: ssh no lee del teclado/stdin (en Windows, sin esto puede no terminar nunca)
$compose = "docker compose -f docker-compose.yml -f docker-compose.prod.yml"

function Fail([string]$msg) { Write-Host $msg -ForegroundColor Red; exit 1 }

# Muestra el paso, ejecuta el programa y devuelve su codigo de salida.
function Invoke-Step([string]$desc, [string]$exe, [string[]]$argList) {
    Write-Host ">> $desc" -ForegroundColor Cyan
    Write-Host "   $exe $($argList -join ' ')" -ForegroundColor DarkGray
    & $exe @argList | Out-Host      # a la pantalla en vivo, no al valor que devuelve la funcion
    return $LASTEXITCODE
}

# Igual que Invoke-Step, pero si falla detiene el script mostrando el codigo de salida.
function Invoke-Required([string]$desc, [string]$exe, [string[]]$argList) {
    $code = Invoke-Step $desc $exe $argList
    if ($code -ne 0) { Fail "ERROR: '$desc' fallo con codigo de salida $code." }
}

if (-not (Test-Path -LiteralPath "$KeyPath")) { Fail "No encuentro la clave SSH en `"$KeyPath`". Pasala con -KeyPath `"C:\ruta\clave.pem`"" }
foreach ($tool in "ssh", "scp", "tar") {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { Fail "Falta '$tool' (viene con Windows 10/11: Configuracion > Aplicaciones > Caracteristicas opcionales > Cliente OpenSSH)." }
}

# 1. El .env del servidor debe existir antes de levantar nada (sin el, compose se niega a arrancar)
Write-Host "1/4 Revisando el servidor $ServerIp ..." -ForegroundColor Yellow
$code = Invoke-Step "Comprobar que existe ~/intitech/.env en el servidor" "ssh" ($sshOpts + @($target, "test -f ~/intitech/.env"))
if ($code -eq 255) { Fail "ERROR: no pude conectarme por SSH a $target (codigo 255). Revisa que la instancia este encendida, la IP, la clave y que el puerto 22 este abierto para tu IP." }
if ($code -eq 1) {
    Write-Host "Falta ~/intitech/.env en el servidor." -ForegroundColor Red
    Write-Host "Crea .env.production en la carpeta del proyecto (copia de .env.example con valores reales y" -ForegroundColor Yellow
    Write-Host "PUBLIC_DOMAIN=$domain y PUBLIC_BASE_URL=https://$domain) y subelo con:" -ForegroundColor Yellow
    Write-Host "  powershell -ExecutionPolicy Bypass -File .\deploy\subir_env.ps1" -ForegroundColor Yellow
    exit 1
}
if ($code -ne 0) { Fail "ERROR: la comprobacion del .env fallo con codigo de salida $code." }
Write-Host "   .env presente en el servidor." -ForegroundColor Green

# 2. Empaquetar sin secretos, sin git, sin entornos virtuales ni datos locales
Write-Host "2/4 Empaquetando el proyecto ..." -ForegroundColor Yellow
$tarFile = Join-Path "$env:TEMP" "intitech-subida.tar.gz"
if (Test-Path -LiteralPath "$tarFile") { Remove-Item -LiteralPath "$tarFile" -Force }
$excludes = @(".env", ".env.*", ".git", ".venv", "venv", "__pycache__", "*.pyc", "*.log", "./data", "data/",
              ".pytest_cache", ".impeccable", ".vscode", ".claude", "*.db", "*.sqlite", "*.sqlite3", "uploads",
              "node_modules", "*.tar.gz", "*.pem")
$tarArgs = @("-czf", "$tarFile")
foreach ($e in $excludes) { $tarArgs += "--exclude=$e" }
$tarArgs += @("-C", "$root", ".")
Invoke-Required "Crear el paquete $tarFile desde `"$root`"" "tar" $tarArgs
$secretos = & tar -tzf "$tarFile" | Where-Object { $_ -match '(^|/)\.env($|\.)' -or $_ -match '\.pem$' }
if ($secretos) { Remove-Item -LiteralPath "$tarFile" -Force; Fail "ERROR: el paquete incluia archivos de secretos ($secretos). Cancelado." }
Write-Host ("   {0:N1} MB, sin archivos de secretos" -f ((Get-Item -LiteralPath "$tarFile").Length / 1MB)) -ForegroundColor Green

# 3. Copiar al servidor
Write-Host "3/4 Copiando al servidor ..." -ForegroundColor Yellow
Invoke-Required "Copiar el paquete con scp" "scp" ($common + @("$tarFile", "${target}:~/intitech-subida.tar.gz"))
Remove-Item -LiteralPath "$tarFile" -Force

# 4. Descomprimir en ~/intitech y levantar. Se borran solo app/, tests/ y docs/ (para no dejar archivos viejos);
#    el .env y los volumenes de Docker (pedidos, usuarios, colas) no se tocan.
Write-Host "4/4 Construyendo y levantando los contenedores (la primera vez tarda unos minutos) ..." -ForegroundColor Yellow
$remote = @"
set -e
echo '   - descomprimiendo en ~/intitech'
mkdir -p ~/intitech
cd ~/intitech
rm -rf app tests docs
tar -xzf ~/intitech-subida.tar.gz
rm -f ~/intitech-subida.tar.gz
# git en Windows puede dejar el script de RabbitMQ con fin de linea CRLF y bash no lo ejecutaria
sed -i 's/\r`$//' deploy/rabbitmq/arranque.sh
if ! docker info >/dev/null 2>&1; then
  echo 'El usuario ubuntu no puede usar Docker. Ejecuta una vez: sudo usermod -aG docker ubuntu  (y vuelve a entrar por SSH)'
  exit 3
fi
echo '   - $compose up -d --build --remove-orphans'
$compose up -d --build --remove-orphans
# La api monta ./app/static del disco (docker-compose.yml). Arriba se borro y se volvio a crear app/, asi que si la
# api no se recreo (codigo igual) su montaje apunta a la carpeta vieja ya borrada: /panel daria 500. Reiniciarla lo
# vuelve a enlazar. Lo mismo vale para Caddy con deploy/Caddyfile.
echo '   - $compose restart api caddy'
$compose restart api caddy
echo
echo '   - $compose ps'
$compose ps
"@
Invoke-Required "Descomprimir y ejecutar docker compose en el servidor" "ssh" ($sshOpts + @($target, ($remote -replace "`r", "")))

Write-Host ""
Write-Host "  Listo. Tienda:  https://$domain" -ForegroundColor Green
Write-Host "  (la primera vez Caddy tarda unos segundos en obtener el certificado HTTPS)"
Write-Host "  Panel (admin):  https://$domain/panel" -ForegroundColor Cyan
Write-Host "  RabbitMQ (solo lectura):  https://$domain/rabbitmq/  (clave de Caddy y luego RABBITMQ_MONITOR_USER)" -ForegroundColor Cyan
Write-Host "  Estado y logs:  powershell -ExecutionPolicy Bypass -File .\deploy\estado.ps1"
