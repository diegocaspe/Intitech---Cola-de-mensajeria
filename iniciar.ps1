# Arranca la demo y publica la tienda en la red local para que el público entre desde su celular.
# Uso (en PowerShell, dentro de la carpeta del proyecto):  .\iniciar.ps1
# Si Windows bloquea el script:  powershell -ExecutionPolicy Bypass -File .\iniciar.ps1

$ip = Get-NetIPConfiguration |
    Where-Object { $_.IPv4DefaultGateway -ne $null -and $_.NetAdapter.Status -eq "Up" } |
    Select-Object -First 1 -ExpandProperty IPv4Address |
    Select-Object -ExpandProperty IPAddress

if (-not $ip) {
    Write-Host "No se encontró una red activa. La tienda solo estará en http://localhost:8000" -ForegroundColor Yellow
    $env:PUBLIC_URL = ""
} else {
    $env:PUBLIC_URL = "http://${ip}:8000"
}

# Las claves (PostgreSQL, RabbitMQ, SECRET_KEY, admin) viven en .env, que no se sube a git.
if (-not (Test-Path "$PSScriptRoot\.env")) {
    Write-Host "Falta el archivo .env. Copia la plantilla y rellena los valores:  Copy-Item .env.example .env" -ForegroundColor Red
    Write-Host "Ver README -> Configuracion." -ForegroundColor Red
    exit 1
}

docker compose up --build -d
if ($LASTEXITCODE -ne 0) { Write-Host "Error al levantar los contenedores. ¿Está abierto Docker Desktop?" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "  Panel (proyector):   http://localhost:8000/panel" -ForegroundColor Cyan
Write-Host "  Tienda (público):    $(if ($env:PUBLIC_URL) { $env:PUBLIC_URL } else { 'http://localhost:8000' })" -ForegroundColor Green
Write-Host ""
Write-Host "  Los celulares deben estar en la misma red Wi-Fi que esta laptop."
Write-Host "  Si no cargan, permite el puerto 8000 en el Firewall de Windows (ver README)."
Start-Process "http://localhost:8000/panel"
