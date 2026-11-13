# Muestra el estado del servidor: contenedores, memoria, disco y las ultimas 50 lineas del log de la api.
# Uso: powershell -ExecutionPolicy Bypass -File .\deploy\estado.ps1 [-ServerIp 1.2.3.4] [-KeyPath "C:\ruta\clave.pem"]

param(
    [string]$ServerIp = "3.214.91.208",
    [string]$KeyPath = "$HOME\.ssh\intitech-key.pem"
)

$target = "ubuntu@$ServerIp"
# BatchMode: nunca pregunta nada; -n: no lee del teclado/stdin (sin esto ssh puede no terminar nunca en Windows)
$sshOpts = @("-n", "-i", "$KeyPath", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new",
             "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")

if (-not (Test-Path -LiteralPath "$KeyPath")) { Write-Host "No encuentro la clave SSH en `"$KeyPath`"" -ForegroundColor Red; exit 1 }

$remote = @"
cd ~/intitech 2>/dev/null || { echo 'No existe ~/intitech: ejecuta primero deploy/subir.ps1'; exit 1; }
C='docker compose -f docker-compose.yml -f docker-compose.prod.yml'
echo '=== Contenedores ==='
`$C ps
echo
echo '=== Memoria por contenedor ==='
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'
echo
echo '=== Memoria del servidor ==='
free -h
echo
echo '=== Disco ==='
df -h /
echo
echo '=== Ultimas 50 lineas del log de la api ==='
`$C logs --tail 50 --no-color api
"@

Write-Host ">> Consultar estado de $target (ps, memoria, disco, logs de la api)" -ForegroundColor Cyan
Write-Host "   ssh $($sshOpts -join ' ') $target <script de estado>" -ForegroundColor DarkGray
& ssh @sshOpts $target ($remote -replace "`r", "")
$code = $LASTEXITCODE
if ($code -eq 255) { Write-Host "ERROR: no pude conectarme por SSH a $target (codigo 255): instancia apagada, IP, clave o puerto 22?" -ForegroundColor Red; exit 1 }
if ($code -ne 0) { Write-Host "ERROR: el estado del servidor fallo con codigo de salida $code." -ForegroundColor Red; exit $code }
