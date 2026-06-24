$ErrorActionPreference = "SilentlyContinue"

$envPath = Join-Path $PSScriptRoot "..\.env"
$port = 3001

if (Test-Path $envPath) {
  $portLine = Get-Content $envPath | Where-Object { $_ -match "^PORT=" } | Select-Object -First 1

  if ($portLine) {
    $port = [int]($portLine -replace "^PORT=", "")
  }
}

$connections = Get-NetTCPConnection -LocalPort $port -State Listen
$processIds = $connections | Select-Object -ExpandProperty OwningProcess -Unique

foreach ($processId in $processIds) {
  if ($processId -and $processId -ne $PID) {
    Write-Host "[SYSTEM] Stopping process using port $port (PID $processId)..."
    Stop-Process -Id $processId -Force
  }
}

Write-Host "[SYSTEM] Starting Infinity Chat on http://localhost:$port"
npm start
