$ErrorActionPreference = "Stop"

# Uso:
# powershell -NoProfile -ExecutionPolicy Bypass -File .\novodashboard2\scripts\run_daily_pipeline.ps1 -Mode Mock
# powershell -NoProfile -ExecutionPolicy Bypass -File .\novodashboard2\scripts\run_daily_pipeline.ps1 -Mode Real

param(
  [ValidateSet("Mock","Real")]
  [string]$Mode = "Mock"
)

$root = (Get-Location).Path
$venv = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $venv)) { Write-Error "Venv não encontrada em .venv"; exit 1 }

# Garantir .env
$envFile = Join-Path (Join-Path $root "novodashboard2") ".env"
if (-not (Test-Path $envFile)) { Write-Error "Arquivo .env não encontrado em novodashboard2\.env"; exit 1 }

# Alternar MOCK_MODE
$envText = Get-Content $envFile -Raw
if ($Mode -eq "Mock") {
  if ($envText -match "(?m)^MOCK_MODE=") {
    ($envText -replace "(?m)^MOCK_MODE=.*$","MOCK_MODE=1") | Set-Content -Path $envFile -Encoding UTF8
  } else {
    Add-Content -Path $envFile -Value "MOCK_MODE=1" -Encoding UTF8
  }
} else {
  if ($envText -match "(?m)^MOCK_MODE=") {
    ($envText -replace "(?m)^MOCK_MODE=.*$","MOCK_MODE=0") | Set-Content -Path $envFile -Encoding UTF8
  } else {
    Add-Content -Path $envFile -Value "MOCK_MODE=0" -Encoding UTF8
  }
}

if ($Mode -eq "Mock") {
  & $venv .\novodashboard2\scripts\seed_mock_data.py
  & $venv .\novodashboard2\scripts\seed_mock_marketing.py
  & $venv -c "import sys, os; sys.path.insert(0, os.path.join(os.getcwd(),'novodashboard2')); from services.engagement_refresh import materialize_engagement_daily; print(materialize_engagement_daily())"
  & $venv -c "import sys, os; sys.path.insert(0, os.path.join(os.getcwd(),'novodashboard2')); from services.comms_impact_refresh import materialize_comms_impact_daily, materialize_comms_impact_summary; print(materialize_comms_impact_daily()); print(materialize_comms_impact_summary())"
  Write-Host "Pipeline MOCK concluído."
  exit 0
}

# Real
& $venv .\novodashboard2\scripts\init_warehouse.py
& $venv .\novodashboard2\scripts\refresh_ga4.py
& $venv .\novodashboard2\scripts\refresh_ga4_utm.py --days 30
try {
  & $venv -c "import sys, os; sys.path.insert(0, os.path.join(os.getcwd(),'novodashboard2')); from services.youtube_refresh import refresh_yt_channel_and_videos; print(refresh_yt_channel_and_videos(1))"
} catch {
  Write-Warning "YT refresh falhou (token ausente ou erro). Continuando."
}
try {
  & $venv .\novodashboard2\scripts\refresh_rd_campaigns.py --days 1
} catch {
  Write-Warning "RD refresh falhou (token ausente ou erro). Continuando."
}
& $venv -c "import sys, os; sys.path.insert(0, os.path.join(os.getcwd(),'novodashboard2')); from services.engagement_refresh import materialize_engagement_daily; print(materialize_engagement_daily())"
& $venv -c "import sys, os; sys.path.insert(0, os.path.join(os.getcwd(),'novodashboard2')); from services.comms_impact_refresh import materialize_comms_impact_daily, materialize_comms_impact_summary; print(materialize_comms_impact_daily()); print(materialize_comms_impact_summary())"

Write-Host "Pipeline REAL concluído."



