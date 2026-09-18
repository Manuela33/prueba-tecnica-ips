# Validación local recomendada antes de Power BI / Databricks.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== pytest ==="
py -3.12 -m pytest src/tests -q
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== validate_pipeline_pandas ==="
py -3.12 scripts/validate_pipeline_pandas.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== run_local_e2e (PySpark opcional) ==="
py -3.12 scripts/run_local_e2e.py
exit $LASTEXITCODE
