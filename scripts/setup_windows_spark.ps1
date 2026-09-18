# Binarios Hadoop para PySpark en Windows (winutils + hadoop.dll).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Bin = Join-Path $Root "tools\hadoop\bin"
New-Item -ItemType Directory -Force -Path $Bin | Out-Null
$Base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
foreach ($file in @("winutils.exe", "hadoop.dll")) {
    $dest = Join-Path $Bin $file
    if (-not (Test-Path $dest)) {
        Write-Host "Descargando $file ..."
        Invoke-WebRequest -Uri "$Base/$file" -OutFile $dest -UseBasicParsing
    }
}
Write-Host "HADOOP_HOME: $(Join-Path $Root 'tools\hadoop')"
