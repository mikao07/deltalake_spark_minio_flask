# 管線外部探針（供 Windows 工作排程器呼叫）
#
# 檢查：ready + guardian + freshness；FAIL 時經 pipeline_notify 發送。
# 註：Bronze 軟／硬熔斷通知在 Silver ETL 當下即時發送，不由本探針取代。
#
# 用法：
#   .\scripts\run_pipeline_probe.ps1
#   .\scripts\run_pipeline_probe.ps1 -Dataset drinks
#   .\scripts\run_pipeline_probe.ps1 -NoStrict   # 守護神 WARN 不視為失敗
#
# 工作排程器建議：
#   程式：powershell.exe
#   引數：-NoProfile -ExecutionPolicy Bypass -File "C:\Users\User\flask_spark_delta_docker\scripts\run_pipeline_probe.ps1"

param(
    [string]$Dataset = "drinks",
    [switch]$NoStrict,
    [string]$ComposeFile = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$LogDir = Join-Path $RepoRoot "var"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}
$LogFile = Join-Path $LogDir "probe_scheduler.log"
$Ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

$strictArg = if ($NoStrict) { "" } else { "--strict" }
$composeArgs = @("compose")
if ($ComposeFile -and (Test-Path $ComposeFile)) {
    $composeArgs += @("-f", $ComposeFile)
}
$composeArgs += @("exec", "-T", "web", "python", "scripts/pipeline_probe.py", $Dataset)
if ($strictArg) {
    $composeArgs += $strictArg
}

"[$Ts] start dataset=$Dataset strict=$(-not $NoStrict)" | Add-Content -Encoding utf8 $LogFile

try {
    & docker @composeArgs 2>&1 | Tee-Object -FilePath $LogFile -Append
    $code = $LASTEXITCODE
}
catch {
    "[$Ts] error: $_" | Add-Content -Encoding utf8 $LogFile
    exit 1
}

"[$Ts] exit_code=$code" | Add-Content -Encoding utf8 $LogFile
exit $code
