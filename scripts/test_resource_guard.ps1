# Resource Guard layered verification (blocking tests avoid Bronze OCR)
#
# Layers:
#   1) pytest tests/test_resource_guard.py
#   2) Temp .env thresholds -> API must return 400 (no OCR)
#   3) Restore defaults -> dry_run + optional Silver ETL (no false block)
#
# Usage (run -UnitOnly and -Quick in separate commands, not one line with >>):
#   .\scripts\test_resource_guard.ps1 -UnitOnly
#   .\scripts\test_resource_guard.ps1 -Quick
#   .\scripts\test_resource_guard.ps1
#   .\scripts\test_resource_guard.ps1 -ApiOnly
#   .\scripts\test_resource_guard.ps1 -ComposeFile "docker-compose.yml"
#
# Requires:
#   docker compose up --build -d   (容器須含 P3 Resource Guard 程式)
#   MinIO reachable for bronze batch count test

param(
    [string]$BaseUrl = "http://127.0.0.1:5000",
    [string]$Dataset = "drinks",
    [string]$ComposeFile = "",
    [switch]$Quick,
    [switch]$UnitOnly,
    [switch]$ApiOnly,
    [switch]$NoRecreate
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$LogDir = Join-Path $RepoRoot "var"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}
$LogFile = Join-Path $LogDir "resource_guard_test.log"
$EnvFile = Join-Path $RepoRoot ".env"
$EnvBackup = Join-Path $RepoRoot ".env.rg-test.bak"
$TempDir = Join-Path $env:TEMP ("rg_test_" + [Guid]::NewGuid().ToString("n").Substring(0, 8))

$script:Results = @()
$script:AdminToken = $null
$script:ComposeArgs = @()

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Add-Content -Encoding utf8 -Path $LogFile -Value $line
    Write-Host $line
}

function Add-TestResult {
    param(
        [string]$Name,
        [bool]$Pass,
        [string]$Detail = ""
    )
    $script:Results += [pscustomobject]@{
        Name   = $Name
        Pass   = $Pass
        Detail = $Detail
    }
    if ($Pass) {
        Write-Host "  [PASS] $Name" -ForegroundColor Green
        if ($Detail) { Write-Host "         $Detail" -ForegroundColor DarkGray }
    }
    else {
        Write-Host "  [FAIL] $Name" -ForegroundColor Red
        if ($Detail) { Write-Host "         $Detail" -ForegroundColor Yellow }
    }
}

function Resolve-ComposeFilePath {
    if ($ComposeFile -and (Test-Path (Join-Path $RepoRoot $ComposeFile))) {
        return (Join-Path $RepoRoot $ComposeFile)
    }
    $candidates = @(
        "docker-compose.yml",
        "docker-compose(new_minio).yml",
        "docker-compose - ubuntu.yml"
    )
    foreach ($name in $candidates) {
        $p = Join-Path $RepoRoot $name
        if (Test-Path $p) { return $p }
    }
    return $null
}

function Initialize-ComposeArgs {
    $cf = Resolve-ComposeFilePath
    if (-not $cf) {
        throw "docker-compose file not found; use -ComposeFile"
    }
    $script:ComposeArgs = @("compose", "-f", $cf)
    Write-Log ("compose file: " + $cf)
}

function Get-DotEnvValue {
    param([string]$Key)
    if (-not (Test-Path $EnvFile)) { return $null }
    foreach ($line in Get-Content -LiteralPath $EnvFile -Encoding UTF8) {
        if ($line -match ('^\s*' + [regex]::Escape($Key) + '\s*=\s*(.*)\s*$')) {
            return $Matches[1].Trim()
        }
    }
    return $null
}

function Set-DotEnvValue {
    param(
        [string]$Key,
        [string]$Value
    )
    if (-not (Test-Path $EnvFile)) {
        throw (".env not found: " + $EnvFile)
    }
    $lines = Get-Content -LiteralPath $EnvFile -Encoding UTF8
    $found = $false
    $out = @()
    foreach ($line in $lines) {
        if ($line -match ('^\s*' + [regex]::Escape($Key) + '\s*=')) {
            $found = $true
            $out += ($Key + "=" + $Value)
        }
        else {
            $out += $line
        }
    }
    if (-not $found) {
        $out += ($Key + "=" + $Value)
    }
    Set-Content -LiteralPath $EnvFile -Value $out -Encoding UTF8
}

function Backup-EnvFile {
    if (Test-Path $EnvBackup) {
        Write-Log "WARN: removing stale .env.rg-test.bak from a prior run"
        Remove-Item -LiteralPath $EnvBackup -Force
    }
    Copy-Item -LiteralPath $EnvFile -Destination $EnvBackup -Force
    Write-Log "backed up .env -> .env.rg-test.bak"
}

function Restore-EnvFile {
    if (Test-Path $EnvBackup) {
        Copy-Item -LiteralPath $EnvBackup -Destination $EnvFile -Force
        Remove-Item -LiteralPath $EnvBackup -Force
        Write-Log "restored .env"
    }
}

function Invoke-DockerCompose {
    param([string[]]$ExtraArgs)
    # docker compose 常把進度寫到 stderr；Stop 模式下 PowerShell 會誤判為失敗
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & docker @ComposeArgs @ExtraArgs 2>&1
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
    }
    $text = ($out | Out-String).Trim()
    if ($code -ne 0) {
        if ($text) {
            throw ("docker compose failed (exit " + $code + "): " + $text)
        }
        throw ("docker compose failed (exit " + $code + ")")
    }
    if ($text) {
        $lines = @($out | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) {
                [string]$_.Exception.Message
            }
            else {
                [string]$_
            }
        })
        $summary = ($lines | Where-Object { $_ -match "Container|Error|error|failed|Started" }) -join " | "
        if (-not $summary) {
            $summary = ($lines | Select-Object -Last 2) -join " | "
        }
        Write-Log ("docker ok: " + $summary)
    }
}

function Restart-WebContainer {
    param([int]$WarmupSec = 10)
    if ($NoRecreate) {
        Write-Log "skip recreate (-NoRecreate)"
        return
    }
    Write-Log "recreating web container for new .env ..."
    Invoke-DockerCompose @("up", "-d", "--force-recreate", "web")
    if ($WarmupSec -gt 0) {
        Write-Log ("warmup " + $WarmupSec + "s after recreate ...")
        Start-Sleep -Seconds $WarmupSec
    }
}

function Wait-ForHealth {
    param([int]$TimeoutSec = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $url = ($BaseUrl.TrimEnd("/") + "/health")
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
            if ($r.StatusCode -eq 200) {
                Write-Log ("ready: " + $url)
                return
            }
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }
    throw ("health timeout: " + $url)
}

function Get-ApiHeaders {
    $h = @{ Accept = "application/json" }
    if ($script:AdminToken) {
        $h["X-Admin-Token"] = $script:AdminToken
    }
    return $h
}

function Read-ResponseStreamUtf8 {
    param([System.IO.Stream]$Stream)
    if (-not $Stream) { return "" }
    try {
        $ms = New-Object System.IO.MemoryStream
        $Stream.CopyTo($ms)
        $bytes = $ms.ToArray()
        $ms.Close()
        $Stream.Close()
        return [System.Text.Encoding]::UTF8.GetString($bytes)
    }
    catch {
        try { $Stream.Close() } catch { }
        return ""
    }
}

function Invoke-GuardJson {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null,
        [int]$TimeoutSec = 300
    )
    $uri = $BaseUrl.TrimEnd("/") + $Path
    $bodyFile = $null
    $curlArgs = @(
        "-sS",
        "--max-time", [string]$TimeoutSec,
        "-w", "`n%{http_code}",
        "-X", $Method,
        "-H", "Accept: application/json",
        "-H", "Content-Type: application/json; charset=utf-8",
        $uri
    )
    if ($script:AdminToken) {
        $curlArgs += @("-H", ("X-Admin-Token: " + $script:AdminToken))
    }
    if ($null -ne $Body) {
        $jsonBody = $Body | ConvertTo-Json -Compress -Depth 6
        $bodyFile = Join-Path $env:TEMP ("rg_curl_" + [Guid]::NewGuid().ToString("n") + ".json")
        [System.IO.File]::WriteAllText($bodyFile, $jsonBody, (New-Object System.Text.UTF8Encoding $false))
        try {
            $curlArgs += @("--data-binary", ("@" + $bodyFile))
        }
        catch {
            Remove-Item -LiteralPath $bodyFile -Force -ErrorAction SilentlyContinue
            throw
        }
    }
    try {
        $out = & curl.exe @curlArgs 2>&1
    }
    finally {
        if ($bodyFile -and (Test-Path $bodyFile)) {
            Remove-Item -LiteralPath $bodyFile -Force -ErrorAction SilentlyContinue
        }
    }
    if ($LASTEXITCODE -ne 0) {
        throw ("curl failed: " + ($out | Out-String))
    }
    $lines = ($out | Out-String).TrimEnd() -split "`n"
    $codeLine = $lines[-1]
    $text = ""
    if ($lines.Length -gt 1) {
        $text = ($lines[0..($lines.Length - 2)] -join "`n")
    }
    $code = 0
    [void][int]::TryParse($codeLine.Trim(), [ref]$code)
    $json = $null
    if ($text) {
        try { $json = $text | ConvertFrom-Json } catch { }
    }
    return [pscustomobject]@{
        StatusCode = $code
        Raw        = $text
        Json       = $json
    }
}

function Get-ErrorText {
    param($Response)
    if ($Response.Json -and $Response.Json.error) {
        return [string]$Response.Json.error
    }
    return [string]$Response.Raw
}

function New-TinyPngFiles {
    param([int]$Count)
    if (-not (Test-Path $TempDir)) {
        New-Item -ItemType Directory -Path $TempDir -Force | Out-Null
    }
    $bytes = [byte[]](
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, 0x89,
        0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54,
        0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01,
        0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00,
        0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82
    )
    $paths = @()
    for ($i = 1; $i -le $Count; $i++) {
        $p = Join-Path $TempDir ("tiny_" + $i + ".png")
        [System.IO.File]::WriteAllBytes($p, $bytes)
        $paths += $p
    }
    return $paths
}

function Invoke-UploadImages {
    param(
        [string[]]$FilePaths,
        [string]$Ds
    )
    $uri = $BaseUrl.TrimEnd("/") + "/api/upload/images"
    $curlArgs = @("-sS", "-w", "`n%{http_code}", "-X", "POST", $uri)
    if ($script:AdminToken) {
        $curlArgs += @("-H", ("X-Admin-Token: " + $script:AdminToken))
    }
    foreach ($fp in $FilePaths) {
        $curlArgs += @("-F", ("files=@" + $fp))
    }
    $curlArgs += @("-F", ("dataset_id=" + $Ds))
    $out = & curl.exe @curlArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("curl failed: " + ($out -join " "))
    }
    $lines = ($out -join "`n") -split "`n"
    $codeLine = $lines[-1]
    $body = ($lines[0..($lines.Length - 2)] -join "`n")
    $code = [int]$codeLine
    $json = $null
    if ($body) {
        try { $json = $body | ConvertFrom-Json } catch { }
    }
    return [pscustomobject]@{
        StatusCode = $code
        Raw        = $body
        Json       = $json
    }
}

function Apply-GuardTestEnv {
    param([hashtable]$Overrides)
    Set-DotEnvValue -Key "ETL_RESOURCE_GUARD_ENABLED" -Value "true"
    foreach ($k in $Overrides.Keys) {
        Set-DotEnvValue -Key $k -Value ([string]$Overrides[$k])
    }
    Restart-WebContainer
    Wait-ForHealth
}

function Apply-DefaultGuardEnv {
    Set-DotEnvValue -Key "ETL_RESOURCE_GUARD_ENABLED" -Value "true"
    Set-DotEnvValue -Key "MAX_UPLOAD_FILES_PER_REQUEST" -Value "20"
    Set-DotEnvValue -Key "MAX_BRONZE_OCR_IMAGES" -Value "100"
    Set-DotEnvValue -Key "ETL_MAX_CONCURRENT_JOBS" -Value "1"
    Set-DotEnvValue -Key "ETL_MEMORY_MAX_PERCENT" -Value "85"
    Set-DotEnvValue -Key "ETL_MEMORY_MIN_AVAILABLE_MB" -Value "1536"
    Restart-WebContainer
    Wait-ForHealth
}

function Run-UnitTests {
    Write-Log "=== Layer 1: pytest ==="
    $py = Join-Path $RepoRoot ".venv_lock\Scripts\python.exe"
    if (-not (Test-Path $py)) {
        $py = "python"
    }
    $testPath = Join-Path $RepoRoot "tests\test_resource_guard.py"
    & $py -m pytest $testPath -q --tb=short
    $code = $LASTEXITCODE
    $ok = ($code -eq 0)
    Add-TestResult -Name "pytest_resource_guard" -Pass $ok -Detail ("exit_code=" + $code)
    return $ok
}

function Assert-ContainerHasResourceGuard {
    Write-Log "preflight: container has services.resource_guard ..."
    $py = "import importlib.util; " +
        "assert importlib.util.find_spec('services.resource_guard'), 'missing module'; " +
        "from services.resource_guard import resource_guard_enabled; " +
        "from config import MAX_UPLOAD_FILES_PER_REQUEST; " +
        "print('enabled=' + str(resource_guard_enabled())); " +
        "print('max_files=' + str(MAX_UPLOAD_FILES_PER_REQUEST))"
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & docker @ComposeArgs exec -T web python -c $py 2>&1
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
    }
    if ($code -ne 0) {
        throw (
            "container missing Resource Guard (rebuild required): docker compose up --build -d`n" +
            ($out | Out-String)
        )
    }
    Write-Log ("preflight ok: " + (($out | Out-String).Trim() -replace "`r?`n", " "))
}

function Test-RejectUploadFileCount {
    Write-Log "--- block: upload file count ---"
    Apply-GuardTestEnv @{ MAX_UPLOAD_FILES_PER_REQUEST = "2" }
    $pngs = New-TinyPngFiles -Count 3
    $up = Invoke-UploadImages -FilePaths $pngs -Ds "rg_test_upload"
    $err = Get-ErrorText $up
    $ok = ($up.StatusCode -eq 400) -and ($err -like "*上傳檔案數*")
    $detail = "HTTP " + $up.StatusCode + " " + $err
    if (-not $ok -and $up.StatusCode -eq 200) {
        $detail += " | hint: docker compose up --build -d"
    }
    Add-TestResult -Name "reject_upload_file_count" -Pass $ok -Detail $detail
}

function Test-RejectBronzeBatch {
    Write-Log "--- block: bronze image count (before OCR) ---"
    Apply-GuardTestEnv @{ MAX_BRONZE_OCR_IMAGES = "1" }
    $tg = Invoke-GuardJson -Method POST -Path "/delta/pipeline/to-gold/run" -Body @{
        dataset_id = $Dataset
        write_mode = "append"
        dry_run    = $false
        async      = $false
    } -TimeoutSec 45
    $tgErr = Get-ErrorText $tg
    if (($tg.StatusCode -eq 400) -and (
            ($tgErr -like "*Bronze OCR*") -or ($tgErr -like "*Bronze*OCR*")
        )) {
        Add-TestResult -Name "reject_bronze_batch" -Pass $true -Detail ("HTTP 400 " + $tgErr)
    }
    elseif (($tg.StatusCode -eq 400) -and (($tgErr -like "*來源*") -or ($tgErr -like "*沒有可處理*"))) {
        Add-TestResult -Name "reject_bronze_batch" -Pass $false -Detail (
            "MinIO count may be 0 (skipped batch limit): HTTP " + $tg.StatusCode + " " + $tgErr
        )
    }
    else {
        Add-TestResult -Name "reject_bronze_batch" -Pass $false -Detail (
            "expected 400 Bronze OCR; got HTTP " + $tg.StatusCode + " " + $tgErr
        )
    }
}

function Test-RejectRuntimeMemory {
    Write-Log "--- block: runtime memory ---"
    Apply-GuardTestEnv @{ ETL_MEMORY_MIN_AVAILABLE_MB = "999999" }
    $sv = Invoke-GuardJson -Method POST -Path "/delta/silver/ocr/run" -Body @{
        dataset_id = $Dataset
        dry_run    = $false
    } -TimeoutSec 45
    $svErr = Get-ErrorText $sv
    $ok = ($sv.StatusCode -eq 400) -and (
        ($svErr -like "*可用記憶體*") -or ($svErr -like "*記憶體使用率*")
    )
    $detail = "HTTP " + $sv.StatusCode + " " + $svErr
    Add-TestResult -Name "reject_runtime_memory" -Pass $ok -Detail $detail
}

function Test-PassDryRun {
    Write-Log "--- pass: pipeline dry_run ---"
    $dry = Invoke-GuardJson -Method POST -Path "/delta/pipeline/to-gold/run" -Body @{
        dataset_id = $Dataset
        dry_run    = $true
    }
    $ok = ($dry.StatusCode -eq 200) -and ($dry.Json.status -eq "dry_run")
    Add-TestResult -Name "pass_pipeline_dry_run" -Pass $ok -Detail ("HTTP " + $dry.StatusCode)
}

function Test-PassSilverEtl {
    Write-Log "--- pass: Silver ETL with default guard (merge; may skip unchanged rows) ---"
    $sv2 = Invoke-GuardJson -Method POST -Path "/delta/silver/ocr/run" -Body @{
        dataset_id = $Dataset
        dry_run    = $false
    } -TimeoutSec 600
    $sv2Err = Get-ErrorText $sv2
    $guardBlocked = ($sv2.StatusCode -eq 400) -and (
        ($sv2Err -like "*可用記憶體*") -or
        ($sv2Err -like "*記憶體使用率*") -or
        ($sv2Err -like "*管線工作*") -or
        ($sv2Err -like "*Bronze OCR*") -or
        ($sv2Err -like "*上傳檔案數*")
    )
    $ok = (-not $guardBlocked) -and ($sv2.StatusCode -in @(200, 422))
    $detail = "HTTP " + $sv2.StatusCode + " " + $sv2Err
    if ($sv2.StatusCode -eq 422) {
        $detail += " (not Resource Guard; likely bronze_quarantine or silver_quality)"
    }
    if ($sv2.Json -and $sv2.Json.merge_note) {
        $detail += " | " + $sv2.Json.merge_note
    }
    elseif ($sv2.Json -and ($null -ne $sv2.Json.updated_rows)) {
        $detail += " | updated_rows=" + $sv2.Json.updated_rows
    }
    Add-TestResult -Name "pass_silver_etl_not_guard_blocked" -Pass $ok -Detail $detail
}

function Run-ApiTests {
    Write-Log ("=== Layer 2-3: API BaseUrl=" + $BaseUrl + " Dataset=" + $Dataset + " ===")
    $token = Get-DotEnvValue -Key "ADMIN_TOKEN"
    if ($token -and ($token -ne "change_me") -and ($token.Length -gt 0) -and (-not $token.StartsWith("#"))) {
        $script:AdminToken = $token
        Write-Log "using ADMIN_TOKEN from .env"
    }

    Assert-ContainerHasResourceGuard

    Test-RejectUploadFileCount
    Test-RejectBronzeBatch
    Test-RejectRuntimeMemory

    Write-Log "--- restore default guard thresholds ---"
    Apply-DefaultGuardEnv

    Test-PassDryRun
    if ($Quick) {
        Add-TestResult -Name "pass_silver_etl_not_guard_blocked" -Pass $true -Detail "skipped (-Quick)"
    }
    else {
        Test-PassSilverEtl
    }
}

# --- main ---
Write-Log "========== Resource Guard test start =========="
$envRestored = $false

try {
    if (-not $ApiOnly) {
        Run-UnitTests | Out-Null
        if ($UnitOnly) {
            Write-Log "UnitOnly: skip API tests"
            $failCount = @($script:Results | Where-Object { -not $_.Pass }).Count
            if ($failCount -gt 0) { exit 1 } else { exit 0 }
        }
    }

    if (-not (Test-Path $EnvFile)) {
        throw ".env missing; copy from .env.example"
    }

    Initialize-ComposeArgs
    Backup-EnvFile

    try {
        Run-ApiTests
    }
    finally {
        Restore-EnvFile
        $envRestored = $true
        if (-not $NoRecreate) {
            Write-Log "recreate web after restore ..."
            Restart-WebContainer
            Wait-ForHealth
        }
    }
}
catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    if (-not $envRestored -and (Test-Path $EnvBackup)) {
        Restore-EnvFile
        if (-not $NoRecreate) {
            try { Restart-WebContainer } catch { }
        }
    }
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 2
}
finally {
    if (Test-Path $TempDir) {
        Remove-Item -LiteralPath $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Write-Log "========== summary =========="
$failed = @($script:Results | Where-Object { -not $_.Pass })
foreach ($r in $script:Results) {
    $mark = if ($r.Pass) { "OK" } else { "NG" }
    Write-Log ("  [" + $mark + "] " + $r.Name + " -- " + $r.Detail)
}
Write-Log ("done; failed " + $failed.Count + " / " + $script:Results.Count)
Write-Log ("log: " + $LogFile)

if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host ("FAILED " + $failed.Count + " test(s); see var/resource_guard_test.log") -ForegroundColor Yellow
    exit 1
}
Write-Host ""
Write-Host "All passed." -ForegroundColor Green
exit 0
