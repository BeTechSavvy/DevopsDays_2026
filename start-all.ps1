# start-all.ps1
#
# One command to bring the whole system up on this laptop:
#   Docker Desktop -> Minikube (both nodes Ready) -> MongoDB check ->
#   one Windows Terminal window with tabs: port-forwards, baseline-traffic,
#   pipeline, api, dashboard -> waits for the API, opens the dashboard.
#
#   powershell -ExecutionPolicy Bypass -File .\start-all.ps1
#   .\start-all.ps1 -NoRca                                  # skip Gemini (saves quota)
#   .\start-all.ps1 -BaselineStart 15 -BaselineEnd 3        # fresh cluster, little history
#   .\start-all.ps1 -SetupNodePressure                      # re-apply node 2's eviction threshold
#
# Works from any directory. Stop everything with .\stop-all.ps1; check with .\status.ps1.
# It does not create the cluster or install the Helm charts -- that's RUNBOOK.md steps 2-4.

param(
    [switch]$NoRca,
    [int]$BaselineStart = 0,
    [int]$BaselineEnd = 0,
    [string]$Venv = "C:\venvs\rca-venv2",
    [switch]$SetupNodePressure,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "scripts\system-common.ps1")

function Step([string]$Text) { Write-Host "`n== $Text" -ForegroundColor Cyan }
function Fail([string]$Text) { Write-Host "ERROR: $Text" -ForegroundColor Red; exit 1 }

$python = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path $python)) {
    Fail "No Python at $python. Create the venv (RUNBOOK.md step 1) or pass -Venv <path>."
}
if (($BaselineStart -gt 0) -xor ($BaselineEnd -gt 0)) {
    Fail "Pass both -BaselineStart and -BaselineEnd, or neither."
}

# --- already running? ----------------------------------------------------
$running = Get-SystemProcesses $Venv
$alive = @($running.Keys | Where-Object { $running[$_].Count -gt 0 })
if ($alive.Count -gt 0 -and -not $Force) {
    Write-Host "Already running: $($alive -join ', ')." -ForegroundColor Yellow
    Write-Host "Run .\stop-all.ps1 first (or .\status.ps1 to look). -Force starts another copy anyway."
    exit 1
}

# --- Docker Desktop ------------------------------------------------------
Step "Docker Desktop"
function Test-Docker { docker info --format "{{.ServerVersion}}" 2>$null | Out-Null; return $LASTEXITCODE -eq 0 }
$ErrorActionPreference = "Continue"     # native tools write progress to stderr
if (-not (Test-Docker)) {
    $exe = Join-Path $env:ProgramFiles "Docker\Docker\Docker Desktop.exe"
    if (-not (Test-Path $exe)) { Fail "Docker isn't running and Docker Desktop wasn't found at $exe." }
    Write-Host "Starting Docker Desktop..."
    Start-Process $exe
    $deadline = (Get-Date).AddMinutes(4)
    while (-not (Test-Docker)) {
        if ((Get-Date) -gt $deadline) { Fail "Docker didn't become ready within 4 minutes." }
        Start-Sleep -Seconds 5
    }
}
Write-Host "Docker is running."

# --- Minikube ------------------------------------------------------------
Step "Minikube"
$status = (minikube status 2>&1 | Out-String)
$clusterStarted = $false
if ($status -match 'not found|minikube start` to create') {
    Fail "No minikube cluster. Create it first (RUNBOOK.md step 2):`n  minikube start --driver=docker --nodes=2 --cpus=3 --memory=4096"
}
if ($LASTEXITCODE -ne 0 -or $status -match 'Stopped|Nonexistent') {
    Write-Host "Starting minikube (keeps the existing 2-node cluster)..."
    minikube start
    if ($LASTEXITCODE -ne 0) { Fail "minikube start failed (see above)." }
    $clusterStarted = $true
}
kubectl wait --for=condition=Ready node --all --timeout=300s
if ($LASTEXITCODE -ne 0) { Fail "Nodes not Ready after 5 minutes: kubectl get nodes" }

if ($SetupNodePressure) {
    & (Join-Path $RepoRoot "chaos\setup-node-pressure.ps1")
} elseif ($clusterStarted) {
    Write-Host "Minikube was restarted, which resets node 2's kubelet config. Before running" -ForegroundColor Yellow
    Write-Host "chaos/node_pressure.py, run: .\chaos\setup-node-pressure.ps1 (or start-all -SetupNodePressure)." -ForegroundColor Yellow
}

# --- MongoDB -------------------------------------------------------------
Step "MongoDB"
$mongo = Get-MongoEndpoint
if (-not (Test-Tcp $mongo.Host $mongo.Port)) {
    Fail ("MongoDB isn't reachable at $($mongo.Host):$($mongo.Port). Start it, e.g. in an admin " +
          "PowerShell: Start-Service MongoDB  (or check MONGO_URI in ml-engine\.env).")
}
Write-Host "MongoDB answers on $($mongo.Host):$($mongo.Port)."

# --- tab scripts ---------------------------------------------------------
# One small .ps1 per tab: avoids quoting a whole command line through wt.exe
# (the repo path has a space in it), and gives stop-all a path to match on.
New-Item -ItemType Directory -Force $TabDir | Out-Null
$pipelineArgs = "--loop --interval 60"
if ($NoRca) { $pipelineArgs += " --no-rca" }
if ($BaselineStart -gt 0) { $pipelineArgs += " --baseline-start $BaselineStart --baseline-end $BaselineEnd" }

$tabs = [ordered]@{
    "port-forwards"    = @"
& "$RepoRoot\port-forwards.ps1"
"@
    "baseline-traffic" = @"
kubectl apply -f "$RepoRoot\chaos\baseline-traffic.yaml"
kubectl scale deploy/baseline-traffic --replicas=1
kubectl rollout status deploy/baseline-traffic --timeout=180s
while (`$true) {
    kubectl logs -f deploy/baseline-traffic --all-containers --prefix --tail 20
    Start-Sleep -Seconds 5   # the pod restarted or the stream dropped: follow again
}
"@
    "pipeline"         = @"
Set-Location "$MlEngine"
while (`$true) {
    & "$python" -u "$MlEngine\pipeline.py" $pipelineArgs *>&1 | Tee-Object -FilePath "$MlEngine\logs\pipeline.log" -Append
    Write-Host "pipeline.py exited. Retrying in 120 s (it needs Prometheus history for its baseline). Ctrl+C to stop." -ForegroundColor Yellow
    Start-Sleep -Seconds 120
}
"@
    "api"              = @"
Set-Location "$MlEngine"
& "$python" -u "$MlEngine\api.py"
"@
    "dashboard"        = @"
Set-Location "$Dashboard"
if (-not (Test-Path node_modules)) { npm install }
npm run dev -- --strictPort
"@
}

New-Item -ItemType Directory -Force (Join-Path $MlEngine "logs") | Out-Null
$files = [ordered]@{}
foreach ($name in $tabs.Keys) {
    $file = Join-Path $TabDir "tab-$name.ps1"
    $body = "`$Host.UI.RawUI.WindowTitle = '$name'`n`$env:PYTHONIOENCODING = 'utf-8'`n" + $tabs[$name]
    Set-Content -Path $file -Value $body -Encoding ASCII
    $files[$name] = $file
}

# --- launch --------------------------------------------------------------
Step "Launching tabs"
if (Get-Command wt.exe -ErrorAction SilentlyContinue) {
    $parts = foreach ($name in $files.Keys) {
        "new-tab --title $name --suppressApplicationTitle powershell -NoExit -ExecutionPolicy Bypass -File `"$($files[$name])`""
    }
    Start-Process wt.exe -ArgumentList ("-w new " + ($parts -join " ; "))
    Write-Host "Windows Terminal: tabs $($files.Keys -join ', ')."
} else {
    foreach ($name in $files.Keys) {
        Start-Process powershell -WindowStyle Minimized -ArgumentList "-NoExit -ExecutionPolicy Bypass -File `"$($files[$name])`""
    }
    Write-Host "wt.exe not found: started $($files.Count) minimized PowerShell windows instead."
}

# --- wait and report -----------------------------------------------------
Step "Waiting for Prometheus (port-forward)"
if (Wait-Http "http://localhost:9090/-/ready" 90) {
    $history = Get-PrometheusHistoryMinutes
    $end = if ($BaselineEnd -gt 0) { $BaselineEnd } else { 30 }
    $needed = $end + 12   # the baseline needs >= 20 points at 30 s inside [now-start, now-end]
    Write-Host "Prometheus has ~$history min of the app's history."
    if ($history -lt $needed) {
        Write-Host ("Not enough history for a baseline ending $end min ago (needs ~$needed min). The " +
                    "pipeline tab will keep retrying every 2 min. To start sooner:") -ForegroundColor Yellow
        Write-Host "  .\stop-all.ps1; .\start-all.ps1 -BaselineStart 15 -BaselineEnd 3   # needs ~15 min of history" -ForegroundColor Yellow
    }
} else {
    Write-Host "Prometheus didn't answer on :9090 within 90 s -- check the port-forwards tab." -ForegroundColor Yellow
}

Step "Waiting for the API"
if (Wait-Http "http://localhost:5050/api/health" 90) {
    Write-Host "API is up: http://localhost:5050/api/health"
} else {
    Write-Host "API didn't answer within 90 s -- check the api tab." -ForegroundColor Yellow
}

if (Wait-Http "http://localhost:5173" 120) {
    Start-Process "http://localhost:5173"
    Write-Host "Dashboard opened: http://localhost:5173" -ForegroundColor Green
} else {
    Write-Host "Dashboard didn't answer on :5173 within 2 min -- check the dashboard tab." -ForegroundColor Yellow
}
Write-Host "`nCheck everything with .\status.ps1; stop with .\stop-all.ps1."
