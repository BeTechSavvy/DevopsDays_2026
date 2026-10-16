# status.ps1
#
# One screen: cluster nodes, pods that aren't Running, which local endpoints
# answer, which of our processes are alive, the pipeline's last log line and
# how much Prometheus history the baseline has to work with.
#
#   powershell -ExecutionPolicy Bypass -File .\status.ps1

param([string]$Venv = "C:\venvs\rca-venv2")

. (Join-Path $PSScriptRoot "scripts\system-common.ps1")
$ErrorActionPreference = "Continue"

function Header([string]$Text) { Write-Host "`n-- $Text" -ForegroundColor Cyan }
function Mark([bool]$Ok) { if ($Ok) { "OK  " } else { "DOWN" } }

Header "Nodes"
kubectl get nodes 2>&1 | ForEach-Object { Write-Host "  $_" }

Header "Pods not Running (all namespaces)"
$pods = kubectl get pods -A --field-selector=status.phase!=Running --no-headers 2>&1
if ($LASTEXITCODE -ne 0) { Write-Host "kubectl failed: $pods" }
elseif (-not $pods) { Write-Host "none" }
else { $pods | ForEach-Object { Write-Host "  $_" } }

Header "Endpoints"
$endpoints = [ordered]@{
    "Prometheus :9090" = "http://localhost:9090/-/ready"
    "Flask      :30001" = "http://localhost:30001/"
    "Loki       :3100" = "http://localhost:3100/ready"
    "Grafana    :3000" = "http://localhost:3000/api/health"
    "Promtail   :3101" = "http://127.0.0.1:3101/ready"
    "API        :5050" = "http://127.0.0.1:5050/api/health"
    "Dashboard  :5173" = "http://localhost:5173/"
}
foreach ($name in $endpoints.Keys) {
    Write-Host ("  {0} {1}" -f (Mark (Test-Http $endpoints[$name] 3)), $name)
}
$mongo = Get-MongoEndpoint
Write-Host ("  {0} MongoDB    {1}:{2}" -f (Mark (Test-Tcp $mongo.Host $mongo.Port)), $mongo.Host, $mongo.Port)

Header "Processes"
$found = Get-SystemProcesses $Venv
foreach ($role in @("pipeline", "api", "dashboard", "portforwards", "traffic")) {
    $pids = @($found[$role] | ForEach-Object { $_.ProcessId })
    $detail = if ($pids.Count) { "pid " + ($pids -join ", ") } else { "" }
    Write-Host ("  {0} {1,-12} {2}" -f (Mark ($pids.Count -gt 0)), $role, $detail)
}

Header "Pipeline log (last line)"
$log = Get-ChildItem (Join-Path $MlEngine "logs\pipeline*.log") -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($log) {
    $age = [int]((Get-Date) - $log.LastWriteTime).TotalMinutes
    Write-Host "  $($log.Name), written $age min ago:"
    $line = Get-Content $log.FullName -Tail 1
    Write-Host ("  " + $(if ($line.Length -gt 200) { $line.Substring(0, 200) + "..." } else { $line }))
} else {
    Write-Host "  no ml-engine\logs\pipeline*.log yet"
}

Header "Prometheus history"
$history = Get-PrometheusHistoryMinutes
if ($null -eq $history) {
    Write-Host "  Prometheus not reachable on :9090"
} else {
    $note = if ($history -ge 180) { " (3 h or more)" } elseif ($history -lt 42) { " -- less than the default baseline needs (~42 min); use -BaselineStart 15 -BaselineEnd 3" } else { "" }
    Write-Host "  ~$history min of the app's metrics$note"
}
