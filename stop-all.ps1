# stop-all.ps1
#
# Stops what start-all.ps1 started: pipeline, API, dashboard, baseline traffic
# and port-forwards. Processes are matched by their command lines (this repo's
# scripts, the venv's python, this repo's dashboard), never all python/node.
# Minikube keeps running unless -StopCluster is passed.
#
#   powershell -ExecutionPolicy Bypass -File .\stop-all.ps1
#   .\stop-all.ps1 -StopCluster
#
# Stopping the pipeline mid-cycle is safe: each MongoDB write is a single
# atomic update, and the next start re-scans the recent window.

param(
    [string]$Venv = "C:\venvs\rca-venv2",
    [switch]$StopCluster
)

. (Join-Path $PSScriptRoot "scripts\system-common.ps1")

$all = Get-AllProcesses
$found = Get-SystemProcesses $Venv $all
$stopped = 0
# the tab hosts first: their loops would otherwise restart the pipeline/forwards
foreach ($role in @("tabs", "pipeline", "api", "dashboard", "traffic", "portforwards")) {
    foreach ($p in $found[$role]) {
        if (-not (Get-Process -Id $p.ProcessId -ErrorAction SilentlyContinue)) { continue }   # went with its parent
        Write-Host ("stopping {0,-12} pid {1,-6} {2}" -f $role, $p.ProcessId, $p.Name)
        Stop-Tree $p.ProcessId $all
        $stopped++
    }
}
if ($stopped -eq 0) { Write-Host "Nothing of ours was running." }

# the baseline traffic runs in the cluster: scale it to zero (not deleted)
$ErrorActionPreference = "Continue"
kubectl get deploy/baseline-traffic *> $null
if ($LASTEXITCODE -eq 0) {
    kubectl scale deploy/baseline-traffic --replicas=0 | Out-Null
    Write-Host "baseline-traffic scaled to 0 (start-all.ps1 scales it back to 1)."
}

if ($StopCluster) {
    Write-Host "Stopping minikube..."
    minikube stop
} else {
    Write-Host "Minikube left running (use -StopCluster to stop it)."
}

Start-Sleep -Seconds 1
$left = Get-SystemProcesses $Venv
$still = @($left.Keys | Where-Object { $left[$_].Count -gt 0 })
if ($still.Count -gt 0) {
    Write-Host "Still running: $($still -join ', ') -- run .\status.ps1" -ForegroundColor Yellow
    exit 1
}
Write-Host "All stopped." -ForegroundColor Green
