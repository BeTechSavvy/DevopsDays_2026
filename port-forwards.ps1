# port-forwards.ps1
#
# Opens every port-forward the project needs and restarts any that drop
# (kubectl port-forward exits when a pod restarts or the tunnel idles out).
# Leave it running in its own terminal; Ctrl+C stops all of them.
#
#   powershell -ExecutionPolicy Bypass -File .\port-forwards.ps1
#
#   localhost:9090  Prometheus
#   localhost:30001 Flask app
#   localhost:3100  Loki (via loki-gateway)
#   localhost:3000  Grafana (user admin, password: see RUNBOOK.md)

$forwards = @(
    @{ Name = "prometheus"; Args = "svc/monitoring-kube-prometheus-prometheus 9090:9090" },
    @{ Name = "flask";      Args = "svc/flask-service 30001:5000" },
    @{ Name = "loki";       Args = "svc/loki-gateway 3100:80" },
    @{ Name = "grafana";    Args = "svc/monitoring-grafana 3000:80" }
)

$jobs = foreach ($f in $forwards) {
    Start-Job -Name $f.Name -ArgumentList $f.Args -ScriptBlock {
        param($pfArgs)
        while ($true) {
            & kubectl port-forward @($pfArgs -split " ") 2>&1 | Out-Null
            Start-Sleep -Seconds 2   # tunnel dropped: reopen it
        }
    }
}

Write-Host "Port-forwards running: Prometheus :9090, Flask :30001, Loki :3100, Grafana :3000. Ctrl+C to stop."
try {
    while ($true) { Start-Sleep -Seconds 60 }
} finally {
    $jobs | Stop-Job
    $jobs | Remove-Job -Force
    Get-Process kubectl -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match "port-forward" } | Stop-Process -Force
    Write-Host "Port-forwards stopped."
}
