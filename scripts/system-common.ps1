# system-common.ps1
#
# Shared helpers for start-all.ps1, stop-all.ps1 and status.ps1 (dot-sourced,
# not run on its own). Finds this project's processes by their command lines,
# never "all python" or "all node".

$RepoRoot = Split-Path -Parent $PSScriptRoot
$MlEngine = Join-Path $RepoRoot "ml-engine"
$Dashboard = Join-Path $RepoRoot "dashboard"
$TabDir = Join-Path $env:TEMP "rca-system"      # the generated per-tab scripts live here

$PortForwardServices = "monitoring-kube-prometheus-prometheus|flask-service|loki-gateway|monitoring-grafana"

function Test-Http([string]$Url, [int]$TimeoutSec = 3) {
    # $true if anything answers with a non-5xx status
    try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSec
        return $r.StatusCode -lt 500
    } catch [System.Net.WebException] {
        $resp = $_.Exception.Response
        return ($null -ne $resp) -and ([int]$resp.StatusCode -lt 500)
    } catch {
        return $false
    }
}

function Wait-Http([string]$Url, [int]$Seconds) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Http $Url) { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

function Test-Tcp([string]$HostName, [int]$Port, [int]$TimeoutMs = 2000) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $task = $client.ConnectAsync($HostName, $Port)
        return $task.Wait($TimeoutMs) -and $client.Connected
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Get-MongoEndpoint {
    # MONGO_URI from ml-engine\.env if set, else the default localhost:27017
    $endpoint = @{ Host = "localhost"; Port = 27017 }
    $envFile = Join-Path $MlEngine ".env"
    if (Test-Path $envFile) {
        $line = Select-String -Path $envFile -Pattern '^\s*MONGO_URI\s*=\s*mongodb://([^/:,]+)(:(\d+))?' | Select-Object -First 1
        if ($line) {
            $m = $line.Matches[0]
            $endpoint.Host = $m.Groups[1].Value
            if ($m.Groups[3].Success) { $endpoint.Port = [int]$m.Groups[3].Value }
        }
    }
    return $endpoint
}

function Get-PrometheusHistoryMinutes {
    # Minutes of UNBROKEN Flask metrics up to now (capped at 3 h), or $null if
    # Prometheus isn't reachable. Walks back from now and stops at the first gap
    # longer than 2 minutes -- e.g. the laptop being off overnight -- because the
    # pipeline's baseline window needs recent data, not just old data.
    $q = 'sum(flask_http_request_total{namespace="default"})'
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/query_range" -TimeoutSec 10 -Body @{
            query = $q; start = $now - 3 * 3600; end = $now; step = 60 }
    } catch {
        return $null
    }
    if ($r.data.result.Count -eq 0) { return 0 }
    $times = @($r.data.result[0].values | ForEach-Object { [double]$_[0] })
    if ($times.Count -eq 0 -or $now - $times[-1] -gt 120) { return 0 }   # nothing recent
    $earliest = $times[-1]
    for ($i = $times.Count - 2; $i -ge 0; $i--) {
        if ($times[$i + 1] - $times[$i] -gt 120) { break }
        $earliest = $times[$i]
    }
    return [math]::Round(($now - $earliest) / 60)
}

function Get-AllProcesses {
    Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId, Name, CommandLine, ExecutablePath
}

function Get-SystemProcesses([string]$Venv, $All = (Get-AllProcesses)) {
    # Role -> processes started by start-all.ps1 or by hand from this repo.
    # A venv's python.exe re-launches the base interpreter as a child; the
    # parent is matched here and the child is caught by Stop-Tree.
    $root = $RepoRoot.ToLower()
    $venvLower = $Venv.ToLower()
    $tabs = $TabDir.ToLower()
    $norm = { param($s) if ($s) { $s.ToLower().Replace('/', '\') } else { "" } }

    $ours = {
        param($p, $script)
        $cmd = & $norm $p.CommandLine
        $exe = & $norm $p.ExecutablePath
        ($cmd -match $script) -and ($cmd.Contains($root) -or $exe.StartsWith($venvLower))
    }

    $result = [ordered]@{
        tabs         = @($All | Where-Object { $_.Name -eq "powershell.exe" -and (& $norm $_.CommandLine).Contains($tabs) })
        pipeline     = @($All | Where-Object { $_.Name -eq "python.exe" -and (& $ours $_ 'pipeline\.py') })
        api          = @($All | Where-Object { $_.Name -eq "python.exe" -and (& $ours $_ '\bapi\.py') })
        dashboard    = @($All | Where-Object { $_.Name -eq "node.exe" -and (& $norm $_.CommandLine).Contains("$root\dashboard") })
        portforwards = @($All | Where-Object {
                ($_.Name -eq "powershell.exe" -and (& $norm $_.CommandLine) -match 'port-forwards\.ps1') -or
                ($_.Name -eq "kubectl.exe" -and $_.CommandLine -match 'port-forward' -and
                 ($_.CommandLine -match "svc/($PortForwardServices)" -or $_.CommandLine -match 'pod/promtail-'))
            })
        traffic      = @($All | Where-Object { $_.Name -eq "kubectl.exe" -and $_.CommandLine -match '\blogs\b' -and $_.CommandLine -match 'baseline-traffic' })
    }
    return $result
}

function Get-Descendants([int]$ProcessId, $All) {
    $children = @($All | Where-Object { $_.ParentProcessId -eq $ProcessId -and $_.ProcessId -ne $ProcessId })
    foreach ($c in $children) {
        Get-Descendants $c.ProcessId $All
        $c
    }
}

function Stop-Tree([int]$ProcessId, $All) {
    # children first, so a supervising loop can't restart what we just stopped
    foreach ($d in @(Get-Descendants $ProcessId $All)) {
        Stop-Process -Id $d.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}
