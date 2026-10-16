# sample-pipeline.ps1
#
# Samples the running pipeline.py's CPU and memory every -Interval seconds
# into ml-engine\logs\pipeline_usage.csv (appends), for eval/evaluate.py.
# Read-only: it only looks at the processes, never touches them.
#
#   powershell -ExecutionPolicy Bypass -File .\scripts\sample-pipeline.ps1
#
# The venv's python.exe re-launches the base interpreter as a child, so each
# sample is the sum over the pipeline process and its descendants.
# cpu_percent is of ONE core (100 = one core busy for the whole interval);
# cpu_percent_machine divides that by the number of logical CPUs.

param(
    [string]$Venv = "C:\venvs\rca-venv2",
    [int]$Interval = 60
)

. (Join-Path $PSScriptRoot "system-common.ps1")

$csv = Join-Path $MlEngine "logs\pipeline_usage.csv"
if (-not (Test-Path $csv)) {
    "time,pids,cpu_percent,cpu_percent_machine,working_set_mb,private_mb" | Out-File -Encoding ascii $csv
}
$cores = [Environment]::ProcessorCount
$previous = @{}        # pid -> TotalProcessorTime seconds at the last sample
$lastTime = $null

Write-Host "Sampling pipeline.py every $Interval s into $csv. Ctrl+C to stop."
while ($true) {
    $all = Get-AllProcesses
    $roots = (Get-SystemProcesses $Venv $all)["pipeline"]
    $ids = @()
    foreach ($r in $roots) {
        $ids += $r.ProcessId
        $ids += @(Get-Descendants $r.ProcessId $all | ForEach-Object { $_.ProcessId })
    }
    $ids = @($ids | Sort-Object -Unique)
    $procs = @($ids | ForEach-Object { Get-Process -Id $_ -ErrorAction SilentlyContinue })
    $now = Get-Date

    $current = @{}
    foreach ($p in $procs) { $current[$p.Id] = $p.TotalProcessorTime.TotalSeconds }
    if ($procs.Count -gt 0 -and $null -ne $lastTime) {
        $cpuSeconds = 0.0
        foreach ($id in $current.Keys) {
            $before = if ($previous.ContainsKey($id)) { $previous[$id] } else { 0.0 }   # new process: all its time is new
            $cpuSeconds += [math]::Max(0.0, $current[$id] - $before)   # 0.0: an int 0 picks Max(int,int) and truncates
        }
        $wall = ($now - $lastTime).TotalSeconds
        $cpu = 100 * $cpuSeconds / $wall
        $ws = ($procs | Measure-Object WorkingSet64 -Sum).Sum / 1MB
        $priv = ($procs | Measure-Object PrivateMemorySize64 -Sum).Sum / 1MB
        $line = "{0},{1},{2:F2},{3:F2},{4:F1},{5:F1}" -f $now.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss"),
            ($ids -join " "), $cpu, ($cpu / $cores), $ws, $priv
        Add-Content -Path $csv -Value $line -Encoding ascii
    }
    $previous = $current
    $lastTime = $now
    Start-Sleep -Seconds $Interval
}
