# Chaos scenarios

Small, self-cleaning failure scenarios for demonstrating Chaos Healer's
detection and AI diagnosis. Each scenario is triggered by one script, cleans
up after itself (even on Ctrl+C), and is sized to run on a laptop minikube.

## Prerequisites

- Port-forwards running in their own terminals:
  - Prometheus: `kubectl port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090`
  - Flask app: `kubectl port-forward svc/flask-service 30001:5000`
- MongoDB running locally, `ml-engine/api.py` and the dashboard running.
- Python: `C:\venvs\rca-venv2\Scripts\python.exe` (shown as `python` below).
  Run the scripts from the repo root.

## Demo flow (same for every scenario)

The detector learns "normal" from the minutes **before** it starts, so give it
clean normal traffic first:

1. **Baseline traffic** (leave running, stops by itself):
   `python chaos/baseline_traffic.py --minutes 25`
2. **Wait ~12 minutes**, then start the pipeline (from `ml-engine/`):
   `python pipeline.py --loop --interval 60 --baseline-minutes 10`
   Wait until it prints `Polling every 60s`.
3. **Wait ~3 minutes more**, then trigger the scenario (below).
   The gap keeps any leftover baseline blip from merging into the incident.
4. Watch the dashboard. Stop the pipeline and traffic with Ctrl+C when done.

Add `--no-rca` to the pipeline if the Gemini quota runs out: incidents still
appear, just without a diagnosis.

## (a) CPU exhaustion

| | |
|---|---|
| **Trigger** | `python chaos/cpu_exhaustion.py` (~4 min) |
| **What it does** | Runs `chaos/cpu-load-job.yaml`: a small busybox Job inside the cluster that calls `flask-service:5000/cpu-heavy` (~150 ms of CPU-bound hashing per request) with 4 back-to-back workers for 180s. Going through the Service spreads load across **both** flask pods, so both sit at their 250m CPU limit. |
| **Expected metrics** | `cpu_percent` climbs from ~1% to ~95% over ~2 minutes (the `rate[2m]` window), holds, then falls back. The script prints it every 15s. |
| **Expected on the dashboard** | Within 1–2 pipeline polls: a new **HIGH** incident, detection badge **ML**, anomalies showing CPU rising to ~95% with memory and error rate normal. The diagnosis names CPU exhaustion / saturation of the flask pods. The incident grows while the spike lasts; Gemini re-diagnoses only when it doubles in size or gets more severe. |
| **Recovery** | Automatic: the script deletes the Job when the load finishes (also on Ctrl+C), and CPU is back to normal ~2 minutes later. If the script was killed hard: `kubectl delete job chaos-cpu-load` (the Job also has a 5-minute hard deadline and auto-deletes 2 minutes after finishing). |
| **Check for leftovers** | `kubectl get jobs,pods -l chaos` should return nothing. |

`/cpu-heavy` does real, bounded CPU work (repeated SHA-256, capped at 200 ms), so it
saturates a pod without any CPU-limit change.
Earlier spikes peaked at ~11% because they were short (diluted by the 2-minute
rate window), went through a port-forward that pins one pod (so at most 50% of
the combined limit), and paused between requests.
