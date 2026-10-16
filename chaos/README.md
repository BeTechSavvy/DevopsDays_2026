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
| **Trigger** | `python chaos/cpu_exhaustion.py` (~5 min) |
| **What it does** | Runs `chaos/cpu-load-job.yaml`: a small busybox Job inside the cluster that calls `flask-service:5000/cpu-heavy` (~150 ms of CPU-bound hashing per request) with 4 back-to-back workers for 240s. Going through the Service spreads load across **both** flask pods, so both sit at their 250m CPU limit. |
| **Expected metrics** | `cpu_percent` climbs from ~1% to ~95% over ~2 minutes (the `rate[2m]` window), holds, then falls back. The script prints it every 15s. |
| **Expected on the dashboard** | Within 1–2 pipeline polls: a new **HIGH** incident, detection badge **ML**, anomalies showing CPU rising to ~95% with memory and error rate normal. The diagnosis names CPU exhaustion / saturation of the flask pods. The incident grows while the spike lasts; Gemini re-diagnoses only when it doubles in size or gets more severe. |
| **Recovery** | Automatic: the script deletes the Job when the load finishes (also on Ctrl+C), and CPU is back to normal ~2 minutes later. If the script was killed hard: `kubectl delete job chaos-cpu-load` (the Job also has a 5-minute hard deadline and auto-deletes 2 minutes after finishing). |
| **Check for leftovers** | `kubectl get jobs,pods -l chaos` should return nothing. |

`/cpu-heavy` does real, bounded CPU work (repeated SHA-256, capped at 200 ms), so it
saturates a pod without any CPU-limit change.
Earlier spikes peaked at ~11% because they were short (diluted by the 2-minute
rate window), went through a port-forward that pins one pod (so at most 50% of
the combined limit), and paused between requests.

## (b) OOMKilled

| | |
|---|---|
| **Trigger** | `python chaos/oom_killed.py` (~6 min) |
| **What it does** | Runs `chaos/oom-deployment.yaml`: one pod (`chaos-oom`) that leaks 0.5 MiB/s under a 128Mi limit. The leak is slow on purpose, so the failure predictor can warn before the kill. |
| **Expected** | Memory climbs steadily; a `predicted_exhaustion` warning (MEDIUM) about a minute or more before the first kill at ~4 min; then restarts with reason **OOMKilled** (and CrashLoopBackOff), a **HIGH** rule incident for workload `chaos-oom`. |
| **Recovery** | The script deletes the Deployment. Manual: `kubectl delete deployment chaos-oom`. |

## (c) CrashLoopBackOff

| | |
|---|---|
| **Trigger** | `python chaos/crashloop.py` (~4 min) |
| **What it does** | Runs `chaos/crashloop-deployment.yaml`: a container that prints a fatal config error and exits 1 on every start. |
| **Expected** | Restarts (reason Error), waiting in **CrashLoopBackOff**, the Deployment unavailable: a **HIGH** rule incident for `chaos-crashloop`; the log evidence shows the FATAL line. |
| **Recovery** | The script deletes the Deployment. Manual: `kubectl delete deployment chaos-crashloop`. |

## (d) Failed rollout

| | |
|---|---|
| **Trigger** | `python chaos/failed_rollout.py` (~6 min) |
| **What it does** | Creates `chaos-rollout` (a copy of the flask app, not behind flask-service), waits until healthy, then sets its image to `flask-demo:v2-does-not-exist`. After 4 min (at least 3) it runs `kubectl rollout undo`. |
| **Expected** | New pods in **ErrImagePull / ImagePullBackOff** while the old ones keep serving: a **HIGH** rule incident for `chaos-rollout`. |
| **Recovery** | Rollback, then the script deletes the Deployment. Manual: `kubectl delete deployment chaos-rollout`. |

## (e) Node memory pressure

| | |
|---|---|
| **Setup (once per `minikube start`)** | `powershell -ExecutionPolicy Bypass -File .\chaos\setup-node-pressure.ps1` -- gives node 2's kubelet a `memory.available` eviction threshold (minikube's default config has none; see the script header). |
| **Trigger** | `python chaos/node_pressure.py` (~4 min). Refuses to run without the setup. |
| **What it does** | Runs `chaos/node-stress.yaml`: a BestEffort pod pinned to node 2 (`minikube-m02`) that takes ~1.6 GiB. Monitoring is pinned to node 1. |
| **Expected** | Node 2 reports **MemoryPressure**, the stress pod is **Evicted**, its replacement can't be scheduled: HIGH rule incidents for node `minikube-m02` / workload `chaos-node-stress`. |
| **Recovery** | The script deletes the Deployment and evicted pods. |

## (f) Database connectivity loss

| | |
|---|---|
| **Setup (once)** | `minikube image build -t demo-db-app demo-db/ --all` and `kubectl apply -f demo-db/demo-db.yaml` (orders service + Redis). |
| **Trigger** | `python chaos/db_failure.py` (~4 min) |
| **What it does** | Scales `demo-redis` to 0 for 4 minutes, then back to 1. |
| **Expected** | `demo-db-app` answers 500 "database unavailable" to the baseline traffic, so its error_rate jumps to ~100%: an ML incident for `demo-db-app` with "cannot reach redis" log lines as evidence (kind `db_connection`). |
| **Recovery** | The script scales Redis back to 1 (also on Ctrl+C). Manual: `kubectl scale deployment demo-redis --replicas=1`. |

## Running all of them (evaluation)

`python chaos/run_all.py` runs every scenario in sequence, 3 rounds, with 6-minute
cool-downs (~2.5 hours), with `pipeline.py --loop` running. Each script appends
its ground truth (`scenario, start, end, target, expected_kind`) to
`chaos/injections.jsonl`; then `python eval/evaluate.py` scores the run.