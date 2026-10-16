# RUNBOOK — fresh machine to running demo

Windows 11 + PowerShell, Docker Desktop, Minikube (Docker driver, 2 nodes).
Every command is run from the repository root (`Final_year_project\`) unless a
step says otherwise. Times in the scripts and the dashboard are **UTC**.

## Quick start (machine already set up)

Once steps 0–4 below have been done once, everything else is three commands
(from any directory; Windows Terminal opens one window with a tab per service):

```powershell
.\start-all.ps1            # Docker, minikube, port-forwards, traffic, pipeline (with RCA), API, dashboard
.\status.ps1               # one screen: nodes, failing pods, endpoints, processes, history
.\stop-all.ps1             # stop all of it; -StopCluster also stops minikube
```

`start-all.ps1` options: `-NoRca` (no Gemini calls), `-BaselineStart 15
-BaselineEnd 3` (fresh cluster with little history — it warns when that's
needed), `-SetupNodePressure` (re-applies step 6 after a minikube restart),
`-Venv <path>` (default `C:\venvs\rca-venv2`). If PowerShell refuses to run
scripts, use `powershell -ExecutionPolicy Bypass -File .\start-all.ps1`.

---

## 0. Install once

| Tool | Version used | Notes |
|---|---|---|
| Docker Desktop | 4.x | WSL 2 backend |
| Minikube | 1.38 | `winget install Kubernetes.minikube` |
| kubectl | 1.35 | `winget install Kubernetes.kubectl` |
| Helm | 3.x | `winget install Helm.Helm` |
| Python | 3.13 | python.org installer |
| Node.js | 20+ | for the dashboard |
| MongoDB Community | 7+ | as a Windows service on `localhost:27017` |

Give WSL (and so Docker) 10 GB of memory — two 4 GB nodes don't fit in the
default ~7.6 GB:

```powershell
@"
[wsl2]
memory=10GB
processors=8
"@ | Out-File -Encoding ascii "$env:USERPROFILE\.wslconfig"
wsl --shutdown
# start Docker Desktop again and wait for "Engine running", then check (~10.4e9):
docker info --format "{{.MemTotal}}"
```

## 1. Python environment

```powershell
python -m venv C:\venvs\rca-venv2
C:\venvs\rca-venv2\Scripts\python.exe -m pip install --upgrade pip
C:\venvs\rca-venv2\Scripts\python.exe -m pip install -r ml-engine\requirements.txt
```

Below, `python` means `C:\venvs\rca-venv2\Scripts\python.exe` (or activate it:
`C:\venvs\rca-venv2\Scripts\Activate.ps1`).

Create `ml-engine\.env` from the template and put your Gemini key in it:

```powershell
Copy-Item ml-engine\.env.example ml-engine\.env
notepad ml-engine\.env        # set GOOGLE_API_KEY=...
```

## 2. Cluster (2 nodes)

```powershell
minikube start --driver=docker --nodes=2 --cpus=3 --memory=4096
kubectl get nodes            # minikube and minikube-m02, both Ready
```

Pre-pull the small images the chaos scenarios use, on both nodes (a slow pull
during an experiment shows up as an extra ImagePullBackOff incident):

```powershell
minikube ssh -n minikube -- docker pull busybox:1.36
minikube ssh -n minikube-m02 -- docker pull busybox:1.36
minikube ssh -n minikube -- docker pull redis:7.4-alpine
```

## 3. Monitoring stack (Helm)

```powershell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm install monitoring prometheus-community/kube-prometheus-stack -f monitoring-values.yaml --wait --timeout 10m
helm install loki grafana/loki -f loki-values.yaml --wait --timeout 10m
helm install promtail grafana/promtail -f promtail-values.yaml --wait
```

The release name **must be `monitoring`**: `servicemonitor.yaml` and
`demo-db/demo-db.yaml` carry `release: monitoring`, and the port-forwards use
`svc/monitoring-…`. Prometheus, Grafana, Loki and kube-state-metrics are pinned
to node 1 so the node-pressure scenario on node 2 can't take them down.

## 4. Applications

```powershell
minikube image build -t flask-demo . --all
minikube image build -t demo-db-app demo-db/ --all
kubectl apply -f deployment.yaml -f service.yaml -f servicemonitor.yaml
kubectl apply -f demo-db/demo-db.yaml
kubectl rollout status deploy/flask-deployment
kubectl rollout status deploy/demo-db-app
kubectl apply -f chaos/baseline-traffic.yaml      # steady normal traffic, in-cluster
```

`--all` loads the image on both nodes (`deployment.yaml` uses
`imagePullPolicy: Never`).

## 5. Port-forwards (own terminal, leave running)

```powershell
powershell -ExecutionPolicy Bypass -File .\port-forwards.ps1
```

Prometheus `:9090`, Flask `:30001`, Loki `:3100`, Grafana `:3000` — reopened
automatically if one drops. Check http://localhost:9090/targets: the
`flask-service` and `demo-db-service` targets must be **UP**.

Grafana login: `admin` / the password from
`kubectl get secret monitoring-grafana -o jsonpath="{.data.admin-password}"`
(base64 — decode with
`[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String("<value>"))`).

## 6. Node pressure setup (after every `minikube start`)

```powershell
powershell -ExecutionPolicy Bypass -File .\chaos\setup-node-pressure.ps1
```

Gives node 2's kubelet a `memory.available < 8Gi` eviction threshold.
Minikube's default config has none, so a memory hog would OOM-kill node 2's
container instead of the kubelet reporting MemoryPressure. The kubelet sees
~9.7 GiB of capacity and ~0.5 GiB in use on an idle node 2, so 8Gi trips at
~1.7 GiB used — the stress pod's 1.6 GiB crosses it, idle doesn't. Check what
the kubelet sees with
`kubectl get --raw /api/v1/nodes/minikube-m02/proxy/stats/summary` and pass
`-Threshold 7.5Gi` (etc.) if your numbers differ. Undo with `-Undo`.

## 7. Run the system (each in its own terminal)

The detector learns "normal" from **[now-60m, now-30m]** by default, so let the
baseline traffic from step 4 run ~35 minutes first — or on a fresh cluster use
a shorter window, e.g. `--baseline-start 15 --baseline-end 3` (needs ~15 min).

```powershell
cd ml-engine; python pipeline.py --loop --interval 60            # detection + RCA
cd ml-engine; python api.py                                      # http://localhost:5050
cd dashboard; npm install; npm run dev                           # http://localhost:5173
```

`pipeline.py --no-rca` skips Gemini; incidents still appear, with evidence and
the rule-based counterfactual but no LLM diagnosis.

**Gemini quota:** the free tier allows **20 requests per day per model**
(error `429 RESOURCE_EXHAUSTED`, quota `GenerateRequestsPerDayPerProjectPerModel-FreeTier`).
A demo fits in that; a full evaluation run (~40+ incidents) does not. For the
evaluation, run the pipeline with `--no-rca` and diagnose a sample afterwards:
`python eval/rca_backfill.py --since <run start>` (one call per detected
injection; `--model gemini-flash-lite-latest` uses a different model's quota).
Or enable billing on the Google AI project.

For a long experiment, `--refit-every 10` keeps the baseline closer to slow
drift (e.g. the app's memory creeping up) than the default 30. If the cluster
has already had incidents in the last hour (earlier tests), the default window
may not have the 20 clean points it needs after they are cut out — use a wider
one, e.g. `--baseline-start 120 --baseline-end 30`. The full evaluation was run
with `--loop --interval 60 --refit-every 10 --no-rca --baseline-start 120
--baseline-end 30`. If you must restart the pipeline mid-experiment, do it in a
cool-down gap (`run_all.py` prints `Cool-down: … (until HH:MM:SS UTC)`); its
first cycle re-scans the recent window, so nothing is lost.

## 8. Chaos and evaluation

Single scenarios (see `chaos/README.md` for what each one does and expects):

```powershell
python chaos/cpu_exhaustion.py
python chaos/oom_killed.py
python chaos/crashloop.py
python chaos/failed_rollout.py
python chaos/node_pressure.py
python chaos/db_failure.py
python ml-engine/traffic_generator.py      # error spike (validation row)
```

Full experiment (~3.8 h for 3 rounds of all 7), then the scores:

```powershell
$start = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
powershell -File .\scripts\sample-pipeline.ps1   # optional, own terminal: pipeline CPU/memory -> results.md
python chaos/run_all.py                    # --repeats 1 --gap 4 for a quick one
python eval/rca_backfill.py --since $start # only if the pipeline ran with --no-rca
                                           # add --priority-since <time> to diagnose later runs first
python eval/evaluate.py --since $start     # -> eval/results.md, results.json, f1_by_scenario.png
```

Grade the RCA answers by hand in `eval/rca_labels.csv` (`correct` = y/n) and
run `python eval/evaluate.py --since $start` again to include RCA accuracy.

The cluster clock can run ~20 s behind Windows (seen with WSL 2); the
evaluation's 3-minute grace window absorbs that.

## 9. Tests

```powershell
cd ml-engine; python -m pytest -q
```

No cluster or Gemini needed; the MongoDB tests use throwaway databases and are
skipped if MongoDB is down.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Can't fit a baseline: only N clean data points` | Not enough history yet: wait, or use `--baseline-start 15 --baseline-end 3`. |
| Target `flask-service` missing in Prometheus | `servicemonitor.yaml` label must match `helm list -A` release name (`monitoring`). |
| `ErrImageNeverPull` for flask-demo | Image not on that node: `minikube image build -t flask-demo . --all`. |
| `node_pressure.py` refuses to run | Run step 6 again (minikube restarts reset the kubelet config). |
| Loki returns lines from the wrong pods | Upgrade Promtail with this repo's `promtail-values.yaml` (`helm upgrade promtail grafana/promtail -f promtail-values.yaml`). |
| RCA warnings `429` / quota | Free tier is 20 requests/day/model: `pipeline.py --no-rca`, or another `GEMINI_MODEL` in `.env` (e.g. `gemini-flash-lite-latest`). |
| `docker pull` inside minikube fails with "server gave HTTP response to HTTPS client" | Transient Docker Hub/network issue seen on this laptop; retry. The kubelet retries pulls itself. |
| `pip install` fails with `IncompleteRead` / `Connection broken` | Network or corrupt cache: `pip install --no-cache-dir -r ml-engine\requirements.txt`. |
| `alertmanager-…-0` stuck in `Init:0/1` | Not used by this project; safe to ignore. |
