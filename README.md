# AI-Based Root Cause Analysis and Failure Prediction for Kubernetes

*(Chaos Healer)* — detects failures in a Kubernetes cluster from metrics,
cluster state, logs and events; predicts resource exhaustion before it
happens; explains each incident with an LLM grounded in past incidents; and
recommends what would have prevented it. Advisory only: nothing is changed
automatically.

- **Set up and run it:** [RUNBOOK.md](RUNBOOK.md) (fresh machine → running demo, exact PowerShell commands)
- **Present it:** [DEMO_SCRIPT.md](DEMO_SCRIPT.md) (10-minute demo)
- **Break it on purpose:** [chaos/README.md](chaos/README.md) (six failure scenarios)
- **Results:** [eval/results.md](eval/results.md)

## How it works

```
Prometheus ──► cpu / memory / error rate per workload ──► Isolation Forest (fitted on an earlier, clean window)
     │                                                          │
     ├──► kube-state-metrics rules: OOMKilled, CrashLoopBackOff,│
     │    ImagePullBackOff, unavailable replicas, node pressure,├──► correlation: one incident per workload
     │    evictions, unschedulable pods                         │    and time window ──► MongoDB
     └──► predictor: least-squares trend → "100% in N s"  ──────┘                          │
                                                                                            ▼
Loki error lines + Kubernetes events ──► evidence ──► Gemini RCA + ChromaDB similar past incidents
                                                       + rule-based counterfactual ──► API ──► React dashboard
```

| Module (`ml-engine/`) | Role |
|---|---|
| `prometheus_source.py` | Pulls cpu/memory (as % of limits) and error rate from Prometheus |
| `anomaly_detector.py` | Isolation Forest + hard metric limits for severity |
| `k8s_signals.py` | Kubernetes state rules (restarts, waiting reasons, replicas, node pressure, evictions, scheduling) |
| `predictor.py` | Failure prediction: trend of the last 10 samples, warns when the limit is ≤ 10 min away |
| `correlation_engine.py` | Groups anomalies into incidents; deterministic incident ids |
| `incident_store.py` | MongoDB storage, merging of overlapping incidents per workload |
| `evidence.py` | Loki error lines and K8s events for the incident's pods |
| `rca_engine.py` | Gemini diagnosis grounded in ChromaDB history and the evidence |
| `counterfactual.py` | "What would have prevented this", filled with observed values |
| `failure_kinds.py` | Shared failure-kind vocabulary (detection, evaluation, recommendations) |
| `pipeline.py` | The loop that ties it together (`--loop`, `--no-rca`) |
| `api.py` | REST API for the dashboard |

Other folders: `dashboard/` (React UI), `chaos/` (failure scenarios + ground-truth
log), `eval/` (evaluation harness), `demo-db/` (Flask + Redis service for the DB
scenario). The monitored demo app is `app.py` (`/work`, `/cpu-heavy`, …).

## Components

Flask application · Docker · Kubernetes (Minikube, 2 nodes) · Prometheus +
Grafana (kube-prometheus-stack, Helm release `monitoring`) · Loki + Promtail ·
MongoDB · ChromaDB · Gemini (`langchain-google-genai`) · React (Vite).

## Quick reference

```powershell
minikube image build -t flask-demo . --all        # the app image is "flask-demo" everywhere
kubectl apply -f deployment.yaml -f service.yaml -f servicemonitor.yaml
powershell -ExecutionPolicy Bypass -File .\port-forwards.ps1
cd ml-engine; python pipeline.py --loop
cd ml-engine; python -m pytest -q
```

Configuration lives in `ml-engine/.env` (template: `ml-engine/.env.example`):
`GOOGLE_API_KEY`, `GEMINI_MODEL`, `MONGO_URI`, `API_PORT`, `PROMETHEUS_URL`,
`LOKI_URL`, `FLASK_URL`.
