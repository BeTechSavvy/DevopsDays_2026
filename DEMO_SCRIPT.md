# DEMO SCRIPT — 10 minutes

## Quick start

```powershell
.\start-all.ps1            # brings everything up and opens the dashboard
.\status.ps1               # all green? (endpoints, processes, ≥ 40 min history)
.\stop-all.ps1             # afterwards
```

Run `.\start-all.ps1 -SetupNodePressure` instead if you'll show node pressure
after a minikube restart. See RUNBOOK.md for first-time setup.

**Before the audience arrives (≥ 40 min ahead):** follow RUNBOOK.md steps 2–7 so
the cluster, port-forwards, baseline traffic, `pipeline.py --loop`, `api.py` and
the dashboard are all running, and the pipeline has printed `Polling every 60s`.
Run `chaos\setup-node-pressure.ps1` if you'll show node pressure. Open three
windows side by side: the **dashboard** (http://localhost:5173), the **pipeline
terminal**, and a **chaos terminal** at the repo root.

**Gemini:** the free tier is 20 requests/day per model and a demo uses ~5–10
(one per new incident, plus re-diagnoses). Don't spend the day's quota on
testing beforehand; if needed, point `GEMINI_MODEL` in `ml-engine\.env` at a
model whose quota is unused (e.g. `gemini-flash-lite-latest`).

Kick off the OOM scenario about **4 minutes before** you start talking — its
leak takes ~4 minutes to hit the limit, and you want the prediction to land
during part 3:

```powershell
python chaos/oom_killed.py
```

---

### 0:00 – 1:30 · The problem and the architecture

- Kubernetes failures show up in three places at once: metrics, cluster state,
  logs/events. An on-call engineer has to correlate them by hand.
- Walk the pipeline left to right: **Prometheus** (cpu/memory/error rate per
  workload) → **Isolation Forest** fitted on an *earlier, clean* window →
  **kube-state-metrics rules** (OOMKilled, CrashLoopBackOff, ImagePullBackOff,
  node pressure, evictions) → **correlation** into one incident per workload →
  **MongoDB** → **RCA** with Gemini + ChromaDB history, grounded in **Loki logs
  and K8s events** → dashboard. Advisory only — nothing auto-remediates.

### 1:30 – 3:00 · Normal operation

- Dashboard: point at the **Connected** pill and the empty/quiet list.
- Pipeline terminal: `N points scored, no anomalies` — normal /work traffic
  already fails ~10% of the time, and the model has learned that is normal.
- Mention the separate baseline: it never scores the data it learned from, and
  it refuses to re-learn from a window containing an incident.

### 3:00 – 5:00 · Failure *prediction* (OOM scenario started earlier)

- Pipeline: a **predicted_exhaustion** signal appears for `chaos-oom` —
  "memory trending to 100% of its limit in ~Ns".
- Dashboard: the incident shows the amber **Early warning** box with the
  time to breach. Say: *this is before anything has failed.*
- Within a minute or two the pod is **OOMKilled**: the same incident turns
  HIGH, gains the OOMKilled restart signal, and gets re-diagnosed.
- Open the detail panel:
  - **Likely root cause** names the container and OOMKilled.
  - **What would have prevented this**: raise the limit to the observed peak
    +25% (with the number) — or fix the leak, because usage climbed steadily.
  - **Evidence**: the K8s events, Warnings first (`BackOff` restarting the
    failed container), and the pod's last state `OOMKilled`.

### 5:00 – 7:00 · A second, different failure: bad rollout

```powershell
python chaos/failed_rollout.py --minutes 3
```

- Within 1–2 polls: a HIGH **Rule** incident for `chaos-rollout`, signal
  `ErrImagePull / ImagePullBackOff`.
- Point out what the ML model alone could not see (no metric changed — the
  old pods kept serving), which is why the rules exist.
- Counterfactual: validate image tags in CI, `kubectl rollout undo`.
- Evidence: `Failed to pull image "flask-demo:v2-does-not-exist"`.

*Swap option (if asked about logs):* `python chaos/db_failure.py` instead of
the rollout. Redis goes away, `demo-db-app`'s error rate jumps, and the Loki
**Evidence** shows `database error: cannot reach redis`, so the incident is
classified `db_connection`, not just "error spike". It recovers when the script
scales Redis back up.

### 7:00 – 8:30 · Evaluation results

Open `eval/results.md` (and `eval/f1_by_scenario.png`):

- Six failure scenarios × 3 rounds (CPU exhaustion, OOM, crash loop, failed
  rollout, node pressure on node 2, DB failure), plus the traffic generator's
  error spike as a separate validation row. Ground truth was written by the
  chaos scripts themselves (`chaos/injections.jsonl`), not by the detector.
- Read the overall **precision / recall / F1**, the **MTTD** column, the
  **RCA accuracy** (hand-graded), and Gemini calls per incident (re-diagnosis
  only when an incident gets worse or doubles in size).
- Be honest about the weakest row and why (it's in the table).

### 8:30 – 10:00 · Wrap-up and questions

- What's novel: ML + rules + logs/events in one incident; prediction ahead of
  failure; counterfactual "what would have prevented this"; RCA grounded in
  past incidents (ChromaDB).
- Limits: single-cluster demo, thresholds tuned for a laptop minikube, LLM
  output is advisory and was graded by hand.

**If something goes wrong live:** the scenario scripts always clean up on
Ctrl+C. If Gemini is rate-limited, incidents still appear with the rule-based
counterfactual — say so and move on. Backup: screenshots of a previous run's
dashboard and `eval/results.md`.
