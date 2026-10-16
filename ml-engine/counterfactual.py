"""
counterfactual.py

Counterfactual recommendations: "what change would have prevented this
incident?" -- a rule table keyed by failure kind (failure_kinds.py),
filled in with values actually observed during the incident (limits,
peaks, restart counts, pod names). Deterministic and grounded, unlike the
LLM's free-text suggested_fix, and stored next to it on the diagnosis as
`counterfactual`.

Advisory only, like the rest of the diagnosis: nothing here changes the
cluster.
"""

import math
from datetime import datetime, timedelta, timezone

import pandas as pd

from failure_kinds import (
    CPU_SATURATION, CRASH_LOOP, ERROR_SPIKE, IMAGE_PULL, MEMORY_SATURATION, OOM_KILLED,
    REPLICAS_UNAVAILABLE, incident_kinds, primary_kind,
)

HEADROOM = 1.25  # recommend limits 25% above the observed peak


def _round_up(value: float, to: int) -> int:
    return int(math.ceil(value / to) * to)


def _signal_targets(doc: dict, kind: str | None = None) -> list[str]:
    return sorted({s["target"] for s in doc.get("k8s_signals", []) if kind is None or s["kind"] == kind})


def _peak_metric(doc: dict, metric: str) -> float | None:
    values = [a["metrics"].get(metric) for a in doc.get("anomalies", []) if a.get("metrics")]
    values = [v for v in values if v is not None]
    return max(values) if values else None


def _oom(doc, obs):
    workload = doc.get("workload", "the workload")
    limit = obs.get("memory_limit_mib")
    peak = obs.get("memory_peak_mib") or limit
    restarts = int(sum(s["value"] for s in doc.get("k8s_signals", []) if s.get("reason") == "OOMKilled"))
    if peak:
        new_limit = _round_up(peak * HEADROOM, 16)
        against = f" against a {limit:.0f}Mi limit" if limit else ""
        kills = f"{restarts} OOM kill(s)" if restarts else "OOM kill"
        what_if = (f"Had {workload}'s memory limit been {new_limit}Mi or more (observed peak {peak:.0f}Mi"
                   f"{against}, +25% headroom), the {kills} would not have happened -- unless memory keeps "
                   "growing without bound, which means a leak that a bigger limit only delays.")
    else:
        new_limit = None
        what_if = (f"Had {workload}'s memory limit been at least 25% above its real peak usage, "
                   "the OOM kill would not have happened -- unless memory grows without bound (a leak).")
    actions = [
        f"Raise resources.limits.memory to {new_limit}Mi" if new_limit else
        "Raise resources.limits.memory to the observed peak + 25%",
        "If usage climbs steadily until killed, fix the leak (profile heap growth) instead of only raising the limit",
        "Set resources.requests.memory close to normal usage so the scheduler places the pod on a node with room",
    ]
    return what_if, actions


def _cpu(doc, obs):
    workload = doc.get("workload", "the workload")
    peak = obs.get("cpu_peak_percent") or _peak_metric(doc, "cpu_percent")
    limit_m = obs.get("cpu_limit_millicores")
    if peak and limit_m:
        needed = _round_up(limit_m * peak / 100 * HEADROOM, 50)
        what_if = (f"{workload} ran at {peak:.0f}% of its {limit_m:.0f}m CPU limit. With a limit of "
                   f"{needed}m (+25% headroom), or an HPA adding replicas at 70% CPU, requests would not "
                   "have queued behind a throttled CPU.")
        first = f"Raise resources.limits.cpu to {needed}m per pod"
    else:
        what_if = (f"{workload} saturated its CPU limit{f' ({peak:.0f}%)' if peak else ''}. More CPU per pod, "
                   "or more pods via an HPA, would have absorbed the load.")
        first = "Raise resources.limits.cpu to the observed peak + 25%"
    actions = [
        first,
        f"kubectl autoscale deployment {workload} --cpu-percent=70 --min=2 --max=5 (needs metrics-server)",
        "Find what drove the load (traffic spike, hot endpoint) and rate-limit or cache it",
    ]
    return what_if, actions


def _crashloop(doc, obs):
    workload = doc.get("workload", "the workload")
    first_error = obs.get("first_error_line")
    log_hint = f' (log: "{first_error}")' if first_error else ""
    what_if = (f"{workload} exits during start-up{log_hint}. Had the start-up error been fixed "
               "(or caught by a startupProbe in a staged rollout), the pod would not be crash-looping.")
    actions = [
        "Fix the start-up error shown in the container logs (kubectl logs --previous)",
        "Add a startupProbe so a slow or broken start is reported clearly instead of a restart loop",
        "Validate required config/env vars at deploy time (e.g. in CI) rather than at container start",
    ]
    return what_if, actions


def _image_pull(doc, obs):
    workload = doc.get("workload", "the workload")
    image = obs.get("image")
    what_if = (f"The rollout of {workload} referenced an image{f' ({image})' if image else ''} that could not "
               "be pulled. Had the tag been validated before the rollout, the bad ReplicaSet would never "
               "have been created; old pods kept serving only because maxUnavailable allowed it.")
    actions = [
        f"kubectl rollout undo deployment/{workload}",
        "Validate image tags exist in the registry in CI before updating the Deployment",
        "Keep a progressDeadlineSeconds and alert on ProgressDeadlineExceeded",
    ]
    return what_if, actions


def _node_pressure(doc, obs):
    node = obs.get("node") or ", ".join(_signal_targets(doc, "node_pressure")) or "the node"
    what_if = (f"{node} ran out of resources and the kubelet evicted pods. Had every pod set requests "
               "(so the scheduler reserves room) and critical workloads had a PodDisruptionBudget, "
               "evictions would have hit only best-effort pods.")
    actions = [
        "Set resources.requests on every workload so nodes aren't overcommitted",
        "Add a PodDisruptionBudget for critical Deployments",
        "Add capacity (another node) or cap the noisy workload's limits",
    ]
    return what_if, actions


def _db(doc, obs):
    workload = doc.get("workload", "the service")
    what_if = (f"{workload} kept receiving traffic while its database was unreachable and returned 500s. "
               "A readiness probe that checks the DB would have taken it out of the Service, and retries "
               "with a circuit breaker would have turned errors into fast, explicit degradation.")
    actions = [
        "Add a readinessProbe that fails when the database is unreachable",
        "Retry DB connections with exponential backoff, behind a circuit breaker",
        "Run the database with >1 replica or a PodDisruptionBudget so it can't be scaled to zero by accident",
    ]
    return what_if, actions


def _errors(doc, obs):
    workload = doc.get("workload", "the workload")
    peak = _peak_metric(doc, "error_rate")
    what_if = (f"{workload}'s error rate peaked at {peak:.0f}%. " if peak else f"{workload}'s error rate spiked. ") + \
        "Had the failing change been caught by a canary or rolled back automatically on error rate, " \
        "users would have seen far fewer 5xx responses."
    actions = [
        "Check the logs for the failing endpoint and the most recent deploy",
        f"Roll back if a deploy preceded the spike: kubectl rollout undo deployment/{workload}",
        "Alert on error-rate SLO burn so spikes are caught in minutes",
    ]
    return what_if, actions


def _predicted(doc, obs):
    ttb = min((s["value"] for s in doc.get("k8s_signals", []) if s["kind"] == "predicted_exhaustion"), default=None)
    workload = doc.get("workload", "the workload")
    what_if = (f"{workload} was trending to its resource limit{f' within ~{int(ttb)}s' if ttb else ''}. "
               "Acting on the warning (raising the limit or scaling out) before the breach would avoid the failure.")
    return what_if, ["Raise the limit or scale out now, before the predicted breach",
                     "Investigate the steady growth (leak or load ramp)"]


RULES = {
    OOM_KILLED: _oom,
    MEMORY_SATURATION: _oom,
    CPU_SATURATION: _cpu,
    CRASH_LOOP: _crashloop,
    REPLICAS_UNAVAILABLE: _crashloop,
    IMAGE_PULL: _image_pull,
    "node_pressure": _node_pressure,
    "evicted": _node_pressure,
    "unschedulable": _node_pressure,
    "db_connection": _db,
    ERROR_SPIKE: _errors,
    "predicted_exhaustion": _predicted,
}


def recommend(doc: dict, observed: dict | None = None) -> dict | None:
    """The counterfactual for one incident document, or None if no rule applies."""
    kind = primary_kind(doc)
    rule = RULES.get(kind)
    if rule is None:
        return None
    what_if, actions = rule(doc, observed or {})
    return {"kind": kind, "all_kinds": sorted(incident_kinds(doc)), "what_if": what_if, "actions": actions}


# --- observed values from Prometheus ---------------------------------------

def fetch_observed(client, doc: dict) -> dict:
    """
    Peak memory / CPU and limits of the incident's workload pods during the
    incident (+/- 2 min), from cAdvisor and kube-state-metrics. Best-effort:
    returns whatever it could get.
    """
    from k8s_signals import NAMESPACE

    workload = doc.get("workload", "flask-deployment")
    sel = f'namespace="{NAMESPACE}", pod=~"{workload}-.*"'
    def utc(t):
        return pd.Timestamp(t).floor("s").to_pydatetime().replace(tzinfo=timezone.utc)

    start = utc(doc["start_time"]) - timedelta(minutes=2)
    end = min(utc(doc["end_time"]) + timedelta(minutes=2), datetime.now(timezone.utc))
    queries = {
        "memory_peak_mib": f"max(container_memory_working_set_bytes{{{sel}}}) / 2^20",
        "memory_limit_mib": f'max(sum by (pod) (kube_pod_container_resource_limits{{{sel}, resource="memory"}})) / 2^20',
        "cpu_limit_millicores": f'max(sum by (pod) (kube_pod_container_resource_limits{{{sel}, resource="cpu"}})) * 1000',
    }
    observed = {}
    for key, q in queries.items():
        try:
            result = client.query_range(q, start, end, step="30s")
            values = [float(v) for s in result for _, v in s["values"]]
            if values:
                observed[key] = max(values)
        except Exception:
            pass
    return observed
