"""
predictor.py

Failure PREDICTION, ahead of the failure: a least-squares trend over the
last 10 samples of each workload's cpu_percent and memory_percent (usage
as a percentage of its limit). When the trend is rising and would reach
100% of the limit within 10 minutes, it raises a warning -- before the
pod is actually OOM-killed or throttled flat.

The trend must also be close to a straight line (R^2 >= 0.8), so a pod's
start-up ramp that levels off doesn't count as a leak.

Warnings become K8sSignal(kind="predicted_exhaustion"), so they flow
through attach_k8s_signals like any other signal: attached to an incident
of the same workload, or opening a medium-severity incident of their own.
Their value is the predicted time to breach in seconds.

Lead time (how much warning the prediction gave) is measured in
eval/evaluate.py: first predicted_exhaustion warning -> first OOMKilled
restart for the same injection.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from correlation_engine import workload_from_target
from k8s_signals import K8S_POD_REGEX, NAMESPACE, K8sSignal

WINDOW_SAMPLES = 10
HORIZON_SECONDS = 600
# A pod's start-up ramp that then levels off fits a rising line badly but can
# still project a breach; a steady leak or load ramp is close to linear.
MIN_R2 = 0.8
STEP = "15s"
KIND = "predicted_exhaustion"
REASONS = {"memory_percent": "MemoryExhaustionPredicted", "cpu_percent": "CPUExhaustionPredicted"}

# Per POD, as a % of its limits -- the same idea as prometheus_source's
# cpu/memory queries, but not summed over pods, so one leaking pod stands
# out. Per pod rather than per container because minikube's cAdvisor
# reports pod-level cgroups only (no `container` label).
_SEL = f'namespace="{NAMESPACE}", pod=~"{K8S_POD_REGEX}"'
_LIMIT = 'sum by (pod) (kube_pod_container_resource_limits{{{sel}, resource="{res}"}})'
MEMORY_QUERY = (
    f"100 * max by (pod) (container_memory_working_set_bytes{{{_SEL}}}) "
    f"/ on (pod) {_LIMIT.format(sel=_SEL, res='memory')}"
)
CPU_QUERY = (
    f"100 * max by (pod) (rate(container_cpu_usage_seconds_total{{{_SEL}}}[1m])) "
    f"/ on (pod) {_LIMIT.format(sel=_SEL, res='cpu')}"
)
QUERIES = {"memory_percent": MEMORY_QUERY, "cpu_percent": CPU_QUERY}


def time_to_breach(times_s: np.ndarray, values: np.ndarray, limit: float = 100.0) -> tuple[float, float] | None:
    """
    Least-squares line through (time, value). Returns (slope per second,
    seconds from the last sample until the line reaches `limit`), or None
    if the trend isn't rising.
    """
    if len(values) < 2:
        return None
    slope, intercept = np.polyfit(times_s, values, 1)
    if slope <= 1e-9:  # flat lines fit to +/-1e-16, not exactly 0
        return None
    now_value = slope * times_s[-1] + intercept
    return float(slope), float(max(0.0, (limit - now_value) / slope))


def r_squared(times_s: np.ndarray, values: np.ndarray) -> float:
    """How well a straight line explains the samples (1 = perfectly linear)."""
    slope, intercept = np.polyfit(times_s, values, 1)
    residual = np.sum((values - (slope * times_s + intercept)) ** 2)
    total = np.sum((values - values.mean()) ** 2)
    return 1.0 - residual / total if total > 0 else 0.0


def predict(series: list[dict], metric: str, window: int = WINDOW_SAMPLES,
            horizon: float = HORIZON_SECONDS) -> list[K8sSignal]:
    """
    One warning per series (pod/container) whose last `window` samples
    trend to 100% within `horizon` seconds. `series` is a Prometheus range
    query result.
    """
    warnings = []
    for s in series:
        values = s["values"][-window:]
        if len(values) < window:
            continue  # not enough history for a trend yet (e.g. a pod that just restarted)
        t = np.array([float(ts) for ts, _ in values])
        v = np.array([float(x) for _, x in values])
        result = time_to_breach(t - t[0], v)
        if result is None or result[1] > horizon or r_squared(t - t[0], v) < MIN_R2:
            continue
        slope, ttb = result
        warnings.append(K8sSignal(
            timestamp=pd.Timestamp(t[-1], unit="s"), kind=KIND,
            target=f"{s['metric'].get('pod')}/pod", reason=REASONS[metric], value=round(ttb),
        ))
    return warnings


def fetch_predictions(client, since=None) -> list[K8sSignal]:
    """Runs both queries over just enough history for one trend window and predicts."""
    end = datetime.now(timezone.utc)
    start = end - pd.Timedelta(STEP) * (WINDOW_SAMPLES + 1)
    warnings = []
    for metric, query in QUERIES.items():
        warnings += predict(client.query_range(query, start, end, step=STEP), metric)
    if since is not None:
        warnings = [w for w in warnings if w.timestamp > since]
    return warnings


def describe(w: K8sSignal) -> str:
    resource = "memory" if w.reason.startswith("Memory") else "CPU"
    return (f"{workload_from_target(w.target)}: {resource} trending to 100% of its limit "
            f"in ~{int(w.value)}s ({w.target})")


if __name__ == "__main__":
    from prometheus_source import PrometheusClient

    found = fetch_predictions(PrometheusClient())
    print(f"{len(found)} exhaustion warning(s)")
    for w in found:
        print("  " + describe(w))
