"""
k8s_signals.py

Kubernetes STATE signals from kube-state-metrics, alongside the
cpu / memory / error-rate usage metrics in prometheus_source.py.

Isolation Forest only sees resource usage, so it can miss failures that
are obvious from cluster state: a pod OOMKilled and restarted, a container
stuck in CrashLoopBackOff or ImagePullBackOff, a deployment with
unavailable replicas. This module:

  1. fetches those signals for a time window      -> fetch_k8s_signals()
  2. attaches them to ML incidents close in time,
     and turns the rest into rule-based incidents  -> attach_k8s_signals()

so a failure is reported (and diagnosed) even when the ML model flags nothing.

Scoped to the flask app and the chaos/ workloads by default -- other pods in
the namespace (e.g. monitoring) are ignored so their problems don't raise
incidents. Override with the K8S_POD_REGEX / K8S_DEPLOYMENT_REGEX env vars.
"""

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from correlation_engine import CorrelationEngine, Incident

NAMESPACE = os.environ.get("PROM_NAMESPACE", "default")
K8S_POD_REGEX = os.environ.get("K8S_POD_REGEX", "flask-deployment-.*|chaos-.*")
K8S_DEPLOYMENT_REGEX = os.environ.get("K8S_DEPLOYMENT_REGEX", "flask-deployment|chaos-.*")

BAD_WAITING_REASONS = ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"]
HIGH_SEVERITY_REASONS = {"OOMKilled", *BAD_WAITING_REASONS}

# A normal rollout restart, scale-up or node reboot can leave a deployment
# briefly short of ready replicas. Unavailable replicas only count as a
# failure once they persist for this many consecutive samples (~90s at a
# 30s step). Waiting reasons and restarts above are never normal, so they
# count immediately.
MIN_UNAVAILABLE_SAMPLES = 3

# --- PromQL queries -----------------------------------------------------
_POD_SELECTOR = f'namespace="{NAMESPACE}", pod=~"{K8S_POD_REGEX}"'

RESTARTS_QUERY = f"kube_pod_container_status_restarts_total{{{_POD_SELECTOR}}}"

# last_terminated_reason is sticky -- it keeps reporting the reason of the
# LAST termination forever (e.g. "Error" from a laptop reboot days ago). So
# it's only used to label a restart that actually happened in the window,
# never as a signal on its own.
TERMINATED_REASON_QUERY = f"kube_pod_container_status_last_terminated_reason{{{_POD_SELECTOR}}} == 1"

WAITING_REASON_QUERY = (
    f"kube_pod_container_status_waiting_reason{{{_POD_SELECTOR}, "
    f'reason=~"{"|".join(BAD_WAITING_REASONS)}"}} == 1'
)

UNAVAILABLE_QUERY = (
    f'kube_deployment_status_replicas_unavailable{{namespace="{NAMESPACE}", '
    f'deployment=~"{K8S_DEPLOYMENT_REGEX}"}} > 0'
)


@dataclass
class K8sSignal:
    """One observation of something wrong in cluster state."""
    timestamp: pd.Timestamp
    kind: str      # "restart" | "waiting" | "replicas_unavailable"
    target: str    # "pod/container" or "deployment"
    reason: str    # e.g. OOMKilled, Error, CrashLoopBackOff, ReplicasUnavailable
    value: float   # restarts added (restart), unavailable replicas, or 1 (waiting)


def _ts(unix_ts) -> pd.Timestamp:
    # naive UTC, matching prometheus_source.py's timestamps
    return pd.Timestamp(float(unix_ts), unit="s")


def _reason_at(reasons: list[tuple[pd.Timestamp, str]], t: pd.Timestamp, step: pd.Timedelta) -> str:
    """Termination reason reported at (or just after) a restart at time t."""
    candidates = [(ts, reason) for ts, reason in reasons if ts <= t + step]
    return max(candidates)[1] if candidates else "unknown"


def _consecutive_runs(values: list, step: pd.Timedelta) -> list[list[tuple[pd.Timestamp, float]]]:
    """
    Splits a range-query series into runs of back-to-back samples. The
    queries filter with `> 0` / `== 1`, so a gap in timestamps means the
    condition cleared in between.
    """
    runs: list[list[tuple[pd.Timestamp, float]]] = []
    for ts, value in values:
        t = _ts(ts)
        if runs and t - runs[-1][-1][0] <= step:
            runs[-1].append((t, float(value)))
        else:
            runs.append([(t, float(value))])
    return runs


def fetch_k8s_signals(client, minutes_back: int, since=None, step: str = "30s") -> list[K8sSignal]:
    """
    Queries kube-state-metrics over the last `minutes_back` minutes. `client`
    is a prometheus_source.PrometheusClient. Only signals after `since` are
    returned, mirroring how pipeline.py filters the metrics DataFrame.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes_back)
    step_td = pd.Timedelta(step)
    # one extra step back so a restart right at the window start still has a "before" sample
    lookback_start = start - step_td.to_pytimedelta()

    signals: list[K8sSignal] = []

    reasons: dict[tuple, list] = {}
    for series in client.query_range(TERMINATED_REASON_QUERY, lookback_start, end, step=step):
        m = series["metric"]
        key = (m.get("pod"), m.get("container"))
        for ts, _ in series["values"]:
            reasons.setdefault(key, []).append((_ts(ts), m.get("reason", "unknown")))

    for series in client.query_range(RESTARTS_QUERY, lookback_start, end, step=step):
        m = series["metric"]
        key = (m.get("pod"), m.get("container"))
        values = series["values"]
        for (_, prev), (ts, cur) in zip(values, values[1:]):
            added = float(cur) - float(prev)
            if added > 0:
                t = _ts(ts)
                signals.append(K8sSignal(
                    t, "restart", f"{key[0]}/{key[1]}",
                    _reason_at(reasons.get(key, []), t, step_td), added,
                ))

    for series in client.query_range(WAITING_REASON_QUERY, start, end, step=step):
        m = series["metric"]
        for ts, _ in series["values"]:
            signals.append(K8sSignal(
                _ts(ts), "waiting", f"{m.get('pod')}/{m.get('container')}", m.get("reason", "unknown"), 1,
            ))

    # Look back far enough that a run already underway at the window start can still reach the minimum.
    unavailable_start = start - (step_td * (MIN_UNAVAILABLE_SAMPLES - 1)).to_pytimedelta()
    window_start = _ts(start.timestamp())
    for series in client.query_range(UNAVAILABLE_QUERY, unavailable_start, end, step=step):
        m = series["metric"]
        for run in _consecutive_runs(series["values"], step_td):
            if len(run) < MIN_UNAVAILABLE_SAMPLES:
                continue  # transient, e.g. a normal rollout restart
            for t, value in run:
                if t >= window_start:
                    signals.append(K8sSignal(
                        t, "replicas_unavailable", m.get("deployment", "unknown"), "ReplicasUnavailable", value,
                    ))

    if since is not None:
        signals = [s for s in signals if s.timestamp > since]
    return sorted(signals, key=lambda s: s.timestamp)


def summarize_signals(signals: list[K8sSignal]) -> list[dict]:
    """
    Collapses raw samples (one every 30s) into one entry per
    (kind, target, reason) -- the compact form stored on the incident.
    `value` is total restarts for kind=restart, otherwise the peak value.
    """
    groups: dict[tuple, dict] = {}
    for s in sorted(signals, key=lambda s: s.timestamp):
        g = groups.setdefault((s.kind, s.target, s.reason), {
            "kind": s.kind, "target": s.target, "reason": s.reason,
            "first_seen": s.timestamp, "last_seen": s.timestamp, "value": 0,
        })
        g["last_seen"] = s.timestamp
        g["value"] = g["value"] + s.value if s.kind == "restart" else max(g["value"], s.value)

    return [
        {**g, "first_seen": str(g["first_seen"]), "last_seen": str(g["last_seen"])}
        for g in groups.values()
    ]


def signal_severity(summary: dict) -> str:
    if summary["reason"] in HIGH_SEVERITY_REASONS or summary["kind"] == "replicas_unavailable":
        return "high"
    return "medium"


def describe_signal(summary: dict) -> str:
    """Plain-English line for one summarized signal, used in the Gemini prompt."""
    window = f"({summary['first_seen']} to {summary['last_seen']})"
    if summary["kind"] == "restart":
        n = int(summary["value"])
        return (f"container {summary['target']} restarted {n} time{'s' if n != 1 else ''}, "
                f"last terminated reason: {summary['reason']} {window}")
    if summary["kind"] == "waiting":
        return f"container {summary['target']} stuck waiting in {summary['reason']} {window}"
    return (f"deployment {summary['target']} had up to {int(summary['value'])} "
            f"unavailable replica(s) {window}")


def _max_severity(*severities: str) -> str:
    return max(severities, key=lambda s: CorrelationEngine._SEVERITY_RANK[s])


def attach_k8s_signals(incidents: list[Incident], signals: list[K8sSignal],
                       window: timedelta) -> list[Incident]:
    """
    Attaches each signal to the ML incident it falls within `window` of
    (modifies those incidents in place), then groups the leftover signals
    by time proximity into rule-based incidents, which are returned.
    """
    matched: dict[str, list[K8sSignal]] = {}
    leftover: list[K8sSignal] = []
    for s in signals:
        match = next(
            (i for i in incidents if i.start_time - window <= s.timestamp <= i.end_time + window),
            None,
        )
        if match is None:
            leftover.append(s)
        else:
            matched.setdefault(match.incident_id, []).append(s)

    for incident in incidents:
        if incident.incident_id in matched:
            incident.k8s_signals = summarize_signals(matched[incident.incident_id])
            incident.detection = "ml+rule"
            incident.max_severity = _max_severity(
                incident.max_severity, *(signal_severity(s) for s in incident.k8s_signals)
            )

    groups: list[list[K8sSignal]] = []
    for s in leftover:  # already sorted by time
        if groups and s.timestamp - groups[-1][-1].timestamp <= window:
            groups[-1].append(s)
        else:
            groups.append([s])

    rule_incidents = []
    for group in groups:
        summary = summarize_signals(group)
        rule_incidents.append(Incident(
            incident_id=str(uuid.uuid4())[:8],
            start_time=group[0].timestamp,
            end_time=group[-1].timestamp,
            anomalies=[],
            max_severity=_max_severity(*(signal_severity(s) for s in summary)),
            avg_confidence=1.0,  # rule match on observed cluster state, not a statistical guess
            detection="rule",
            k8s_signals=summary,
        ))
    return rule_incidents


if __name__ == "__main__":
    # Quick live check: print whatever signals exist in the last 30 minutes.
    from prometheus_source import PrometheusClient

    found = fetch_k8s_signals(PrometheusClient(), minutes_back=30)
    print(f"{len(found)} raw signal samples for pods matching '{K8S_POD_REGEX}'")
    for summary in summarize_signals(found):
        print(f"  [{signal_severity(summary)}] {describe_signal(summary)}")
