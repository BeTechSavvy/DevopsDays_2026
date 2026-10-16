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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from correlation_engine import CorrelationEngine, Incident, make_incident_id, workload_from_target

NAMESPACE = os.environ.get("PROM_NAMESPACE", "default")
K8S_POD_REGEX = os.environ.get("K8S_POD_REGEX", "flask-deployment-.*|chaos-.*|demo-.*")
K8S_DEPLOYMENT_REGEX = os.environ.get("K8S_DEPLOYMENT_REGEX", "flask-deployment|chaos-.*|demo-.*")

BAD_WAITING_REASONS = ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"]
NODE_CONDITIONS = ["MemoryPressure", "DiskPressure", "PIDPressure"]
HIGH_SEVERITY_REASONS = {"OOMKilled", *BAD_WAITING_REASONS, *NODE_CONDITIONS, "Evicted"}

# A normal rollout restart, scale-up or node reboot can leave a deployment
# briefly short of ready replicas. Unavailable replicas only count as a
# failure once they persist for this many consecutive samples (~90s at a
# 30s step). Waiting reasons and restarts above are never normal, so they
# count immediately.
MIN_UNAVAILABLE_SAMPLES = 3

# --- PromQL queries -----------------------------------------------------
_POD_SELECTOR = f'namespace="{NAMESPACE}", pod=~"{K8S_POD_REGEX}"'

RESTARTS_QUERY = f"kube_pod_container_status_restarts_total{{{_POD_SELECTOR}}}"
CREATED_QUERY = f"kube_pod_created{{{_POD_SELECTOR}}}"  # value = pod creation time, unix seconds

# last_terminated_reason is sticky -- it keeps reporting the reason of the
# LAST termination forever (e.g. "Error" from a laptop reboot days ago). So
# it's only used to label a restart that actually happened in the window,
# never as a signal on its own.
TERMINATED_REASON_QUERY = f"kube_pod_container_status_last_terminated_reason{{{_POD_SELECTOR}}} == 1"

WAITING_REASON_QUERY = (
    f"kube_pod_container_status_waiting_reason{{{_POD_SELECTOR}, "
    f'reason=~"{"|".join(BAD_WAITING_REASONS)}"}} == 1'
)

# A container that is stopped RIGHT NOW because it crashed. Not sticky like
# last_terminated_reason. Needed because a container that dies within
# milliseconds spends most of its back-off in terminated/Error, and the
# CrashLoopBackOff waiting state can fall between two 30s samples.
CRASH_TERMINATED_REASONS = ["Error", "OOMKilled", "ContainerCannotRun", "StartError"]
TERMINATED_NOW_QUERY = (
    f"kube_pod_container_status_terminated_reason{{{_POD_SELECTOR}, "
    f'reason=~"{"|".join(CRASH_TERMINATED_REASONS)}"}} == 1'
)

UNAVAILABLE_QUERY = (
    f'kube_deployment_status_replicas_unavailable{{namespace="{NAMESPACE}", '
    f'deployment=~"{K8S_DEPLOYMENT_REGEX}"}} > 0'
)

# Node-level pressure, any node: the kubelet sets these conditions when it
# starts evicting (memory/disk/PIDs below its eviction thresholds).
NODE_PRESSURE_QUERY = (
    f'kube_node_status_condition{{condition=~"{"|".join(NODE_CONDITIONS)}", status="true"}} == 1'
)

# Pods the kubelet evicted (the pod object stays, phase Failed, reason Evicted)
EVICTED_QUERY = f'kube_pod_status_reason{{{_POD_SELECTOR}, reason="Evicted"}} == 1'

# FailedScheduling: no node can take the pod (e.g. the only matching node is tainted by pressure)
UNSCHEDULABLE_QUERY = f"kube_pod_status_unschedulable{{{_POD_SELECTOR}}} == 1"

# Pending for a long time for any other reason. Pods are Pending for a few
# seconds on every start, so like unavailable replicas this needs a run.
PENDING_QUERY = f'kube_pod_status_phase{{{_POD_SELECTOR}, phase="Pending"}} == 1'
MIN_PENDING_SAMPLES = 4

PREDICTED = "predicted_exhaustion"  # signals from predictor.py


@dataclass
class K8sSignal:
    """One observation of something wrong in cluster state."""
    timestamp: pd.Timestamp
    kind: str      # "restart" | "waiting" | "terminated" | "replicas_unavailable"
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

    created = {  # pod -> creation time (unix seconds)
        s["metric"].get("pod"): float(s["values"][-1][1])
        for s in client.query_range(CREATED_QUERY, lookback_start, end, step=step) if s["values"]
    }

    for series in client.query_range(RESTARTS_QUERY, lookback_start, end, step=step):
        m = series["metric"]
        key = (m.get("pod"), m.get("container"))
        values = series["values"]
        # A pod CREATED inside the window whose restarts are already > 0 at its
        # first sample crashed before kube-state-metrics first saw it -- there's
        # no "before" sample to diff against, so count those restarts there.
        first_t, first_v = _ts(values[0][0]), float(values[0][1])
        if first_v > 0 and created.get(key[0], 0) >= lookback_start.timestamp():
            signals.append(K8sSignal(
                first_t, "restart", f"{key[0]}/{key[1]}",
                _reason_at(reasons.get(key, []), first_t, step_td), first_v,
            ))
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

    for series in client.query_range(TERMINATED_NOW_QUERY, start, end, step=step):
        m = series["metric"]
        for ts, _ in series["values"]:
            signals.append(K8sSignal(
                _ts(ts), "terminated", f"{m.get('pod')}/{m.get('container')}", m.get("reason", "unknown"), 1,
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

    signals += _fetch_node_and_scheduling(client, start, end, step, step_td, window_start)

    if since is not None:
        signals = [s for s in signals if s.timestamp > since]
    return sorted(signals, key=lambda s: s.timestamp)


def _fetch_node_and_scheduling(client, start, end, step, step_td, window_start) -> list[K8sSignal]:
    """Node pressure conditions, evicted pods, unschedulable and long-Pending pods."""
    signals = []
    for series in client.query_range(NODE_PRESSURE_QUERY, start, end, step=step):
        m = series["metric"]
        for ts, _ in series["values"]:
            signals.append(K8sSignal(_ts(ts), "node_pressure", m.get("node", "unknown"), m.get("condition", "unknown"), 1))

    for query, kind, reason in ((EVICTED_QUERY, "evicted", "Evicted"),
                                (UNSCHEDULABLE_QUERY, "unschedulable", "FailedScheduling")):
        for series in client.query_range(query, start, end, step=step):
            pod = series["metric"].get("pod", "unknown")
            for ts, _ in series["values"]:
                signals.append(K8sSignal(_ts(ts), kind, f"{pod}/pod", reason, 1))

    pending_start = start - (step_td * (MIN_PENDING_SAMPLES - 1)).to_pytimedelta()
    for series in client.query_range(PENDING_QUERY, pending_start, end, step=step):
        pod = series["metric"].get("pod", "unknown")
        for run in _consecutive_runs(series["values"], step_td):
            if len(run) >= MIN_PENDING_SAMPLES:
                signals += [K8sSignal(t, "pending", f"{pod}/pod", "Pending", 1) for t, _ in run if t >= window_start]
    return signals


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
        if s.kind == "restart":
            g["value"] += s.value
        elif s.kind == PREDICTED:
            # value = predicted seconds to breach; the most urgent warning counts
            g["value"] = s.value if g["value"] == 0 else min(g["value"], s.value)
            g["time_to_breach"] = g["value"]
        else:
            g["value"] = max(g["value"], s.value)

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
    if summary["kind"] == "terminated":
        return f"container {summary['target']} stopped after a crash (terminated: {summary['reason']}) {window}"
    if summary["kind"] == PREDICTED:
        resource = "memory" if summary["reason"].startswith("Memory") else "CPU"
        return (f"PREDICTION: {resource} of pod {summary['target'].split('/')[0]} trending to 100% of its "
                f"limit in ~{int(summary['value'])}s (least-squares trend) {window}")
    if summary["kind"] == "node_pressure":
        return f"node {summary['target']} reported {summary['reason']} {window}"
    if summary["kind"] == "evicted":
        return f"pod {summary['target'].split('/')[0]} was evicted by the kubelet {window}"
    if summary["kind"] in ("unschedulable", "pending"):
        return f"pod {summary['target'].split('/')[0]} stuck {summary['reason']} (not scheduled/started) {window}"
    return (f"deployment {summary['target']} had up to {int(summary['value'])} "
            f"unavailable replica(s) {window}")


def _max_severity(*severities: str) -> str:
    return max(severities, key=lambda s: CorrelationEngine._SEVERITY_RANK[s])


def attach_k8s_signals(incidents: list[Incident], signals: list[K8sSignal],
                       window: timedelta) -> list[Incident]:
    """
    Attaches each signal to the ML incident of the same workload it falls
    within `window` of (modifies those incidents in place), then groups the
    leftover signals by workload and time proximity into rule-based
    incidents, which are returned.
    """
    matched: dict[str, list[K8sSignal]] = {}
    leftover: list[K8sSignal] = []
    for s in signals:
        match = next(
            (i for i in incidents
             if i.workload == workload_from_target(s.target)
             and i.start_time - window <= s.timestamp <= i.end_time + window),
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

    # one failing workload per incident, so two chaos scenarios that overlap
    # in time are still reported (and diagnosed) separately
    groups: list[tuple[str, list[K8sSignal]]] = []
    open_group: dict[str, list[K8sSignal]] = {}
    for s in leftover:  # already sorted by time
        workload = workload_from_target(s.target)
        group = open_group.get(workload)
        if group is not None and s.timestamp - group[-1].timestamp <= window:
            group.append(s)
        else:
            open_group[workload] = [s]
            groups.append((workload, open_group[workload]))

    rule_incidents = []
    for workload, group in groups:
        summary = summarize_signals(group)
        rule_incidents.append(Incident(
            incident_id=make_incident_id(workload, group[0].timestamp),
            workload=workload,
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
