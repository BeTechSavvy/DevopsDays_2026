"""
failure_kinds.py

Names the KIND of failure a stored incident shows, from its evidence:
Kubernetes signals (k8s_signals.py) and the metrics of its anomalies.

One shared vocabulary, so that
  - each chaos/ scenario declares the kind it should produce (expected_kind
    in chaos/injections.jsonl),
  - eval/evaluate.py checks the incident actually showed that kind, and
  - counterfactual recommendations are chosen by kind.
"""

from anomaly_detector import METRIC_HIGH_THRESHOLDS

OOM_KILLED = "OOMKilled"
CRASH_LOOP = "CrashLoopBackOff"
IMAGE_PULL = "ImagePullBackOff"
REPLICAS_UNAVAILABLE = "ReplicasUnavailable"
CPU_SATURATION = "cpu_saturation"
MEMORY_SATURATION = "memory_saturation"
ERROR_SPIKE = "error_spike"

# waiting / termination reasons -> kind
_REASON_KINDS = {
    "OOMKilled": OOM_KILLED,
    "CrashLoopBackOff": CRASH_LOOP,
    "Error": CRASH_LOOP,  # a container exiting non-zero on its own
    "ImagePullBackOff": IMAGE_PULL,
    "ErrImagePull": IMAGE_PULL,
    "CreateContainerConfigError": CRASH_LOOP,
    "ReplicasUnavailable": REPLICAS_UNAVAILABLE,
}

_METRIC_KINDS = {
    "cpu_percent": CPU_SATURATION,
    "memory_percent": MEMORY_SATURATION,
    "error_rate": ERROR_SPIKE,
}


def incident_kinds(doc: dict) -> set[str]:
    """
    Every failure kind the incident document shows evidence of. A
    signal-level kind (e.g. a "kind" stored by later detectors such as
    predicted_exhaustion or node_pressure) is used as-is when it isn't one
    of the classic restart/waiting/replicas kinds.
    """
    kinds = set()
    for s in doc.get("k8s_signals", []):
        if s["kind"] in ("restart", "waiting", "terminated", "replicas_unavailable"):
            kinds.add(_REASON_KINDS.get(s["reason"], s["reason"]))
        else:
            kinds.add(s["kind"])
    for a in doc.get("anomalies", []):
        metrics = a.get("metrics", {})
        for col, limit in METRIC_HIGH_THRESHOLDS.items():
            if metrics.get(col, 0) > limit:
                kinds.add(_METRIC_KINDS[col])
    return kinds


def primary_kind(doc: dict) -> str | None:
    """The single most specific kind, for picking a recommendation."""
    kinds = incident_kinds(doc)
    for kind in (OOM_KILLED, IMAGE_PULL, "node_pressure", "evicted", "unschedulable", "db_connection",
                 CRASH_LOOP, CPU_SATURATION, MEMORY_SATURATION, ERROR_SPIKE,
                 "predicted_exhaustion", REPLICAS_UNAVAILABLE):
        if kind in kinds:
            return kind
    return next(iter(sorted(kinds)), None)
