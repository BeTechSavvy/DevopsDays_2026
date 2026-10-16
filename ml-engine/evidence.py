"""
evidence.py

Log and event EVIDENCE for an incident, gathered just before diagnosis:

  1. Loki: error-pattern log lines from the incident workload's pods in
     the incident window (query_range), de-duplicated, at most 8 lines.
  2. Kubernetes events (Warning/Normal) for the same pods, ReplicaSets and
     Deployment, from the API server (kubectl's kubeconfig), e.g.
     "Back-off restarting failed container", "Failed to pull image ...",
     "Evicted: The node was low on resource: memory".

Both are stored on the incident (`evidence`) and included in the Gemini
prompt, so the diagnosis can quote what the cluster actually said instead
of inferring from metrics. Best-effort: if Loki or the API server is
unreachable, that part is simply empty.
"""

import re
from datetime import timedelta

import pandas as pd
import requests

from config import LOKI_URL
from k8s_signals import NAMESPACE

MAX_LOG_LINES = 8
MAX_EVENTS = 8
MARGIN = timedelta(minutes=2)  # look a little before/after the incident window
# `" 5\d\d ` is an HTTP 5xx in an access-log line (`"GET /work HTTP/1.1" 500 -`)
ERROR_PATTERN = r'(?i)(error|exception|fatal|fail|panic|traceback|killed|refused|timed? ?out|unavailable|denied|" 5\d\d )'

_VOLATILE = re.compile(r"\d+(\.\d+)*|0x[0-9a-f]+|[0-9a-f]{8,}", re.IGNORECASE)
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _ns(t: pd.Timestamp) -> int:
    return int(pd.Timestamp(t).value)  # naive UTC -> ns since epoch


def _normalize(line: str) -> str:
    """Line with numbers/ids/timestamps blanked, so repeats of the same message dedupe."""
    return _VOLATILE.sub("#", line.strip())[:200]


def dedupe(lines: list[str], limit: int = MAX_LOG_LINES) -> list[str]:
    seen, out = set(), []
    for line in lines:
        line = _ANSI.sub("", line)  # werkzeug colours its access log
        key = _normalize(line)
        if line.strip() and key not in seen:
            seen.add(key)
            out.append(line.strip()[:300])
            if len(out) == limit:
                break
    return out


def fetch_logs(workload: str, start, end, base_url: str = LOKI_URL, timeout: int = 10) -> list[str]:
    """Error-pattern lines from the workload's pods, oldest first, de-duplicated."""
    # workload names are DNS-1123 ([a-z0-9-]), safe inside the regex as-is
    query = f'{{namespace="{NAMESPACE}", pod=~"{workload}-.*"}} |~ `{ERROR_PATTERN}`'
    resp = requests.get(f"{base_url.rstrip('/')}/loki/api/v1/query_range", params={
        "query": query, "start": _ns(pd.Timestamp(start) - MARGIN), "end": _ns(pd.Timestamp(end) + MARGIN),
        "limit": 500, "direction": "forward",
    }, timeout=timeout)
    resp.raise_for_status()
    entries = [(int(ts), line) for stream in resp.json()["data"]["result"] for ts, line in stream["values"]]
    return dedupe([line for _, line in sorted(entries)])


def fetch_events(workload: str, start, end) -> list[str]:
    """K8s events whose object name starts with the workload name, in the window. Warnings first."""
    from kubernetes import client, config

    config.load_kube_config()
    lo, hi = pd.Timestamp(start) - MARGIN, pd.Timestamp(end) + MARGIN
    events = []
    for e in client.CoreV1Api().list_namespaced_event(NAMESPACE).items:
        name = e.involved_object.name or ""
        if not (name == workload or name.startswith(f"{workload}-")):
            continue
        when = e.last_timestamp or e.event_time or e.first_timestamp
        if when is None:
            continue
        when = pd.Timestamp(when).tz_convert("UTC").tz_localize(None)
        if lo <= when <= hi:
            count = f" (x{e.count})" if e.count and e.count > 1 else ""
            events.append((e.type, when,
                           f"{e.type} {e.reason} {e.involved_object.kind}/{name}: {(e.message or '').strip()}{count}"))
    return order_events(events)


def order_events(events: list[tuple], limit: int = MAX_EVENTS) -> list[str]:
    """
    (type, time, text) -> texts with Warnings first (Failed, BackOff, Evicted,
    OOMKilling...), then routine Normal events, each oldest first; de-duplicated.
    """
    ranked = sorted(events, key=lambda e: (e[0] != "Warning", e[1]))
    return dedupe([text for _, _, text in ranked], limit)


def gather(doc: dict) -> dict:
    """{"logs": [...], "events": [...], "errors": [...]} for an incident document."""
    workload = doc.get("workload", "flask-deployment")
    evidence = {"logs": [], "events": [], "errors": []}
    for key, fetch in (("logs", fetch_logs), ("events", fetch_events)):
        try:
            evidence[key] = fetch(workload, doc["start_time"], doc["end_time"])
        except Exception as e:
            evidence["errors"].append(f"{key}: {type(e).__name__}: {str(e)[:150]}")
    return evidence


def describe(evidence: dict) -> str:
    """Prompt block for rca_engine."""
    parts = []
    if evidence.get("events"):
        parts.append("Kubernetes events:\n" + "\n".join(f"  - {e}" for e in evidence["events"]))
    if evidence.get("logs"):
        parts.append("Error lines from the pods' logs (Loki, de-duplicated):\n"
                     + "\n".join(f"  - {line}" for line in evidence["logs"]))
    return "\n".join(parts)


if __name__ == "__main__":
    import json
    import sys

    workload = sys.argv[1] if len(sys.argv) > 1 else "flask-deployment"
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    print(json.dumps(gather({"workload": workload, "start_time": str(now - timedelta(minutes=10)),
                             "end_time": str(now)}), indent=2))
