"""
chaos/common.py

Shared helpers for the chaos scenario scripts: kubectl, UTC time, and the
ground-truth log. Every scenario appends exactly one line to
chaos/injections.jsonl when it finishes (also after Ctrl+C or an error):

    {"scenario": "oom_killed", "start": "...Z", "end": "...Z",
     "target": "chaos-oom", "expected_kind": "OOMKilled", "completed": true}

start = when the fault was injected, end = when it was removed (cleanup
done). eval/evaluate.py matches incidents against these windows.
expected_kind uses the vocabulary in ml-engine/failure_kinds.py.
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

CHAOS_DIR = Path(__file__).resolve().parent
INJECTIONS_LOG = CHAOS_DIR / "injections.jsonl"

sys.path.insert(0, str(CHAOS_DIR.parent / "ml-engine"))


def kubectl(*args, check=True) -> str:
    result = subprocess.run(["kubectl", *args], text=True, capture_output=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hms(t: datetime | None = None) -> str:
    # incidents and the dashboard use UTC, so the scripts do too
    return (t or utc_now()).strftime("%H:%M:%S")


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def log_injection(scenario: str, start: datetime, end: datetime, target: str,
                  expected_kind: str, completed: bool = True, **extra) -> None:
    """Appends one ground-truth line to chaos/injections.jsonl."""
    record = {
        "scenario": scenario, "start": iso(start), "end": iso(end),
        "target": target, "expected_kind": expected_kind, "completed": completed, **extra,
    }
    with open(INJECTIONS_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(f"Logged injection to {INJECTIONS_LOG.name}: {scenario} {record['start']} -> {record['end']}"
          + ("" if completed else " (interrupted)"))


def watch(seconds: float, status_fn, poll: float = 15) -> None:
    """Prints status_fn() every `poll` seconds for `seconds`."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        time.sleep(min(poll, max(0, deadline - time.time())))
        print(f"  {hms()}  {status_fn()}")


def pod_status(selector: str) -> str:
    """'pod-a Running restarts=2 (OOMKilled)' for each pod matching the label selector."""
    out = kubectl("get", "pods", "-l", selector, "-o",
                  "jsonpath={range .items[*]}{.metadata.name} "
                  "{.status.containerStatuses[0].state.waiting.reason}{.status.phase} "
                  "restarts={.status.containerStatuses[0].restartCount} "
                  "({.status.containerStatuses[0].lastState.terminated.reason}){\"\\n\"}{end}",
                  check=False)
    return "; ".join(line.strip() for line in out.splitlines() if line.strip()) or "no pods yet"
