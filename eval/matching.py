"""
eval/matching.py

Pure matching logic for eval/evaluate.py: decides which stored incidents
detected which chaos injections. No I/O, so it is unit-tested directly
(ml-engine/tests/test_evaluate.py).

Rules
-----
- An injection is DETECTED (a TP) if at least one incident of the same
  workload overlaps [injection start, injection end + grace] (grace
  defaults to 3 minutes: the detection needs a poll or two after the fault
  is removed, and the rate() window lags). Otherwise it is a FN.
- Further incidents matching an already-detected injection are DUPLICATES:
  the same failure reported twice. They are counted and reported, but not
  as false positives.
- An incident inside the evaluation period that matches no injection is a
  FP. It is attributed to the scenario whose target is its workload, or to
  "other" if no scenario targets that workload.
- MTTD = the matching incident's stored_at minus the injection start.
"""

from dataclasses import dataclass, field
from datetime import timedelta

import pandas as pd

GRACE = timedelta(minutes=3)
DEFAULT_WORKLOAD = "flask-deployment"  # incidents stored before the workload field existed


def ts(value) -> pd.Timestamp:
    """Any timestamp string/object -> naive UTC Timestamp."""
    t = pd.Timestamp(value)
    return t.tz_convert("UTC").tz_localize(None) if t.tzinfo is not None else t


@dataclass
class Injection:
    scenario: str
    start: pd.Timestamp
    end: pd.Timestamp
    target: str
    expected_kind: str
    completed: bool = True

    @classmethod
    def from_record(cls, r: dict) -> "Injection":
        return cls(r["scenario"], ts(r["start"]), ts(r["end"]), r["target"], r["expected_kind"],
                   r.get("completed", True))


@dataclass
class Match:
    injection: Injection
    incidents: list[dict] = field(default_factory=list)  # first one is the detection, rest duplicates

    @property
    def detected(self) -> bool:
        return bool(self.incidents)

    @property
    def first(self) -> dict | None:
        return self.incidents[0] if self.incidents else None

    def mttd_seconds(self) -> float | None:
        doc = self.first
        if doc is None or not doc.get("stored_at"):
            return None
        return (ts(doc["stored_at"]) - self.injection.start).total_seconds()


def incident_workload(doc: dict) -> str:
    return doc.get("workload") or DEFAULT_WORKLOAD


def overlaps(doc: dict, inj: Injection, grace: timedelta = GRACE) -> bool:
    # a target may name alternatives, e.g. "minikube-m02|chaos-node-stress" for node pressure
    return (incident_workload(doc) in inj.target.split("|")
            and ts(doc["start_time"]) <= inj.end + grace
            and ts(doc["end_time"]) >= inj.start)


def match_all(injections: list[Injection], incidents: list[dict],
              grace: timedelta = GRACE) -> tuple[list[Match], list[dict]]:
    """
    Returns (one Match per injection, false-positive incidents). Each
    incident is used for at most one injection -- the earliest one it
    overlaps -- so a long incident spanning two injections of the same
    workload can't count as two detections.
    """
    matches = [Match(inj) for inj in sorted(injections, key=lambda i: i.start)]
    used = set()
    for doc in sorted(incidents, key=lambda d: ts(d["start_time"])):
        for m in matches:
            if overlaps(doc, m.injection, grace):
                m.incidents.append(doc)
                used.add(doc["incident_id"])
                break
    false_positives = [d for d in incidents if d["incident_id"] not in used]
    return matches, false_positives


def prf(tp: int, fp: int, fn: int) -> dict:
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    if precision is None or recall is None:
        f1 = None
    else:
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall, "f1": f1}


def score(matches: list[Match], false_positives: list[dict], separate=("error_spike",)) -> dict:
    """
    Per-scenario and overall TP/FP/FN/precision/recall/F1. Scenarios in
    `separate` (validation rows like error_spike) get their own row but are
    left out of "overall".
    """
    scenarios = sorted({m.injection.scenario for m in matches})
    target_of = {}
    for m in matches:
        for target in m.injection.target.split("|"):
            target_of.setdefault(target, m.injection.scenario)
    fp_by_scenario: dict[str, int] = {}
    for doc in false_positives:
        owner = target_of.get(incident_workload(doc), "other")
        fp_by_scenario[owner] = fp_by_scenario.get(owner, 0) + 1

    rows = {}
    for s in scenarios:
        ms = [m for m in matches if m.injection.scenario == s]
        tp = sum(m.detected for m in ms)
        rows[s] = {**prf(tp, fp_by_scenario.get(s, 0), len(ms) - tp),
                   "injections": len(ms), "duplicates": sum(max(0, len(m.incidents) - 1) for m in ms)}

    main = [s for s in scenarios if s not in separate]
    tp = sum(rows[s]["tp"] for s in main)
    fn = sum(rows[s]["fn"] for s in main)
    fp = sum(rows[s]["fp"] for s in main) + fp_by_scenario.get("other", 0)
    overall = {**prf(tp, fp, fn), "injections": sum(rows[s]["injections"] for s in main),
               "duplicates": sum(rows[s]["duplicates"] for s in main)}
    return {"scenarios": rows, "overall": overall, "fp_other": fp_by_scenario.get("other", 0)}
