"""
tests/test_evaluate.py

Unit tests for the evaluation matching rules in eval/matching.py: which
incidents count as detections, duplicates and false positives, the
precision/recall/F1 arithmetic, and MTTD.

Run from ml-engine/:
    python -m pytest tests/test_evaluate.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "eval"))

from matching import Injection, match_all, prf, score  # noqa: E402

T0 = pd.Timestamp("2026-10-16 10:00:00")


def _inj(scenario, start_min, end_min, target, kind="OOMKilled"):
    return Injection(scenario, T0 + pd.Timedelta(minutes=start_min), T0 + pd.Timedelta(minutes=end_min), target, kind)


def _inc(incident_id, start_min, end_min, workload, stored_min=None):
    doc = {"incident_id": incident_id, "workload": workload,
           "start_time": str(T0 + pd.Timedelta(minutes=start_min)),
           "end_time": str(T0 + pd.Timedelta(minutes=end_min)), "max_severity": "high"}
    if stored_min is not None:
        doc["stored_at"] = str(T0 + pd.Timedelta(minutes=stored_min))
    return doc


def test_overlap_within_grace_and_same_workload_only():
    inj = _inj("oom_killed", 0, 4, "chaos-oom")
    matches, fps = match_all([inj], [
        _inc("late", 6.5, 7, "chaos-oom"),          # starts 2.5 min after end: inside the 3-min grace
        _inc("other", 1, 2, "chaos-crashloop"),     # right time, wrong workload
        _inc("toolate", 7.5, 8, "chaos-oom"),       # 3.5 min after end
        _inc("before", -3, -1, "chaos-oom"),        # ended before the injection
    ])
    assert [d["incident_id"] for d in matches[0].incidents] == ["late"]
    assert {d["incident_id"] for d in fps} == {"other", "toolate", "before"}


def test_duplicates_are_not_false_positives():
    matches, fps = match_all([_inj("crashloop", 0, 4, "chaos-crashloop")],
                             [_inc("a", 0.5, 2, "chaos-crashloop"), _inc("b", 3, 5, "chaos-crashloop")])
    assert len(matches[0].incidents) == 2 and fps == []
    s = score(matches, fps)
    assert s["scenarios"]["crashloop"]["duplicates"] == 1 and s["scenarios"]["crashloop"]["fp"] == 0


def test_one_incident_cannot_detect_two_injections():
    injections = [_inj("crashloop", 0, 4, "chaos-crashloop"), _inj("crashloop", 5, 9, "chaos-crashloop")]
    matches, _ = match_all(injections, [_inc("long", 1, 8, "chaos-crashloop")])
    assert [m.detected for m in matches] == [True, False]


def test_missing_workload_means_flask():
    doc = _inc("legacy", 1, 2, "x")
    del doc["workload"]
    matches, fps = match_all([_inj("cpu_exhaustion", 0, 3, "flask-deployment", "cpu_saturation")], [doc])
    assert matches[0].detected and not fps


def test_scores_per_scenario_and_overall():
    injections = [
        _inj("oom_killed", 0, 4, "chaos-oom"), _inj("oom_killed", 20, 24, "chaos-oom"),   # 1 TP, 1 FN
        _inj("crashloop", 40, 44, "chaos-crashloop"),                                       # 1 TP
        _inj("error_spike", 60, 62, "flask-deployment", "error_spike"),                     # separate row
    ]
    incidents = [
        _inc("o1", 1, 3, "chaos-oom", stored_min=2), _inc("c1", 41, 43, "chaos-crashloop", stored_min=42.5),
        _inc("e1", 60.5, 61, "flask-deployment"),
        _inc("fp1", 30, 31, "chaos-oom"),           # FP attributed to oom_killed
        _inc("fp2", 50, 51, "baseline-traffic"),    # FP on a workload no scenario targets -> "other"
    ]
    matches, fps = match_all(injections, incidents)
    s = score(matches, fps)

    oom = s["scenarios"]["oom_killed"]
    assert (oom["tp"], oom["fp"], oom["fn"]) == (1, 1, 1)
    assert oom["precision"] == 0.5 and oom["recall"] == 0.5 and oom["f1"] == 0.5
    assert s["scenarios"]["error_spike"]["tp"] == 1
    # overall leaves out error_spike but includes the "other" FP
    assert (s["overall"]["tp"], s["overall"]["fp"], s["overall"]["fn"]) == (2, 2, 1)
    assert s["fp_other"] == 1

    by_id = {m.first["incident_id"]: m for m in matches if m.detected}
    assert by_id["o1"].mttd_seconds() == 120
    assert by_id["c1"].mttd_seconds() == 150
    assert by_id["e1"].mttd_seconds() is None  # no stored_at


def test_target_alternatives():
    inj = _inj("node_pressure", 0, 4, "minikube-m02|chaos-node-stress", "node_pressure")
    matches, fps = match_all([inj], [_inc("node", 1, 3, "minikube-m02"), _inc("pod", 1.5, 3, "chaos-node-stress"),
                                     _inc("fp", 30, 31, "chaos-node-stress")])
    assert [d["incident_id"] for d in matches[0].incidents] == ["node", "pod"]
    s = score(matches, fps)
    assert s["scenarios"]["node_pressure"]["fp"] == 1 and s["fp_other"] == 0


def test_prf_edge_cases():
    assert prf(0, 0, 0)["f1"] is None
    assert prf(0, 2, 3) == {"tp": 0, "fp": 2, "fn": 3, "precision": 0.0, "recall": 0.0, "f1": 0.0}
    assert prf(3, 0, 0)["f1"] == 1.0
