"""
tests/test_counterfactual.py

Offline checks for counterfactual.py: the right rule is picked for each
failure kind, and observed values are filled into the recommendation.

Run from ml-engine/:
    python -m pytest tests/test_counterfactual.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from counterfactual import recommend  # noqa: E402


def _doc(workload, signals=(), anomalies=()):
    return {"incident_id": "x", "workload": workload, "start_time": "2026-10-16 10:00:00",
            "end_time": "2026-10-16 10:05:00", "k8s_signals": list(signals), "anomalies": list(anomalies)}


def _sig(kind, reason, value=1, target="p/c"):
    return {"kind": kind, "target": target, "reason": reason, "value": value,
            "first_seen": "2026-10-16 10:01:00", "last_seen": "2026-10-16 10:02:00"}


def test_oom_uses_observed_peak_plus_25_percent():
    doc = _doc("chaos-oom", [_sig("restart", "OOMKilled", 2), _sig("waiting", "CrashLoopBackOff")])
    cf = recommend(doc, {"memory_peak_mib": 128, "memory_limit_mib": 128})
    assert cf["kind"] == "OOMKilled"
    assert "160Mi" in cf["what_if"] and "128Mi limit" in cf["what_if"] and "2 OOM kill(s)" in cf["what_if"]
    assert any("160Mi" in a for a in cf["actions"]) and any("leak" in a for a in cf["actions"])


def test_cpu_recommends_limit_and_hpa():
    doc = _doc("flask-deployment", anomalies=[{"metrics": {"cpu_percent": 96, "memory_percent": 12, "error_rate": 8}}])
    cf = recommend(doc, {"cpu_limit_millicores": 250})
    assert cf["kind"] == "cpu_saturation"
    assert "96%" in cf["what_if"] and "250m" in cf["what_if"]
    assert any("autoscale" in a for a in cf["actions"])


def test_crashloop_rollout_and_errors():
    assert recommend(_doc("chaos-crashloop", [_sig("waiting", "CrashLoopBackOff")]))["kind"] == "CrashLoopBackOff"
    cf = recommend(_doc("chaos-rollout", [_sig("waiting", "ErrImagePull")]), {"image": "flask-demo:v2-does-not-exist"})
    assert cf["kind"] == "ImagePullBackOff" and "v2-does-not-exist" in cf["what_if"]
    assert any("rollout undo deployment/chaos-rollout" in a for a in cf["actions"])
    cf = recommend(_doc("flask-deployment", anomalies=[{"metrics": {"cpu_percent": 2, "error_rate": 80}}]))
    assert cf["kind"] == "error_spike" and "80%" in cf["what_if"]


def test_no_rule_for_plain_noise():
    assert recommend(_doc("flask-deployment", anomalies=[{"metrics": {"cpu_percent": 2, "error_rate": 9}}])) is None
