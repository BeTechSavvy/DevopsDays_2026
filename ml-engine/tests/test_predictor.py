"""
tests/test_predictor.py

Offline checks for predictor.py: the least-squares time-to-breach, and
which series raise a predicted_exhaustion warning.

Run from ml-engine/:
    python -m pytest tests/test_predictor.py
"""

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import predictor  # noqa: E402
from correlation_engine import workload_from_target  # noqa: E402
from k8s_signals import attach_k8s_signals, signal_severity, summarize_signals  # noqa: E402

T0 = 1_792_130_000.0


def _series(pod, values, step=15):
    return {"metric": {"pod": pod}, "values": [[T0 + i * step, str(v)] for i, v in enumerate(values)]}


def test_time_to_breach_linear():
    t = np.arange(10) * 15.0
    slope, ttb = predictor.time_to_breach(t, 40 + 0.4 * t)  # 0.4 %/s, at 94% after 135 s
    assert abs(slope - 0.4) < 1e-9 and abs(ttb - 15) < 1e-6
    assert predictor.time_to_breach(t, 50 - 0.1 * t) is None  # falling
    assert predictor.time_to_breach(t, np.full(10, 50.0)) is None  # flat


def test_predict_warns_only_within_horizon_and_with_full_window():
    leak = _series("chaos-oom-5f7d8c9b4-q2w3e", [40 + 3 * i for i in range(12)])      # +0.2%/s -> ~2.5 min left
    slow = _series("flask-deployment-7c9d8f6b5-aaaaa", [12 + 0.01 * i for i in range(12)])  # years away
    short = _series("chaos-oom-5f7d8c9b4-zzzzz", [40, 60, 80])                          # < 10 samples
    warnings = predictor.predict([leak, slow, short], "memory_percent")

    assert len(warnings) == 1
    w = warnings[0]
    assert w.kind == "predicted_exhaustion" and w.reason == "MemoryExhaustionPredicted"
    assert workload_from_target(w.target) == "chaos-oom"
    assert 0 < w.value <= predictor.HORIZON_SECONDS


def test_startup_ramp_that_levels_off_is_not_a_leak():
    # 0 -> 30% in 3 samples, then flat: rising least-squares slope, but not linear
    ramp = _series("chaos-rollout-7c9d8e6f4-u6i7o", [0, 15, 30, 30, 30, 30, 30, 30, 30, 30], step=15)
    assert predictor.predict([ramp], "memory_percent", horizon=10_000) == []


def test_warning_becomes_medium_rule_incident_for_its_workload():
    w = predictor.predict([_series("chaos-oom-5f7d8c9b4-q2w3e", [40 + 3 * i for i in range(10)])], "memory_percent")
    incidents = attach_k8s_signals([], w, window=__import__("datetime").timedelta(minutes=3))
    assert len(incidents) == 1 and incidents[0].workload == "chaos-oom"
    assert incidents[0].max_severity == "medium"
    assert signal_severity(summarize_signals(w)[0]) == "medium"
