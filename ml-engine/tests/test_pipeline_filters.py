"""
tests/test_pipeline_filters.py

Offline checks for two scenario (a) fixes:
  1. metric-based severity in anomaly_detector.py (cpu/memory > 85, error_rate > 25 -> high)
  2. the noise filter in pipeline.py (a lone low-severity anomaly can't open a new incident)

No cluster or Gemini needed. The noise-filter test uses a throwaway MongoDB
database that is dropped at the end.

Run from ml-engine/ with the rca-venv2 Python:
    python tests/test_pipeline_filters.py
"""

import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline  # noqa: E402
from anomaly_detector import Anomaly, AnomalyDetector  # noqa: E402
from correlation_engine import Incident  # noqa: E402
from incident_store import IncidentStore  # noqa: E402

WINDOW = timedelta(minutes=3)
TEST_DB = "chaos_healer_pipeline_filters_test"
FEATURES = ["cpu_percent", "memory_percent", "error_rate"]


def _normal_traffic(n=60, seed=1):
    """Looks like the live /work baseline: ~1.5% CPU, ~12% memory, ~10% errors."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-09-24 05:00", periods=n, freq="30s"),
        "cpu_percent": rng.normal(1.5, 0.3, n).clip(0),
        "memory_percent": rng.normal(12, 0.2, n).clip(0),
        "error_rate": rng.normal(10, 2, n).clip(0),
    })


def _score(detector, cpu, memory, errors):
    row = pd.DataFrame({"timestamp": [pd.Timestamp("2026-09-24 06:00")],
                        "cpu_percent": [cpu], "memory_percent": [memory], "error_rate": [errors]})
    return detector.score_batch(row)


def test_metric_thresholds():
    ms = AnomalyDetector._metric_severity
    assert ms({"cpu_percent": 94, "memory_percent": 12, "error_rate": 3}) == "high"
    assert ms({"cpu_percent": 2, "memory_percent": 90, "error_rate": 3}) == "high"
    assert ms({"cpu_percent": 2, "memory_percent": 12, "error_rate": 39}) == "high"
    # exactly at the limit is not past it; below every limit defers to the model
    assert ms({"cpu_percent": 85, "memory_percent": 85, "error_rate": 25}) == "low"


def test_cpu_spike_scores_high():
    detector = AnomalyDetector(contamination=0.05)
    detector.fit(_normal_traffic(), feature_columns=FEATURES)

    spike = _score(detector, cpu=94, memory=12, errors=3)
    assert len(spike) == 1 and spike[0].severity == "high", spike
    # the model alone would not have said high (the reason for this fix)
    assert AnomalyDetector._score_to_severity(spike[0].anomaly_score) != "high"

    # an anomaly below every threshold keeps the model's own severity
    # (off-range on several metrics, since one off-range metric alone may not be flagged)
    moderate = _score(detector, cpu=60, memory=20, errors=22)
    assert len(moderate) == 1, moderate
    assert moderate[0].severity == AnomalyDetector._score_to_severity(moderate[0].anomaly_score) == "medium"


def _ml_incident(incident_id, minute, severities, detection="ml", signals=None):
    t0 = pd.Timestamp("2026-09-24 05:50") + pd.Timedelta(minutes=minute)
    anomalies = [
        Anomaly(t0 + pd.Timedelta(seconds=30 * i), {"cpu_percent": 1.4, "memory_percent": 12, "error_rate": 10.5},
                -0.01, 0.5, sev)
        for i, sev in enumerate(severities)
    ]
    rank = {"low": 0, "medium": 1, "high": 2}
    return Incident(incident_id, anomalies[0].timestamp, anomalies[-1].timestamp, anomalies,
                    max(severities, key=rank.__getitem__), 0.5, detection, signals or [])


def test_is_weak():
    assert pipeline.is_weak(_ml_incident("a", 0, ["low"]))
    assert not pipeline.is_weak(_ml_incident("b", 0, ["medium"]))
    assert not pipeline.is_weak(_ml_incident("c", 0, ["low", "low"]))
    assert not pipeline.is_weak(_ml_incident("d", 0, ["low"], detection="ml+rule", signals=[{
        "kind": "restart", "target": "p/c", "reason": "OOMKilled",
        "first_seen": "2026-09-24 05:50:00", "last_seen": "2026-09-24 05:50:00", "value": 1}]))


def test_noise_filter_blocks_new_but_allows_merge():
    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        print("  (skipped: MongoDB not reachable)")
        return
    store.collection.drop()
    try:
        # the live blip: one low anomaly on its own -> ignored, nothing written
        new, merged, skipped, diag = pipeline.save_incidents(store, [_ml_incident("blip0001", 0, ["low"])], WINDOW)
        assert (new, merged, skipped, len(diag)) == (0, 0, 1, 0)
        assert store.collection.count_documents({}) == 0

        # a real spike starts a new incident
        new, merged, skipped, diag = pipeline.save_incidents(
            store, [_ml_incident("spike001", 10, ["medium", "high"])], WINDOW)
        assert (new, merged, skipped, len(diag)) == (1, 0, 0, 1)

        # a single weak anomaly next to it still merges in (the recovery tail of the spike)
        new, merged, skipped, _ = pipeline.save_incidents(store, [_ml_incident("tail0001", 12, ["low"])], WINDOW)
        assert (new, merged, skipped) == (0, 1, 0)
        doc = store.collection.find_one({"incident_id": "spike001"})
        assert doc["anomaly_count"] == 3

        # two low anomalies together are enough to open an incident
        new, _, skipped, _ = pipeline.save_incidents(store, [_ml_incident("pair0001", 30, ["low", "low"])], WINDOW)
        assert (new, skipped) == (1, 0)
        assert store.collection.count_documents({}) == 2
    finally:
        store.client.drop_database(TEST_DB)


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {name}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    raise SystemExit(1 if failed else 0)
