"""
tests/test_phase1_fixes.py

Offline checks for the final-sprint Phase 1 fixes:
  1. deterministic incident ids (workload + 5-minute start bucket)
  2. find_overlapping compares real datetimes, per workload; id collisions get a suffix
  3. the baseline is fitted on an earlier window and a refit is skipped over an incident
  4. RCA: a failed Gemini call returns an error dict, and only a valid diagnosis reaches Chroma

No cluster or Gemini needed. Mongo tests use a throwaway database that is dropped.

Run from ml-engine/:
    python -m pytest tests/test_phase1_fixes.py
"""

import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline  # noqa: E402
from anomaly_detector import Anomaly  # noqa: E402
from correlation_engine import (  # noqa: E402
    CorrelationEngine, Incident, make_incident_id, workload_from_target,
)
from incident_store import IncidentStore  # noqa: E402

WINDOW = timedelta(minutes=3)
TEST_DB = "chaos_healer_phase1_test"


def _store():
    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        return None
    store.collection.drop()
    return store


# --- 1. deterministic ids ------------------------------------------------

def test_incident_id_is_deterministic_per_workload_and_bucket():
    t = pd.Timestamp("2026-10-16 10:01:30")
    assert make_incident_id("chaos-oom", t) == make_incident_id("chaos-oom", t + pd.Timedelta(minutes=3))  # same 5-min bucket
    assert make_incident_id("chaos-oom", t) != make_incident_id("chaos-oom", t + pd.Timedelta(minutes=4))  # next bucket
    assert make_incident_id("chaos-oom", t) != make_incident_id("chaos-crashloop", t)
    assert len(make_incident_id("x", t)) == 8


def test_correlation_engine_ids_repeat_across_runs():
    t = pd.Timestamp("2026-10-16 10:00:00")
    anomalies = [Anomaly(t + pd.Timedelta(seconds=30 * i), {"cpu_percent": 90}, -0.2, 0.7, "high") for i in range(3)]
    a = CorrelationEngine().correlate(anomalies)
    b = CorrelationEngine().correlate(list(reversed(anomalies)))
    assert a[0].incident_id == b[0].incident_id and a[0].workload == "flask-deployment"


def test_workload_from_target():
    assert workload_from_target("chaos-oom-7d9f8c6b5-x2k4p/hog") == "chaos-oom"
    assert workload_from_target("flask-deployment-5b6c7d8f9-abcde/flask-container") == "flask-deployment"
    assert workload_from_target("chaos-cpu-load-q7w2z/load") == "chaos-cpu-load"  # Job pod
    assert workload_from_target("demo-redis") == "demo-redis"  # deployment target left alone


# --- 2. datetime overlap + collisions --------------------------------------

def _rule_incident(workload, start, minutes=1):
    end = start + pd.Timedelta(minutes=minutes)
    return Incident(make_incident_id(workload, start), start, end, [], "high", 1.0, "rule",
                    [{"kind": "waiting", "target": f"{workload}-5d8f7b9c4-x2k4p/c", "reason": "CrashLoopBackOff",
                      "first_seen": str(start), "last_seen": str(end), "value": 1}],
                    workload=workload)


def test_find_overlapping_uses_datetimes_and_workload():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        t = pd.Timestamp("2026-10-16 09:59:00")
        store.save_or_merge(_rule_incident("chaos-oom", t), WINDOW)
        # a stored string in a different format must still compare correctly
        store.collection.update_one({}, {"$set": {"start_time": "2026-10-16T09:59:00+00:00"}})

        assert store.find_overlapping(t + pd.Timedelta(minutes=3), t + pd.Timedelta(minutes=4), WINDOW, "chaos-oom")
        assert store.find_overlapping(t + pd.Timedelta(minutes=5), t + pd.Timedelta(minutes=6), WINDOW, "chaos-oom") is None
        # same time, different workload -> not the same incident
        assert store.find_overlapping(t, t, WINDOW, "chaos-crashloop") is None
        # a timezone-aware query time is the same instant
        assert store.find_overlapping(pd.Timestamp("2026-10-16 10:00:00+00:00"), t, WINDOW, "chaos-oom")
    finally:
        store.client.drop_database(TEST_DB)


def test_same_bucket_non_overlapping_incidents_get_distinct_ids():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        t = pd.Timestamp("2026-10-16 10:00:00")
        first = _rule_incident("chaos-oom", t, minutes=0)
        second = _rule_incident("chaos-oom", t + pd.Timedelta(minutes=4), minutes=0)  # 4 min gap > 3 min window
        second.incident_id = first.incident_id  # what make_incident_id gives within one bucket
        d1, _ = store.save_or_merge(first, WINDOW)
        d2, _ = store.save_or_merge(second, WINDOW)
        assert d1["incident_id"] != d2["incident_id"] and d2["incident_id"].startswith(d1["incident_id"])
        assert store.collection.count_documents({}) == 2
        assert "stored_at" in store.collection.find_one({"incident_id": d1["incident_id"]})
    finally:
        store.client.drop_database(TEST_DB)


# --- 3. separate baseline ---------------------------------------------------

class FakeMetricsClient:
    """get_metrics_dataframe over [now-minutes_back, now-end_minutes_back], 30s steps of normal traffic."""

    def __init__(self):
        self.calls = []

    def get_metrics_dataframe(self, minutes_back=30, step="30s", end_minutes_back=0, queries=None, quiet=False):
        self.calls.append((minutes_back, end_minutes_back))
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        ts = pd.date_range(now - pd.Timedelta(minutes=minutes_back), now - pd.Timedelta(minutes=end_minutes_back), freq="30s")
        rng = np.random.default_rng(0)
        return pd.DataFrame({"timestamp": ts, "cpu_percent": rng.normal(2, 0.3, len(ts)),
                             "memory_percent": rng.normal(12, 0.2, len(ts)), "error_rate": rng.normal(10, 2, len(ts))})


def test_baseline_fits_on_earlier_window_and_refit_skips_incidents():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        client = FakeMetricsClient()
        detector, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW)
        assert detector is not None and detector.is_fitted, msg
        assert client.calls == [(60, 30)]  # only the earlier window was fetched

        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        # a CLOSED flask incident 45 minutes ago, inside [now-60m, now-30m]: its rows are left out
        store.save_or_merge(_rule_incident("flask-deployment", now - pd.Timedelta(minutes=45), minutes=2), WINDOW)
        refit, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW, skip_if_open=True,
                                           workload="flask-deployment")
        assert refit is not None and "left out" in msg, msg

        # another workload's incident doesn't touch flask's baseline
        refit, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW, skip_if_open=True, workload="demo-db-app")
        assert refit is not None and "left out" not in msg, msg

        # an OPEN flask incident (started 40 min ago, still being extended now) -> skip the refit
        store.save_or_merge(_rule_incident("flask-deployment", now - pd.Timedelta(minutes=40), minutes=40), WINDOW)
        refit, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW, skip_if_open=True,
                                           workload="flask-deployment")
        assert refit is None and "open incident" in msg, msg
    finally:
        store.client.drop_database(TEST_DB)


# --- 4. RCA failure handling ------------------------------------------------

class _FakeCollection:
    def __init__(self):
        self.upserts = []

    def count(self):
        return 0

    def upsert(self, **kw):
        self.upserts.append(kw["ids"][0])


class _FakeLLM:
    def __init__(self, reply=None, error=None):
        self.reply, self.error = reply, error

    def invoke(self, _messages):
        if self.error:
            raise self.error
        return type("R", (), {"content": self.reply})()


def _rca(llm):
    from rca_engine import RCAEngine
    rca = object.__new__(RCAEngine)  # skip __init__: no API key, Chroma or Mongo needed
    rca.llm, rca.collection, rca.store, rca.mongo_available = llm, _FakeCollection(), None, False
    return rca


def test_rca_failed_call_returns_error_and_skips_chroma():
    rca = _rca(_FakeLLM(error=RuntimeError("429 quota exceeded")))
    result = rca.diagnose(_rule_incident("chaos-oom", pd.Timestamp("2026-10-16 10:00")), persist=False)
    assert "error" in result and "quota" in result["error"]
    assert rca.collection.upserts == []


def test_rca_unparseable_reply_skips_chroma_valid_reply_upserts():
    incident = _rule_incident("chaos-oom", pd.Timestamp("2026-10-16 10:00"))

    bad = _rca(_FakeLLM(reply="sorry, I can't help"))
    assert "raw_response" in bad.diagnose(incident, persist=False)
    assert bad.collection.upserts == []

    good = _rca(_FakeLLM(reply='```json\n{"summary": "s", "likely_root_cause": "OOM", '
                               '"confidence_reasoning": "c", "suggested_fix": "f"}\n```'))
    result = good.diagnose(incident, persist=False)
    assert result["likely_root_cause"] == "OOM"
    assert good.collection.upserts == [incident.incident_id]


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
