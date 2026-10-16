"""
tests/test_incident_closing.py

The perpetual-incident fix (found during the evaluation run): steady
low/medium noise kept one flask incident open for 100+ minutes, it absorbed
the next CPU injection, and as an open incident it blocked every baseline
refit, so the stale baseline kept producing the noise.

  1. an incident closes after QUIET_CLOSE without medium/high activity, or at
     MAX_INCIDENT_DURATION; later activity starts a new incident
  2. a stale low/medium incident doesn't block a refit and its rows are kept
     (drift); a long pre-fix incident loses only the rows around its high
     anomalies

Run from ml-engine/:
    python -m pytest tests/test_incident_closing.py
"""

import os
import sys
from datetime import timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pipeline  # noqa: E402
from anomaly_detector import Anomaly  # noqa: E402
from correlation_engine import Incident, make_incident_id  # noqa: E402
from incident_store import IncidentStore, is_closed, is_noise  # noqa: E402
from test_phase1_fixes import FakeMetricsClient  # noqa: E402

WINDOW = timedelta(minutes=3)
TEST_DB = "chaos_healer_closing_test"
T0 = pd.Timestamp("2026-10-16 09:00:00")


def _store():
    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        return None
    store.collection.drop()
    return store


def _incident(start, minutes, severity="medium", every=1, high_at=(), workload="flask-deployment"):
    """An ML incident with one `severity` anomaly every `every` minutes, plus high ones at `high_at`."""
    times = [start + pd.Timedelta(minutes=m) for m in range(0, minutes + 1, every)]
    anomalies = [Anomaly(t, {"cpu_percent": 2.0, "memory_percent": 14.6, "error_rate": 11.0}, -0.1, 0.6,
                         "high" if t in high_at else severity) for t in times]
    worst = "high" if high_at else severity
    return Incident(make_incident_id(workload, start), start, times[-1], anomalies, worst, 0.6, "ml", [],
                    workload=workload)


def _doc(*args, **kw):
    return _incident(*args, **kw).to_dict()


# --- 1. closing ------------------------------------------------------------

def test_quiet_for_ten_minutes_closes():
    doc = _doc(T0, 5)                                    # medium anomalies until T0+5
    assert not is_closed(doc, T0 + pd.Timedelta(minutes=14))
    assert is_closed(doc, T0 + pd.Timedelta(minutes=15))


def test_low_only_activity_does_not_keep_it_open():
    doc = _doc(T0, 12, severity="low")                   # only low anomalies: quiet since its start
    assert is_closed(doc, T0 + pd.Timedelta(minutes=10))


def test_thirty_minutes_closes_even_when_busy():
    doc = _doc(T0, 29)                                   # medium every minute
    assert not is_closed(doc, T0 + pd.Timedelta(minutes=29))
    assert is_closed(doc, T0 + pd.Timedelta(minutes=30))


def test_kubernetes_signals_count_as_activity():
    doc = _doc(T0, 1, severity="low")
    doc["k8s_signals"] = [{"kind": "waiting", "reason": "CrashLoopBackOff", "target": "x/c",
                           "first_seen": str(T0), "last_seen": str(T0 + pd.Timedelta(minutes=8)), "value": 1}]
    assert not is_closed(doc, T0 + pd.Timedelta(minutes=12))
    assert not is_noise(doc)


def test_closed_incident_is_not_extended_by_new_activity():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        first, _ = store.save_or_merge(_incident(T0, 29), WINDOW)
        # the next cycle's anomalies, 30 minutes after the start: a new incident
        second, _ = store.save_or_merge(_incident(T0 + pd.Timedelta(minutes=30), 1), WINDOW)
        assert second["incident_id"] != first["incident_id"]
        assert store.collection.count_documents({}) == 2
        # while it's still within both limits, it merges as before
        third, _ = store.save_or_merge(_incident(T0 + pd.Timedelta(minutes=32), 1), WINDOW)
        assert third["incident_id"] == second["incident_id"]
        assert store.collection.count_documents({}) == 2
    finally:
        store.client.drop_database(TEST_DB)


# --- 2. refits --------------------------------------------------------------

def test_stale_noise_does_not_block_refit_and_is_kept():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        client = FakeMetricsClient()
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        # medium noise from 50 min ago until now -- still open, inside [now-60m, now-30m]
        store.collection.insert_one(dict(_doc(now - pd.Timedelta(minutes=50), 50), start_dt=(now - pd.Timedelta(minutes=50)).to_pydatetime(),
                                         end_dt=now.to_pydatetime()))
        refit, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW, skip_if_open=True,
                                           workload="flask-deployment")
        assert refit is not None, msg
        assert "left out" not in msg, msg               # its rows were used: drift, not a fault
    finally:
        store.client.drop_database(TEST_DB)


def test_long_prefix_incident_loses_only_rows_around_high_anomalies():
    store = _store()
    if store is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        client = FakeMetricsClient()
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        start = now - pd.Timedelta(minutes=100)
        spike = start + pd.Timedelta(minutes=50)        # one high anomaly, inside [now-60m, now-30m]
        doc = _doc(start, 90, high_at=(spike,))
        store.collection.insert_one(dict(doc, start_dt=start.to_pydatetime(),
                                         end_dt=(start + pd.Timedelta(minutes=90)).to_pydatetime()))
        refit, msg = pipeline.fit_baseline(client, store, 60, 30, WINDOW, workload="flask-deployment")
        assert refit is not None, msg
        # 30 min at 30 s = 61 rows; only the spike +/- one 30 s sample is dropped
        n = int(msg.split("fitted on ")[1].split(" points")[0])
        assert 55 <= n < 61, msg
    finally:
        store.client.drop_database(TEST_DB)
