"""
tests/test_k8s_signals.py

Offline checks for k8s_signals.py and the incident merge in incident_store.py.
Uses fake Prometheus responses (no cluster needed) and a throwaway MongoDB
database that is dropped at the end -- the real chaos_healer DB is untouched.

Run from ml-engine/ with the rca-venv2 Python:
    python tests/test_k8s_signals.py

Also pytest-compatible (test_* functions) if pytest is installed later.
"""

import os
import sys
import time
from datetime import timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import k8s_signals as ks  # noqa: E402
from anomaly_detector import Anomaly  # noqa: E402
from correlation_engine import Incident  # noqa: E402
from incident_store import IncidentStore  # noqa: E402

WINDOW = timedelta(minutes=3)
TEST_DB = "chaos_healer_k8s_signals_test"


def _series(metric, points):
    return {"metric": metric, "values": [[t, str(v)] for t, v in points]}


class FakeClient:
    """Stands in for PrometheusClient.query_range with canned kube-state-metrics data."""

    def __init__(self):
        # Samples are placed relative to the real clock, 30s apart, so they
        # line up with fetch_k8s_signals' own window calculation.
        self.now = time.time()

    def t(self, mins_ago):
        return self.now - mins_ago * 60

    def query_range(self, q, start, end, step="30s"):
        t = self.t
        if q == ks.RESTARTS_QUERY:
            return [
                # OOM pod: restarts 0 -> 1 -> 2 around 10 min ago
                _series({"pod": "flask-deployment-a", "container": "flask"},
                        [(t(12), 0), (t(11), 0), (t(10), 1), (t(9), 2), (t(8), 2)]),
                # restarted 40 min ago (e.g. laptop reboot): flat in window, sticky "Error" must NOT fire
                _series({"pod": "flask-deployment-b", "container": "flask"},
                        [(t(12), 3), (t(8), 3)]),
            ]
        if q == ks.TERMINATED_REASON_QUERY:
            return [
                _series({"pod": "flask-deployment-a", "container": "flask", "reason": "OOMKilled"},
                        [(t(10), 1), (t(9), 1)]),
                _series({"pod": "flask-deployment-b", "container": "flask", "reason": "Error"},
                        [(t(12), 1), (t(8), 1)]),
            ]
        if q == ks.WAITING_REASON_QUERY:
            return [_series({"pod": "chaos-crashloop-x", "container": "busybox", "reason": "CrashLoopBackOff"},
                            [(t(25), 1)])]  # a single sample must still count
        if q == ks.UNAVAILABLE_QUERY:
            return [
                # normal rollout restart: 2 consecutive samples -> ignored
                _series({"deployment": "flask-deployment"}, [(t(5), 1), (t(4.5), 1)]),
                # failing deployment: unavailable for 4 consecutive samples -> counts
                _series({"deployment": "chaos-crashloop"},
                        [(t(25), 1), (t(24.5), 1), (t(24), 1), (t(23.5), 1)]),
                # two 2-sample blips separated by a gap -> ignored
                _series({"deployment": "chaos-blip"}, [(t(18), 1), (t(17.5), 1), (t(16), 1), (t(15.5), 1)]),
                # run began before the 30-min window: 3 consecutive incl. lookback -> only the in-window sample kept
                _series({"deployment": "chaos-early"}, [(t(30.5), 2), (t(30), 2), (t(29.5), 2)]),
            ]
        raise AssertionError(f"unexpected query: {q}")


def _fetch():
    client = FakeClient()
    return client, ks.fetch_k8s_signals(client, minutes_back=30)


def _crashloop_incident(rule_incidents):
    return next(r for r in rule_incidents if any(s["target"] == "chaos-crashloop" for s in r.k8s_signals))


def test_restarts_use_reason_only_for_restarts_in_window():
    _, signals = _fetch()
    restarts = [s for s in signals if s.kind == "restart"]
    assert len(restarts) == 2 and all(s.reason == "OOMKilled" for s in restarts), restarts
    assert not any(s.target.startswith("flask-deployment-b") for s in signals), "sticky reboot Error leaked"


def test_bad_waiting_reason_counts_immediately():
    _, signals = _fetch()
    assert [s.reason for s in signals if s.kind == "waiting"] == ["CrashLoopBackOff"]


def test_unavailable_replicas_need_min_consecutive_samples():
    client, signals = _fetch()
    unavailable = [s for s in signals if s.kind == "replicas_unavailable"]
    assert {s.target for s in unavailable} == {"chaos-crashloop", "chaos-early"}, unavailable
    assert sum(s.target == "chaos-crashloop" for s in unavailable) == 4
    early = [s for s in unavailable if s.target == "chaos-early"]
    assert len(early) == 1 and early[0].timestamp == pd.Timestamp(client.t(29.5), unit="s"), early


def test_attach_to_ml_incident_and_raise_rule_incidents():
    client, signals = _fetch()
    ts = pd.Timestamp(client.t(9.5), unit="s")
    ml = Incident(
        "mlinc001", ts, ts,
        [Anomaly(ts, {"cpu_percent": 20, "memory_percent": 97, "error_rate": 0}, -0.2, 0.8, "medium")],
        "medium", 0.8,
    )
    rule = ks.attach_k8s_signals([ml], signals, WINDOW)

    assert ml.detection == "ml+rule" and ml.max_severity == "high"
    assert len(ml.k8s_signals) == 1 and ml.k8s_signals[0]["value"] == 2  # 2 OOM restarts

    # chaos-early and the crash loop are > 3 min apart -> two separate rule incidents
    assert len(rule) == 2
    assert all(r.detection == "rule" and r.max_severity == "high" and r.anomalies == [] for r in rule)


def test_merge_instead_of_duplicate():
    _, signals = _fetch()
    first = _crashloop_incident(ks.attach_k8s_signals([], signals, WINDOW))

    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        print("  (skipped: MongoDB not reachable)")
        return
    store.collection.drop()
    try:
        doc, needs = store.save_or_merge(first, WINDOW)
        assert needs and doc["incident_id"] == first.incident_id
        store.save_diagnosis(doc["incident_id"], {"summary": "x"})

        # Same failure seen again on a re-run: new id, same time -> merged, no new Gemini call
        again = _crashloop_incident(ks.attach_k8s_signals([], signals, WINDOW))
        doc2, needs2 = store.save_or_merge(again, WINDOW)
        assert doc2["incident_id"] == first.incident_id and not needs2
        assert store.collection.count_documents({}) == 1

        # Later poll: a new KIND of failure appears -> merged AND re-diagnosed
        t = first.end_time + pd.Timedelta(minutes=1)
        later = Incident("later001", t, t, [], "medium", 1.0, "rule", [{
            "kind": "restart", "target": "chaos-crashloop-x/busybox", "reason": "Error",
            "first_seen": str(t), "last_seen": str(t), "value": 1,
        }])
        doc3, needs3 = store.save_or_merge(later, WINDOW)
        assert doc3["incident_id"] == first.incident_id and needs3
        assert doc3["end_time"] == str(t) and doc3["max_severity"] == "high"
        assert len(Incident.from_dict(doc3).k8s_signals) == 3
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
