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
                _series({"pod": "flask-deployment-7c9d8f6b5-aaaaa", "container": "flask"},
                        [(t(12), 0), (t(11), 0), (t(10), 1), (t(9), 2), (t(8), 2)]),
                # restarted 40 min ago (e.g. laptop reboot): flat in window, sticky "Error" must NOT fire
                _series({"pod": "flask-deployment-7c9d8f6b5-bbbbb", "container": "flask"},
                        [(t(12), 3), (t(8), 3)]),
            ]
        if q == ks.TERMINATED_REASON_QUERY:
            return [
                _series({"pod": "flask-deployment-7c9d8f6b5-aaaaa", "container": "flask", "reason": "OOMKilled"},
                        [(t(10), 1), (t(9), 1)]),
                _series({"pod": "flask-deployment-7c9d8f6b5-bbbbb", "container": "flask", "reason": "Error"},
                        [(t(12), 1), (t(8), 1)]),
            ]
        if q == ks.WAITING_REASON_QUERY:
            return [_series({"pod": "chaos-crashloop-5d8f7b9c4-x2k4p", "container": "busybox", "reason": "CrashLoopBackOff"},
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
        if q == ks.CREATED_QUERY:
            # both flask pods are old; the reboot pod's series starts mid-window (t(12)) with
            # restarts=3, but it was created long before the window -> must not count
            return [_series({"pod": "flask-deployment-7c9d8f6b5-aaaaa"}, [(t(12), t(90))]),
                    _series({"pod": "flask-deployment-7c9d8f6b5-bbbbb"}, [(t(12), t(45))])]
        if q == ks.TERMINATED_NOW_QUERY:
            return []
        if q == ks.NODE_PRESSURE_QUERY:
            return [_series({"node": "minikube-m02", "condition": "MemoryPressure"}, [(t(20), 1), (t(19.5), 1)])]
        if q == ks.EVICTED_QUERY:
            return [_series({"pod": "chaos-node-stress-6d5f8b7c9-k2j3h"}, [(t(19.5), 1)])]
        if q == ks.UNSCHEDULABLE_QUERY:
            return [_series({"pod": "chaos-node-stress-6d5f8b7c9-m4n5b"}, [(t(19), 1)])]
        if q == ks.PENDING_QUERY:
            return [
                # pending 4 samples in a row -> counts; a 2-sample normal start -> ignored
                _series({"pod": "chaos-node-stress-6d5f8b7c9-m4n5b"}, [(t(19), 1), (t(18.5), 1), (t(18), 1), (t(17.5), 1)]),
                _series({"pod": "flask-deployment-7c9d8f6b5-ccccc"}, [(t(3), 1), (t(2.5), 1)]),
            ]
        raise AssertionError(f"unexpected query: {q}")


class NewCrashingPodClient(FakeClient):
    """
    Seen live: a crash-looping pod whose restart counter was already 1 at
    kube-state-metrics' first sample and never moved in Prometheus after that,
    while its container sat in terminated/Error rather than CrashLoopBackOff.
    """

    POD = "chaos-crashloop-fd995fbd-sjsrp"

    def query_range(self, q, start, end, step="30s"):
        t = self.t
        if q == ks.RESTARTS_QUERY:
            return [_series({"pod": self.POD, "container": "app"}, [(t(4), 1), (t(3.5), 1), (t(3), 1)])]
        if q == ks.TERMINATED_REASON_QUERY:
            return [_series({"pod": self.POD, "container": "app", "reason": "Error"}, [(t(4), 1), (t(3), 1)])]
        if q == ks.CREATED_QUERY:
            return [_series({"pod": self.POD}, [(t(4), t(4.3))])]
        if q == ks.TERMINATED_NOW_QUERY:
            return [_series({"pod": self.POD, "container": "app", "reason": "Error"}, [(t(3.5), 1)])]
        return []


def _fetch():
    client = FakeClient()
    return client, ks.fetch_k8s_signals(client, minutes_back=30)


def _crashloop_incident(rule_incidents):
    return next(r for r in rule_incidents if any(s["target"] == "chaos-crashloop" for s in r.k8s_signals))


def test_restarts_use_reason_only_for_restarts_in_window():
    _, signals = _fetch()
    restarts = [s for s in signals if s.kind == "restart"]
    assert len(restarts) == 2 and all(s.reason == "OOMKilled" for s in restarts), restarts
    assert not any(s.target.startswith("flask-deployment-7c9d8f6b5-bbbbb") for s in signals), "sticky reboot Error leaked"


def test_new_pod_already_restarted_and_terminated_now_count():
    signals = ks.fetch_k8s_signals(NewCrashingPodClient(), minutes_back=30)
    restarts = [s for s in signals if s.kind == "restart"]
    assert len(restarts) == 1 and restarts[0].reason == "Error" and restarts[0].value == 1
    assert [s.reason for s in signals if s.kind == "terminated"] == ["Error"]
    incidents = ks.attach_k8s_signals([], signals, WINDOW)
    assert len(incidents) == 1 and incidents[0].workload == "chaos-crashloop"


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


def test_node_pressure_eviction_and_scheduling_signals():
    _, signals = _fetch()
    by_kind = {}
    for s in signals:
        by_kind.setdefault(s.kind, []).append(s)
    assert {s.target for s in by_kind["node_pressure"]} == {"minikube-m02"}
    assert [s.reason for s in by_kind["evicted"]] == ["Evicted"]
    assert [s.reason for s in by_kind["unschedulable"]] == ["FailedScheduling"]
    assert len(by_kind["pending"]) == 4  # the short normal start is ignored

    rule = ks.attach_k8s_signals([], [s for s in signals if s.kind in ("node_pressure", "evicted", "unschedulable", "pending")], WINDOW)
    workloads = {r.workload: r for r in rule}
    assert set(workloads) == {"minikube-m02", "chaos-node-stress"}
    assert all(r.max_severity == "high" for r in rule)


def test_attach_to_ml_incident_and_raise_rule_incidents():
    client, signals = _fetch()
    ts = pd.Timestamp(client.t(9.5), unit="s")
    ml = Incident(
        "mlinc001", ts, ts,
        [Anomaly(ts, {"cpu_percent": 20, "memory_percent": 97, "error_rate": 0}, -0.2, 0.8, "medium")],
        "medium", 0.8,
    )
    classic = [s for s in signals if s.kind in ("restart", "waiting", "replicas_unavailable")]
    rule = ks.attach_k8s_signals([ml], classic, WINDOW)

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
        later = Incident("later001", t, t, [], "medium", 1.0, "rule", workload="chaos-crashloop", k8s_signals=[{
            "kind": "restart", "target": "chaos-crashloop-5d8f7b9c4-x2k4p/busybox", "reason": "Error",
            "first_seen": str(t), "last_seen": str(t), "value": 1,
        }])
        doc3, needs3 = store.save_or_merge(later, WINDOW)
        assert doc3["incident_id"] == first.incident_id and needs3
        assert doc3["end_time"] == str(t) and doc3["max_severity"] == "high"
        assert len(Incident.from_dict(doc3).k8s_signals) == 3
    finally:
        store.client.drop_database(TEST_DB)


def _ml_incident(incident_id, t, cpu, severity):
    return Incident(incident_id, t, t,
                    [Anomaly(t, {"cpu_percent": cpu, "memory_percent": 12, "error_rate": 3}, -0.1, 0.57, severity)],
                    severity, 0.57)


def test_growing_ml_incident_gets_rediagnosed():
    # Seen live in scenario (a): a low-severity blip was diagnosed, then a CPU
    # spike merged into it and kept the stale "low CPU" diagnosis.
    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        print("  (skipped: MongoDB not reachable)")
        return
    store.collection.drop()
    try:
        t0 = pd.Timestamp("2026-09-24 05:50:00")
        doc, _ = store.save_or_merge(_ml_incident("blip0001", t0, 1.4, "low"), WINDOW)
        store.save_diagnosis(doc["incident_id"], {"summary": "low cpu", "based_on_anomalies": 1})

        def add(minutes, cpu, severity):
            t = t0 + pd.Timedelta(minutes=minutes)
            return store.save_or_merge(_ml_incident(f"p{minutes:07.1f}", t, cpu, severity), WINDOW)

        # 1 -> 2 anomalies (doubled) AND low -> medium: re-diagnose
        doc, needs = add(1, 40, "medium")
        assert doc["incident_id"] == "blip0001" and needs
        store.save_diagnosis("blip0001", {"summary": "cpu", "based_on_anomalies": 2})

        # 2 -> 3 anomalies, same severity: keep the diagnosis
        doc, needs = add(1.5, 80, "medium")
        assert not needs, doc["anomaly_count"]

        # 3 -> 4 anomalies = 2x the 2 it was diagnosed on: re-diagnose
        doc, needs = add(2, 93, "medium")
        assert needs and doc["anomaly_count"] == 4

        # legacy diagnosis without based_on_anomalies counts as based on 1
        store.collection.update_one({"incident_id": "blip0001"}, {"$set": {"diagnosis": {"summary": "old"}}})
        _, needs = add(2.5, 94, "medium")
        assert needs
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
