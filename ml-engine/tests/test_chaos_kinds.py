"""
tests/test_chaos_kinds.py

Checks that every chaos/ scenario's expected_kind is something the
detection side can actually produce, and that k8s_signals' scoping regexes
cover the chaos workloads' pod and deployment names. For each scenario it
builds the signals kube-state-metrics reports for that failure, runs them
through attach_k8s_signals, and classifies the resulting incident.

Run from ml-engine/:
    python -m pytest tests/test_chaos_kinds.py
"""

import importlib.util
import os
import re
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import k8s_signals as ks  # noqa: E402
from anomaly_detector import Anomaly  # noqa: E402
from correlation_engine import CorrelationEngine  # noqa: E402
from failure_kinds import incident_kinds, primary_kind  # noqa: E402

CHAOS_DIR = Path(__file__).resolve().parents[2] / "chaos"
WINDOW = timedelta(minutes=3)
T0 = pd.Timestamp("2026-10-16 10:00:00")


def _scenario(script: str):
    """Imports a chaos script as a module (its constants only; main() isn't run)."""
    sys.path.insert(0, str(CHAOS_DIR))
    spec = importlib.util.spec_from_file_location(script, CHAOS_DIR / f"{script}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sig(kind, target, reason, minutes=0, value=1):
    return ks.K8sSignal(T0 + pd.Timedelta(minutes=minutes), kind, target, reason, value)


# what kube-state-metrics reports for each failure (pod names as a ReplicaSet makes them)
OBSERVED = {
    "oom_killed": [
        _sig("restart", "chaos-oom-5f7d8c9b4-q2w3e/leaker", "OOMKilled", 0.5),
        _sig("restart", "chaos-oom-5f7d8c9b4-q2w3e/leaker", "OOMKilled", 1),
        _sig("waiting", "chaos-oom-5f7d8c9b4-q2w3e/leaker", "CrashLoopBackOff", 1.5),
    ],
    "crashloop": [
        _sig("restart", "chaos-crashloop-6b8c9d7f5-r4t5y/app", "Error", 0.5),
        _sig("waiting", "chaos-crashloop-6b8c9d7f5-r4t5y/app", "CrashLoopBackOff", 1),
        _sig("replicas_unavailable", "chaos-crashloop", "ReplicasUnavailable", 1.5),
    ],
    "failed_rollout": [
        _sig("waiting", "chaos-rollout-7c9d8e6f4-u6i7o/app", "ErrImagePull", 0.5),
        _sig("waiting", "chaos-rollout-7c9d8e6f4-u6i7o/app", "ImagePullBackOff", 1),
    ],
}


def test_scoping_regex_matches_chaos_names():
    pod_re = re.compile(f"^(?:{ks.K8S_POD_REGEX})$")  # PromQL =~ is fully anchored
    dep_re = re.compile(f"^(?:{ks.K8S_DEPLOYMENT_REGEX})$")
    for module in ("oom_killed", "crashloop", "failed_rollout", "node_pressure"):
        deployment = _scenario(module).DEPLOYMENT
        assert dep_re.match(deployment), deployment
        assert pod_re.match(f"{deployment}-5f7d8c9b4-q2w3e"), deployment
    assert not pod_re.match("baseline-traffic-5f7d8c9b4-q2w3e")  # traffic generator is not a target


def test_rule_scenarios_produce_their_expected_kind():
    for name, signals in OBSERVED.items():
        module = _scenario(name)
        incidents = ks.attach_k8s_signals([], signals, WINDOW)
        assert len(incidents) == 1, (name, incidents)
        incident = incidents[0]
        assert incident.workload == module.DEPLOYMENT, (name, incident.workload)
        assert incident.max_severity == "high"
        doc = incident.to_dict()
        assert module.EXPECTED_KIND in incident_kinds(doc), (name, incident_kinds(doc))
        assert primary_kind(doc) == module.EXPECTED_KIND, (name, primary_kind(doc))


def test_node_pressure_produces_node_pressure_for_both_targets():
    module = _scenario("node_pressure")
    signals = [
        _sig("node_pressure", module.NODE, "MemoryPressure", 0.5),
        _sig("evicted", "chaos-node-stress-6d5f8b7c9-k2j3h/pod", "Evicted", 1),
        _sig("unschedulable", "chaos-node-stress-6d5f8b7c9-m4n5b/pod", "FailedScheduling", 1.5),
    ]
    incidents = ks.attach_k8s_signals([], signals, WINDOW)
    assert {i.workload for i in incidents} == set(module.TARGET.split("|"))
    for incident in incidents:  # each incident on its own already says node pressure
        assert module.EXPECTED_KIND in incident_kinds(incident.to_dict()), incident.workload


def test_db_failure_needs_errors_and_db_log_lines():
    module = _scenario("db_failure")
    anomalies = [Anomaly(T0 + pd.Timedelta(seconds=30 * i),
                         {"cpu_percent": 3.0, "memory_percent": 20.0, "error_rate": 100.0}, -0.2, 0.8, "high")
                 for i in range(4)]
    doc = CorrelationEngine(WINDOW, workload=module.TARGET).correlate(anomalies)[0].to_dict()
    assert primary_kind(doc) == "error_spike"  # errors alone: could be anything
    doc["evidence"] = {"logs": ["2026-10-16 10:00:01,123 ERROR database error: cannot reach redis at "
                                "demo-redis:6379: Error 111 connecting to demo-redis:6379. Connection refused."]}
    assert primary_kind(doc) == module.EXPECTED_KIND


def test_cpu_scenario_produces_cpu_saturation():
    module = _scenario("cpu_exhaustion")
    anomalies = [Anomaly(T0 + pd.Timedelta(seconds=30 * i),
                         {"cpu_percent": 94.0, "memory_percent": 12.0, "error_rate": 9.0}, -0.1, 0.6, "high")
                 for i in range(4)]
    incident = CorrelationEngine(WINDOW).correlate(anomalies)[0]
    assert incident.workload == module.TARGET
    assert primary_kind(incident.to_dict()) == module.EXPECTED_KIND
