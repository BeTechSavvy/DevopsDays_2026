"""
correlation_engine.py

The "Correlation & Storage Layer" from the architecture diagram.

A single real-world incident usually shows up as SEVERAL related
anomalies at once — e.g. a CPU spike + a memory spike + an error-rate
jump, all within the same couple of minutes. Left ungrouped, the
detector would report these as 3 separate anomalies, which is noisy
and unhelpful for a human (or an LLM) trying to diagnose what happened.

This module groups anomalies that are close together in time into a
single Incident object. That Incident is what gets stored (MongoDB)
and handed to the next layer (Root Cause Analysis / LLM Explanation).
"""

from dataclasses import dataclass, field
from datetime import timedelta
import pandas as pd
import uuid

from anomaly_detector import Anomaly


@dataclass
class Incident:
    """
    A cluster of related anomalies, treated as ONE event.
    This is the object that gets stored in MongoDB and passed
    downstream to root cause analysis / the LLM explanation layer.
    """
    incident_id: str
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    anomalies: list[Anomaly] = field(default_factory=list)
    max_severity: str = "low"
    avg_confidence: float = 0.0
    detection: str = "ml"  # "ml" (Isolation Forest), "rule" (k8s_signals only) or "ml+rule"
    k8s_signals: list[dict] = field(default_factory=list)  # summaries from k8s_signals.py

    @classmethod
    def from_dict(cls, doc: dict) -> "Incident":
        """Rebuilds an Incident from its MongoDB document (the inverse of to_dict)."""
        return cls(
            incident_id=doc["incident_id"],
            start_time=pd.Timestamp(doc["start_time"]),
            end_time=pd.Timestamp(doc["end_time"]),
            anomalies=[
                Anomaly(
                    timestamp=pd.Timestamp(a["timestamp"]),
                    metric_snapshot=a["metrics"],
                    anomaly_score=float("nan"),  # not stored
                    confidence=a["confidence"],
                    severity=a["severity"],
                )
                for a in doc.get("anomalies", [])
            ],
            max_severity=doc["max_severity"],
            avg_confidence=doc["avg_confidence"],
            detection=doc.get("detection", "ml"),
            k8s_signals=doc.get("k8s_signals", []),
        )

    def to_dict(self) -> dict:
        """Flat representation, ready to insert into MongoDB."""
        return {
            "incident_id": self.incident_id,
            "start_time": str(self.start_time),
            "end_time": str(self.end_time),
            "anomaly_count": len(self.anomalies),
            "max_severity": self.max_severity,
            "avg_confidence": round(self.avg_confidence, 3),
            "anomalies": [
                {
                    "timestamp": str(a.timestamp),
                    "metrics": a.metric_snapshot,
                    "confidence": a.confidence,
                    "severity": a.severity,
                }
                for a in self.anomalies
            ],
            "detection": self.detection,
            "k8s_signals": self.k8s_signals,
        }


class CorrelationEngine:
    """
    Groups anomalies into incidents based on time proximity.

    Two anomalies are considered part of the same incident if they
    occur within `time_window` of each other. This is a simple but
    effective heuristic — a real cluster failure rarely announces
    itself as a single isolated data point.
    """

    _SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}

    def __init__(self, time_window: timedelta = timedelta(minutes=3)):
        self.time_window = time_window

    def correlate(self, anomalies: list[Anomaly]) -> list[Incident]:
        """
        Takes a flat list of anomalies (already sorted or not) and
        groups them into Incidents based on time proximity.
        """
        if not anomalies:
            return []

        sorted_anomalies = sorted(anomalies, key=lambda a: a.timestamp)

        incidents: list[Incident] = []
        current_group: list[Anomaly] = [sorted_anomalies[0]]

        for anomaly in sorted_anomalies[1:]:
            time_since_last = anomaly.timestamp - current_group[-1].timestamp
            if time_since_last <= self.time_window:
                # Close enough in time -> part of the same ongoing incident
                current_group.append(anomaly)
            else:
                # Gap too large -> close out current incident, start a new one
                incidents.append(self._build_incident(current_group))
                current_group = [anomaly]

        incidents.append(self._build_incident(current_group))
        return incidents

    def _build_incident(self, group: list[Anomaly]) -> Incident:
        max_severity = max(group, key=lambda a: self._SEVERITY_RANK[a.severity]).severity
        avg_confidence = sum(a.confidence for a in group) / len(group)

        return Incident(
            incident_id=str(uuid.uuid4())[:8],
            start_time=group[0].timestamp,
            end_time=group[-1].timestamp,
            anomalies=group,
            max_severity=max_severity,
            avg_confidence=avg_confidence,
        )


if __name__ == "__main__":
    # Sanity check: run the anomaly detector on mock data, then
    # correlate the results and see how many distinct incidents
    # they collapse into.
    from anomaly_detector import AnomalyDetector, generate_mock_metrics

    data = generate_mock_metrics()
    feature_cols = ["cpu_percent", "memory_percent", "error_rate"]

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(data, feature_columns=feature_cols)
    anomalies = detector.score_batch(data)

    engine = CorrelationEngine(time_window=timedelta(minutes=3))
    incidents = engine.correlate(anomalies)

    print(f"Raw anomalies detected: {len(anomalies)}")
    print(f"Grouped into incidents: {len(incidents)}\n")

    for inc in incidents:
        print(
            f"Incident {inc.incident_id} | {inc.start_time} -> {inc.end_time} | "
            f"{len(inc.anomalies)} anomalies | severity={inc.max_severity} | "
            f"avg_confidence={inc.avg_confidence:.2f}"
        )
        