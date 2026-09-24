"""
incident_store.py

The "Storage" half of the Correlation & Storage Layer from the
architecture diagram.

Takes Incident objects (produced by correlation_engine.py) and
persists them in MongoDB, so they survive past a single script run
and can later be queried by the Root Cause Analysis layer, the LLM
Explanation layer, and the React dashboard.

Connects to a LOCAL MongoDB instance by default (mongodb://localhost:27017).
If you're running MongoDB inside Minikube/Kubernetes instead of locally,
just change MONGO_URI below.
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import timedelta

import pandas as pd

from correlation_engine import CorrelationEngine, Incident


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "chaos_healer"
COLLECTION_NAME = "incidents"


class IncidentStore:
    """
    Thin wrapper around a MongoDB collection, specifically for
    storing and retrieving Incident objects. Keeping this isolated
    means the rest of the pipeline never has to know it's MongoDB
    underneath — if that ever changes, only this file changes.
    """

    def __init__(self, uri: str = MONGO_URI, db_name: str = DB_NAME):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        self.db = self.client[db_name]
        self.collection = self.db[COLLECTION_NAME]

    def is_connected(self) -> bool:
        """Quick check so callers can fail gracefully with a clear message."""
        try:
            self.client.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    def save_incident(self, incident: Incident) -> str:
        """
        Inserts one incident. Uses incident_id as the unique key so
        re-running detection on the same window doesn't create
        duplicate records.
        """
        doc = incident.to_dict()
        self.collection.update_one(
            {"incident_id": doc["incident_id"]},
            {"$set": doc},
            upsert=True,
        )
        return doc["incident_id"]

    def save_many(self, incidents: list[Incident]) -> int:
        """Bulk save, returns count of incidents written."""
        for incident in incidents:
            self.save_incident(incident)
        return len(incidents)

    def find_overlapping(self, start, end, window: timedelta) -> dict | None:
        """Most recent stored incident whose time range is within `window` of [start, end]."""
        lo, hi = pd.Timestamp(start) - window, pd.Timestamp(end) + window
        for doc in self.collection.find({}, {"_id": 0}).sort("start_time", -1).limit(100):
            if pd.Timestamp(doc["start_time"]) <= hi and pd.Timestamp(doc["end_time"]) >= lo:
                return doc
        return None

    def save_or_merge(self, incident: Incident, window: timedelta) -> tuple[dict, bool]:
        """
        Saves a new incident, or folds it into an existing one that overlaps
        in time -- re-running the pipeline over the same window, or a failure
        that lasts across several --loop polls, is the SAME incident, not a
        new one each time.

        Returns (stored_doc, needs_diagnosis). needs_diagnosis is True for a
        new incident, or a merged one that has no diagnosis yet or just
        gained a kind of Kubernetes failure it didn't have before.
        """
        doc = incident.to_dict()
        existing = self.find_overlapping(incident.start_time, incident.end_time, window)
        if existing is None:
            self.collection.update_one({"incident_id": doc["incident_id"]}, {"$set": doc}, upsert=True)
            return doc, True

        merged = _merge_incident_docs(existing, doc)
        self.collection.update_one({"incident_id": merged["incident_id"]}, {"$set": merged})

        def reasons(d):
            return {(s["kind"], s["reason"]) for s in d.get("k8s_signals", [])}

        needs_diagnosis = "diagnosis" not in existing or bool(reasons(doc) - reasons(existing))
        return merged, needs_diagnosis

    def save_diagnosis(self, incident_id: str, diagnosis: dict) -> None:
        """
        Attaches an RCA/LLM diagnosis to an existing incident document.
        Upserts so this also works if the incident wasn't saved separately
        beforehand (e.g. if you only ran rca_engine.py standalone).
        """
        self.collection.update_one(
            {"incident_id": incident_id},
            {"$set": {"diagnosis": diagnosis}},
            upsert=True,
        )

    def get_recent_incidents(self, limit: int = 20) -> list[dict]:
        """Fetch the most recent incidents, newest first."""
        cursor = self.collection.find().sort("start_time", -1).limit(limit)
        return list(cursor)

    def get_by_severity(self, severity: str) -> list[dict]:
        """e.g. get_by_severity('high') for only the serious ones."""
        cursor = self.collection.find({"max_severity": severity})
        return list(cursor)


def _merge_incident_docs(old: dict, new: dict) -> dict:
    """Combines two incident documents describing the same event. Keeps old's id."""
    anomalies = {a["timestamp"]: a for a in old.get("anomalies", []) + new["anomalies"]}
    anomalies = [anomalies[ts] for ts in sorted(anomalies, key=pd.Timestamp)]

    # Re-scanning an overlapping window sees the same restarts again, so
    # counts take the max rather than the sum.
    signals: dict[tuple, dict] = {}
    for s in old.get("k8s_signals", []) + new["k8s_signals"]:
        key = (s["kind"], s["target"], s["reason"])
        if key not in signals:
            signals[key] = dict(s)
            continue
        g = signals[key]
        g["first_seen"] = min(g["first_seen"], s["first_seen"], key=pd.Timestamp)
        g["last_seen"] = max(g["last_seen"], s["last_seen"], key=pd.Timestamp)
        g["value"] = max(g["value"], s["value"])

    parts = set(old.get("detection", "ml").split("+")) | set(new["detection"].split("+"))
    rank = CorrelationEngine._SEVERITY_RANK

    return {
        "incident_id": old["incident_id"],
        "start_time": min(old["start_time"], new["start_time"], key=pd.Timestamp),
        "end_time": max(old["end_time"], new["end_time"], key=pd.Timestamp),
        "anomaly_count": len(anomalies),
        "max_severity": max(old["max_severity"], new["max_severity"], key=rank.__getitem__),
        "avg_confidence": (
            round(sum(a["confidence"] for a in anomalies) / len(anomalies), 3)
            if anomalies else max(old["avg_confidence"], new["avg_confidence"])
        ),
        "anomalies": anomalies,
        "detection": "+".join(p for p in ("ml", "rule") if p in parts),
        "k8s_signals": list(signals.values()),
    }


if __name__ == "__main__":
    # End-to-end sanity check: generate mock metrics -> detect anomalies
    # -> correlate into incidents -> save to MongoDB -> read them back.
    from anomaly_detector import AnomalyDetector, generate_mock_metrics
    from correlation_engine import CorrelationEngine

    store = IncidentStore()

    if not store.is_connected():
        print("Could not connect to MongoDB at", MONGO_URI)
        print("Make sure your local MongoDB service is running, then re-run this file.")
        exit(1)

    print("Connected to MongoDB successfully.\n")

    # Run the full pipeline so far
    data = generate_mock_metrics()
    feature_cols = ["cpu_percent", "memory_percent", "error_rate"]

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(data, feature_columns=feature_cols)
    anomalies = detector.score_batch(data)

    engine = CorrelationEngine(time_window=timedelta(minutes=3))
    incidents = engine.correlate(anomalies)

    saved_count = store.save_many(incidents)
    print(f"Saved {saved_count} incidents to MongoDB.\n")

    print("Reading back the 5 most recent incidents:")
    for doc in store.get_recent_incidents(limit=5):
        print(
            f"  {doc['incident_id']} | {doc['start_time']} | "
            f"severity={doc['max_severity']} | confidence={doc['avg_confidence']}"
        )