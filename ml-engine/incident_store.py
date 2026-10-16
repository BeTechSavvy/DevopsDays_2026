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
set MONGO_URI in ml-engine/.env (see config.py).

Times are stored twice: as strings (start_time/end_time, what the dashboard
and older code read) and as real BSON datetimes (start_dt/end_dt), which is
what overlap queries and sorting use -- string comparison of timestamps
breaks as soon as two formats are mixed.
"""

from pymongo import DESCENDING, MongoClient
from pymongo.errors import ConnectionFailure
from datetime import timedelta

import pandas as pd

from config import MONGO_DB, MONGO_URI
from correlation_engine import DEFAULT_WORKLOAD, CorrelationEngine, Incident


DB_NAME = MONGO_DB
COLLECTION_NAME = "incidents"


def _as_datetime(value):
    """String or Timestamp -> naive-UTC python datetime, the form MongoDB stores."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.floor("ms").to_pydatetime()  # MongoDB stores milliseconds


def _plain(value):
    """numpy scalars (e.g. a restart count from pandas) -> Python ones; BSON can't encode numpy types."""
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value.item() if hasattr(value, "item") and type(value).__module__ == "numpy" else value


def _with_datetimes(doc: dict) -> dict:
    doc = _plain(doc)
    return {**doc, "start_dt": _as_datetime(doc["start_time"]), "end_dt": _as_datetime(doc["end_time"])}


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

    def backfill_datetimes(self) -> int:
        """Adds start_dt/end_dt to incidents saved before those fields existed. Returns how many."""
        fixed = 0
        for doc in self.collection.find({"start_dt": {"$exists": False}}, {"incident_id": 1, "start_time": 1, "end_time": 1}):
            dates = _with_datetimes(doc)
            self.collection.update_one({"_id": doc["_id"]},
                                       {"$set": {"start_dt": dates["start_dt"], "end_dt": dates["end_dt"]}})
            fixed += 1
        return fixed

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
        doc = _with_datetimes(incident.to_dict())
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

    def find_overlapping(self, start, end, window: timedelta, workload: str | None = None) -> dict | None:
        """
        Most recent stored incident whose time range is within `window` of
        [start, end], compared as real datetimes. With `workload`, only
        incidents of that workload count (older docs without the field are
        the flask app's).
        """
        lo, hi = _as_datetime(start) - window, _as_datetime(end) + window
        query = {"start_dt": {"$lte": hi}, "end_dt": {"$gte": lo}}
        if workload is not None:
            # None also matches a missing field
            query["workload"] = {"$in": [workload, None] if workload == DEFAULT_WORKLOAD else [workload]}
        return self.collection.find_one(query, {"_id": 0}, sort=[("start_dt", DESCENDING)])

    def incident_spans(self, start, end, window: timedelta, workload: str | None = None) -> list[tuple]:
        """
        (start, end) of every stored incident within `window` of [start, end];
        with `workload`, only that workload's incidents.
        """
        lo, hi = _as_datetime(start) - window, _as_datetime(end) + window
        query = {"start_dt": {"$lte": hi}, "end_dt": {"$gte": lo}}
        if workload is not None:
            query["workload"] = {"$in": [workload, None] if workload == DEFAULT_WORKLOAD else [workload]}
        return [
            (pd.Timestamp(d["start_dt"]), pd.Timestamp(d["end_dt"]))
            for d in self.collection.find(query, {"start_dt": 1, "end_dt": 1})
        ]

    def _free_id(self, incident_id: str) -> str:
        """
        Incident ids are deterministic (workload + start bucket), so two
        separate incidents in the same bucket would collide; the second
        gets a -2 suffix, the third -3, and so on.
        """
        candidate, n = incident_id, 1
        while self.collection.count_documents({"incident_id": candidate}, limit=1):
            n += 1
            candidate = f"{incident_id}-{n}"
        return candidate

    def save_or_merge(self, incident: Incident, window: timedelta,
                      create: bool = True) -> tuple[dict | None, bool]:
        """
        Saves a new incident, or folds it into an existing one that overlaps
        in time -- re-running the pipeline over the same window, or a failure
        that lasts across several --loop polls, is the SAME incident, not a
        new one each time.

        Returns (stored_doc, needs_diagnosis). needs_diagnosis is True for a
        new incident, or a merged one that has no diagnosis yet or just
        gained a kind of Kubernetes failure it didn't have before.

        create=False only merges: with no overlapping incident, nothing is
        written and (None, False) is returned.
        """
        doc = incident.to_dict()
        existing = self.find_overlapping(incident.start_time, incident.end_time, window, incident.workload)
        if existing is None and not create:
            return None, False
        if existing is None:
            doc["incident_id"] = self._free_id(doc["incident_id"])
            doc["stored_at"] = str(pd.Timestamp.now(tz="UTC").tz_localize(None))  # for time-to-detect
            self.collection.update_one({"incident_id": doc["incident_id"]}, {"$set": _with_datetimes(doc)}, upsert=True)
            return doc, True

        merged = _merge_incident_docs(existing, doc)
        self.collection.update_one({"incident_id": merged["incident_id"]}, {"$set": _with_datetimes(merged)})

        def reasons(d):
            return {(s["kind"], s["reason"]) for s in d.get("k8s_signals", [])}

        diagnosis = existing.get("diagnosis")
        rank = CorrelationEngine._SEVERITY_RANK
        needs_diagnosis = (
            diagnosis is None
            or bool(reasons(doc) - reasons(existing))                           # new kind of k8s failure
            or rank[merged["max_severity"]] > rank[existing["max_severity"]]    # got worse
            # grew a lot since it was diagnosed (e.g. a small blip turned into a CPU spike);
            # doubling keeps Gemini calls per incident logarithmic, not one per poll
            or merged["anomaly_count"] >= 2 * max(diagnosis.get("based_on_anomalies", 1), 1)
        )
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

    def count_llm_call(self, incident_id: str) -> None:
        """+1 on the incident's llm_calls counter (one per Gemini call, failed ones included)."""
        self.collection.update_one({"incident_id": incident_id}, {"$inc": {"llm_calls": 1}})

    def get_recent_incidents(self, limit: int = 20) -> list[dict]:
        """Fetch the most recent incidents, newest first."""
        cursor = self.collection.find({}, {"start_dt": 0, "end_dt": 0}).sort("start_dt", DESCENDING).limit(limit)
        return list(cursor)

    def get_by_severity(self, severity: str) -> list[dict]:
        """e.g. get_by_severity('high') for only the serious ones."""
        cursor = self.collection.find({"max_severity": severity}, {"start_dt": 0, "end_dt": 0})
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
        if s["kind"] == "predicted_exhaustion":  # seconds to breach: keep the most urgent
            g["value"] = g["time_to_breach"] = min(g["value"], s["value"])
        else:
            g["value"] = max(g["value"], s["value"])

    parts = set(old.get("detection", "ml").split("+")) | set(new["detection"].split("+"))
    rank = CorrelationEngine._SEVERITY_RANK

    return {
        "incident_id": old["incident_id"],
        "workload": old.get("workload", new.get("workload", DEFAULT_WORKLOAD)),
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