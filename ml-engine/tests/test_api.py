"""
tests/test_api.py

The dashboard API on a throwaway MongoDB database (skipped if MongoDB is down):
the existing routes keep working, plus GET /api/incidents/<id> and
GET /api/incidents/severity (counts).

Run from ml-engine/:
    python -m pytest tests/test_api.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import api  # noqa: E402
from incident_store import IncidentStore, _with_datetimes  # noqa: E402

TEST_DB = "chaos_healer_api_test"


def _client():
    store = IncidentStore(db_name=TEST_DB)
    if not store.is_connected():
        return None, None
    store.collection.drop()
    for i, (sev, start) in enumerate([("high", "2026-10-16 09:00:00"), ("high", "2026-10-16 09:10:00"),
                                      ("low", "2026-10-16 09:20:00")]):
        doc = {"incident_id": f"inc{i}", "workload": "flask-deployment", "start_time": start, "end_time": start,
               "max_severity": sev, "anomalies": [], "anomaly_count": 0, "diagnosis": {"root_cause": f"cause {i}"}}
        store.collection.insert_one(_with_datetimes(doc))
    api.store = store
    return api.app.test_client(), store


def test_routes():
    client, store = _client()
    if client is None:
        print("  (skipped: MongoDB not reachable)")
        return
    try:
        r = client.get("/api/incidents/inc1")
        assert r.status_code == 200
        body = r.get_json()
        assert body["incident_id"] == "inc1" and body["diagnosis"]["root_cause"] == "cause 1"
        assert "start_dt" not in body and isinstance(body["_id"], str)

        assert client.get("/api/incidents/nope").status_code == 404

        assert client.get("/api/incidents/severity").get_json() == {"high": 2, "medium": 0, "low": 1, "total": 3}

        # unchanged routes the dashboard uses
        assert [d["incident_id"] for d in client.get("/api/incidents").get_json()] == ["inc2", "inc1", "inc0"]
        assert len(client.get("/api/incidents/severity/high").get_json()) == 2
        assert client.get("/api/health").get_json()["status"] == "ok"
    finally:
        store.client.drop_database(TEST_DB)
