"""
api.py

A small Flask API that sits between MongoDB and the React dashboard.
The dashboard doesn't talk to MongoDB directly - it calls this API,
which reads incidents (with their attached diagnoses) and returns
them as JSON.

Run this alongside your detection pipeline:
    python api.py
It starts on http://localhost:5050 (API_PORT in ml-engine/.env to change it)
"""

from flask import Flask, jsonify
from flask_cors import CORS
from config import API_PORT
from incident_store import IncidentStore

app = Flask(__name__)
CORS(app)  # allows the React dev server (different port) to call this API

store = IncidentStore()


@app.route("/api/health", methods=["GET"])
def health():
    """Quick check the dashboard can use to confirm the API + Mongo are up."""
    return jsonify({
        "status": "ok",
        "mongo_connected": store.is_connected(),
    })


@app.route("/api/incidents", methods=["GET"])
def get_incidents():
    """Returns the most recent incidents, newest first."""
    incidents = store.get_recent_incidents(limit=50)
    for doc in incidents:
        doc["_id"] = str(doc["_id"])  # ObjectId isn't JSON-serializable by default
    return jsonify(incidents)


@app.route("/api/incidents/severity/<severity>", methods=["GET"])
def get_incidents_by_severity(severity):
    """e.g. /api/incidents/severity/high"""
    incidents = store.get_by_severity(severity)
    for doc in incidents:
        doc["_id"] = str(doc["_id"])
    return jsonify(incidents)


@app.route("/api/incidents/severity", methods=["GET"])
def get_severity_counts():
    """Number of incidents per severity, e.g. {"high": 12, "medium": 30, "low": 9, "total": 51}."""
    counts = store.severity_counts()
    return jsonify({**counts, "total": sum(counts.values())})


@app.route("/api/incidents/<incident_id>", methods=["GET"])
def get_incident(incident_id):
    """One incident by id, e.g. /api/incidents/0b0b98c0, with its diagnosis and evidence; 404 if unknown."""
    doc = store.get_by_id(incident_id)
    if doc is None:
        return jsonify({"error": f"no incident {incident_id}"}), 404
    doc["_id"] = str(doc["_id"])
    return jsonify(doc)


if __name__ == "__main__":
    if not store.is_connected():
        print("Warning: MongoDB not reachable. The dashboard will show no data.")
    app.run(port=API_PORT, debug=True)