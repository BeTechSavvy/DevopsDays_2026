"""
app.py

The monitored workload. Originally a bare "Hello World" Flask route
with no observability of its own -- CPU/memory could be scraped from
cAdvisor, but there was no source at all for error_rate, since the
app never reported anything about its own requests.

prometheus_flask_exporter adds a /metrics endpoint automatically and
tracks flask_http_request_total{method, path, status} for every
request, which is exactly what ml-engine/prometheus_source.py queries
to build the error_rate signal.

/work is new: it deliberately fails ~10% of the time so there's real
error signal for the anomaly detector to find, instead of an
always-green counter that never gives Isolation Forest anything to
flag as abnormal.
"""

import random
import time

from flask import Flask, jsonify
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
metrics = PrometheusMetrics(app)  # exposes GET /metrics for Prometheus to scrape


@app.route("/")
def home():
    return "Hello from Minikube Kubernetes!"


@app.route("/work")
def work():
    """Simulated workload endpoint with a realistic baseline failure rate."""
    if random.random() < 0.10:
        return jsonify({"error": "simulated failure"}), 500
    return jsonify({"status": "ok"})


@app.route("/spike-errors")
def spike_errors():
    """Always fails. Used deliberately to spike error_rate for demo/testing."""
    return jsonify({"error": "deliberate spike"}), 500


@app.route("/spike-load")
def spike_load():
    """Burns CPU briefly on purpose, to spike cpu_percent for demo/testing."""
    end = time.time() + 0.3
    x = 0
    while time.time() < end:
        x += 1
    return jsonify({"status": "load generated", "iterations": x})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)