"""
demo-db/app.py

A second monitored workload for the DB-connectivity scenario: an "orders"
service that keeps its data in Redis. Instrumented exactly like ../app.py
(prometheus_flask_exporter -> flask_http_request_total{status}), so the
pipeline computes its error_rate the same way.

/orders reads and bumps a counter in Redis. When Redis is unreachable it
logs the connection error and returns 500 -- what a real service without
retries or a circuit breaker does when its database goes away.
"""

import logging
import os

import redis
from flask import Flask, jsonify
from prometheus_flask_exporter import PrometheusMetrics

REDIS_HOST = os.environ.get("REDIS_HOST", "demo-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

app = Flask(__name__)
metrics = PrometheusMetrics(app)  # exposes GET /metrics for Prometheus to scrape
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

# short timeouts: fail fast instead of hanging every request while the DB is down
db = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=0.5, socket_timeout=0.5)


@app.route("/")
def home():
    return "orders service"


@app.route("/orders")
def orders():
    try:
        count = db.incr("orders:count")
        return jsonify({"status": "ok", "orders": count})
    except redis.exceptions.RedisError as e:
        log.error("database error: cannot reach redis at %s:%s: %s", REDIS_HOST, REDIS_PORT, e)
        return jsonify({"error": "database unavailable"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
