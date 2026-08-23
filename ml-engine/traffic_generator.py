"""
traffic_generator.py

Drives traffic against the Flask app in three phases so the anomaly
detector has something real and unambiguous to catch, instead of
relying on /work's random 10% failure rate (which might not spike
enough to register as an "anomaly" at all).

    Phase 1 (baseline):   normal /work traffic, low error rate
    Phase 2 (anomaly):    deliberate error + CPU spikes via /spike-errors
                          and /spike-load
    Phase 3 (baseline):   back to normal /work traffic

Run this against a `kubectl port-forward svc/flask-service 30001:5000`
tunnel rather than the NodePort directly -- port-forward has been the
one reliable path to reach anything in the cluster all session,
whereas NodePort-via-localhost is known to be flaky with Minikube's
Docker driver on Windows.

Usage:
    kubectl port-forward svc/flask-service 30001:5000   # separate terminal, leave running
    python traffic_generator.py
"""

import os
import time

import requests

BASE_URL = os.environ.get("FLASK_URL", "http://127.0.0.1:30001")

BASELINE_SECONDS = int(os.environ.get("BASELINE_SECONDS", 60))
SPIKE_SECONDS = int(os.environ.get("SPIKE_SECONDS", 60))
REQUEST_DELAY = 0.3  # seconds between requests


def hit(path: str):
    try:
        requests.get(f"{BASE_URL}{path}", timeout=5)
    except requests.RequestException as e:
        print(f"  request to {path} failed: {e}")


def run_phase(label: str, path: str, duration_seconds: int):
    print(f"\n--- {label} ({duration_seconds}s, hitting {path}) ---")
    end = time.time() + duration_seconds
    count = 0
    while time.time() < end:
        hit(path)
        count += 1
        time.sleep(REQUEST_DELAY)
    print(f"--- {label} done: {count} requests sent ---")


def main():
    print(f"Target: {BASE_URL}")
    try:
        r = requests.get(f"{BASE_URL}/", timeout=5)
        print(f"Connectivity check: {r.status_code} {r.text[:60]}")
    except requests.RequestException as e:
        print(f"Cannot reach {BASE_URL}: {e}")
        print("Make sure 'kubectl port-forward svc/flask-service 30001:5000' is running in another terminal.")
        raise SystemExit(1)

    run_phase("Phase 1: baseline traffic", "/work", BASELINE_SECONDS)
    run_phase("Phase 2: deliberate error spike", "/spike-errors", SPIKE_SECONDS // 2)
    run_phase("Phase 2: deliberate load spike", "/spike-load", SPIKE_SECONDS // 2)
    run_phase("Phase 3: back to baseline", "/work", BASELINE_SECONDS)

    print("\nDone. Give Prometheus ~30-60s to scrape the last data points, "
          "then run pipeline.py -- the spike should show up as a real incident.")


if __name__ == "__main__":
    main()