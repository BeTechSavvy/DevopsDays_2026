"""
chaos/db_failure.py

Scenario (f): database connectivity loss.

Scales demo-redis (the orders service's database, demo-db/demo-db.yaml)
to 0 replicas for --minutes, then back to 1 and waits until it is ready
again -- always, even on Ctrl+C or error -- and logs the injection to
chaos/injections.jsonl.

Expected: demo-db-app keeps receiving traffic (chaos/baseline-traffic.yaml)
and answers 500 "database unavailable", so its error_rate jumps to ~100%
-- an ML incident for workload demo-db-app, with Loki lines "database
error: cannot reach redis" as evidence (kind db_connection).

Usage (from the repo root):
    python chaos/db_failure.py              # 4 minutes
    python chaos/db_failure.py --minutes 3
"""

import argparse

from common import hms, kubectl, log_injection, utc_now, watch

SCENARIO = "db_failure"
DB_DEPLOYMENT = "demo-redis"
TARGET = "demo-db-app"  # where the symptoms show: the service that depends on the DB
EXPECTED_KIND = "db_connection"  # failure_kinds.DB_CONNECTION


def replicas() -> str:
    return kubectl("get", "deployment", DB_DEPLOYMENT, "-o",
                   "jsonpath={.status.readyReplicas}/{.spec.replicas} ready", check=False) or "?"


def restore():
    print(f"Restoring: scaling deployment/{DB_DEPLOYMENT} back to 1...")
    kubectl("scale", "deployment", DB_DEPLOYMENT, "--replicas=1", check=False)
    kubectl("rollout", "status", f"deployment/{DB_DEPLOYMENT}", "--timeout=120s", check=False)
    print(f"Restored ({replicas()}).")


def main():
    parser = argparse.ArgumentParser(description="DB connectivity chaos scenario.")
    parser.add_argument("--minutes", type=float, default=4, help="How long the database stays down")
    args = parser.parse_args()

    kubectl("get", "deployment", DB_DEPLOYMENT)  # fails clearly if demo-db isn't deployed
    start = utc_now()
    completed = False
    print(f"Starting DB failure at {hms(start)} UTC: {DB_DEPLOYMENT} -> 0 replicas for {args.minutes:g} min")
    try:
        kubectl("scale", "deployment", DB_DEPLOYMENT, "--replicas=0")
        watch(args.minutes * 60, lambda: f"{DB_DEPLOYMENT}: {replicas()}")
        completed = True
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        restore()
        log_injection(SCENARIO, start, utc_now(), TARGET, EXPECTED_KIND, completed)


if __name__ == "__main__":
    main()
