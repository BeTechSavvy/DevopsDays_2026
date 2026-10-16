"""
chaos/cpu_exhaustion.py

Scenario (a): CPU exhaustion.

Starts chaos/cpu-load-job.yaml (in-cluster load on /spike-load across both
flask pods for ~3 minutes), prints cpu_percent from Prometheus while it runs,
then deletes the Job -- always, even on Ctrl+C or error. Recovery is automatic:
once the load stops, CPU drops back to normal within ~2 minutes (the rate window).

Usage (from the repo root; Prometheus port-forward on 9090 for the live readout):
    python chaos/cpu_exhaustion.py
"""

import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ml-engine"))
from prometheus_source import CPU_QUERY, PROM_URL  # noqa: E402

JOB = "chaos-cpu-load"
MANIFEST = Path(__file__).with_name("cpu-load-job.yaml")
TIMEOUT_SECONDS = 300  # matches activeDeadlineSeconds in the manifest
POLL_SECONDS = 15


def kubectl(*args, check=True) -> str:
    result = subprocess.run(["kubectl", *args], text=True, capture_output=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def utc_now() -> str:
    # incidents and the dashboard use UTC, so the script does too
    return time.strftime("%H:%M:%S", time.gmtime())


def cpu_percent() -> str:
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": CPU_QUERY}, timeout=5)
        result = r.json()["data"]["result"]
        return f"{float(result[0]['value'][1]):.1f}%" if result else "n/a"
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return "n/a"


def cleanup():
    print(f"Cleaning up: deleting job/{JOB}...")
    kubectl("delete", "job", JOB, "--ignore-not-found", "--cascade=foreground", "--wait=true", check=False)
    print("Cleanup done. CPU returns to normal within ~2 minutes.")


def main():
    kubectl("delete", "job", JOB, "--ignore-not-found", "--wait=true", check=False)  # leftover from an old run
    print(f"Starting CPU exhaustion at {utc_now()} UTC (cpu_percent now: {cpu_percent()})")
    kubectl("apply", "-f", str(MANIFEST))

    try:
        deadline = time.time() + TIMEOUT_SECONDS
        while time.time() < deadline:
            time.sleep(POLL_SECONDS)
            status = kubectl("get", "job", JOB, "-o",
                             "jsonpath={.status.succeeded}/{.status.failed}", check=False)
            print(f"  {utc_now()}  cpu_percent={cpu_percent()}")
            succeeded, _, failed = status.partition("/")
            if succeeded == "1":
                print("Load finished.")
                print(kubectl("logs", f"job/{JOB}", check=False))
                break
            if failed and failed != "0":
                print("Load job failed:")
                print(kubectl("logs", f"job/{JOB}", check=False))
                break
        else:
            print("Timed out waiting for the load job.")
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
