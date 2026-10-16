"""
chaos/failed_rollout.py

Scenario (d): failed rollout.

Creates chaos-rollout (a copy of the flask app) and waits for it to be
healthy, then sets its image to a tag that doesn't exist -- the fault.
After --minutes (at least 3) it rolls back with `kubectl rollout undo`,
waits for the rollback to finish, and finally deletes the Deployment --
always, even on Ctrl+C or error -- and logs the injection to
chaos/injections.jsonl. The injection window is from the bad image to the
completed rollback.

Expected: the new pods wait in ErrImagePull / ImagePullBackOff -- a HIGH
rule-based incident for workload chaos-rollout.

Usage (from the repo root):
    python chaos/failed_rollout.py              # bad image for 4 minutes
    python chaos/failed_rollout.py --minutes 3
"""

import argparse

from common import CHAOS_DIR, hms, kubectl, log_injection, pod_status, utc_now, watch

SCENARIO = "failed_rollout"
DEPLOYMENT = "chaos-rollout"
CONTAINER = "app"
BAD_IMAGE = "flask-demo:v2-does-not-exist"
EXPECTED_KIND = "ImagePullBackOff"  # failure_kinds.IMAGE_PULL
MANIFEST = CHAOS_DIR / "rollout-deployment.yaml"
MIN_MINUTES = 3


def cleanup():
    print(f"Cleaning up: deleting deployment/{DEPLOYMENT}...")
    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)
    kubectl("wait", "--for=delete", "pod", "-l", f"app={DEPLOYMENT}", "--timeout=60s", check=False)
    print("Cleanup done.")


def main():
    parser = argparse.ArgumentParser(description="Failed-rollout chaos scenario.")
    parser.add_argument("--minutes", type=float, default=4, help=f"How long the bad image stays (min {MIN_MINUTES})")
    args = parser.parse_args()
    minutes = max(args.minutes, MIN_MINUTES)

    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)  # leftover
    start = None
    completed = False
    try:
        print(f"Creating healthy deployment/{DEPLOYMENT} first...")
        kubectl("apply", "-f", str(MANIFEST))
        kubectl("rollout", "status", f"deployment/{DEPLOYMENT}", "--timeout=180s")

        start = utc_now()
        print(f"Starting failed rollout at {hms(start)} UTC: image -> {BAD_IMAGE} for {minutes:g} min")
        kubectl("set", "image", f"deployment/{DEPLOYMENT}", f"{CONTAINER}={BAD_IMAGE}")
        watch(minutes * 60, lambda: pod_status(f"app={DEPLOYMENT}"))

        print("Rolling back...")
        kubectl("rollout", "undo", f"deployment/{DEPLOYMENT}")
        kubectl("rollout", "status", f"deployment/{DEPLOYMENT}", "--timeout=180s")
        print(f"Rollback complete at {hms()} UTC.")
        completed = True
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        end = utc_now()
        cleanup()
        if start is not None:  # the fault was never injected if setup failed
            log_injection(SCENARIO, start, end, DEPLOYMENT, EXPECTED_KIND, completed)


if __name__ == "__main__":
    main()
