"""
chaos/crashloop.py

Scenario (c): CrashLoopBackOff.

Starts chaos/crashloop-deployment.yaml (a container that exits 1 on
start-up), lets it crash-loop for --minutes, then deletes the Deployment --
always, even on Ctrl+C or error -- and logs the injection to
chaos/injections.jsonl.

Expected: restarts with last terminated reason Error, the container
waiting in CrashLoopBackOff, and chaos-crashloop with an unavailable
replica -- a HIGH rule-based incident for workload chaos-crashloop.

Usage (from the repo root):
    python chaos/crashloop.py              # 4 minutes
    python chaos/crashloop.py --minutes 3
"""

import argparse

from common import CHAOS_DIR, hms, kubectl, log_injection, pod_status, utc_now, watch

SCENARIO = "crashloop"
DEPLOYMENT = "chaos-crashloop"
EXPECTED_KIND = "CrashLoopBackOff"  # failure_kinds.CRASH_LOOP
MANIFEST = CHAOS_DIR / "crashloop-deployment.yaml"


def cleanup():
    print(f"Cleaning up: deleting deployment/{DEPLOYMENT}...")
    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)
    kubectl("wait", "--for=delete", "pod", "-l", f"app={DEPLOYMENT}", "--timeout=60s", check=False)
    print("Cleanup done.")


def main():
    parser = argparse.ArgumentParser(description="CrashLoopBackOff chaos scenario.")
    parser.add_argument("--minutes", type=float, default=4, help="How long to keep the crashing pod running")
    args = parser.parse_args()

    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)  # leftover
    start = utc_now()
    completed = False
    print(f"Starting CrashLoopBackOff scenario at {hms(start)} UTC for {args.minutes:g} min")
    try:
        kubectl("apply", "-f", str(MANIFEST))
        watch(args.minutes * 60, lambda: pod_status(f"app={DEPLOYMENT}"))
        completed = True
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        cleanup()
        log_injection(SCENARIO, start, utc_now(), DEPLOYMENT, EXPECTED_KIND, completed)


if __name__ == "__main__":
    main()
