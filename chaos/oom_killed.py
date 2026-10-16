"""
chaos/oom_killed.py

Scenario (b): OOMKilled.

Starts chaos/oom-deployment.yaml (a pod leaking 0.5 MiB/s under a 128Mi
limit), lets it get OOM-killed and restarted for --minutes, printing the
pod's restarts as it goes, then deletes the Deployment -- always, even on
Ctrl+C or error -- and logs the injection to chaos/injections.jsonl.

Expected: memory climbs ~0.5 MiB/s until the pod is OOM-killed (~4 min),
then kube-state-metrics reports restarts with last terminated reason
OOMKilled (then CrashLoopBackOff), which k8s_signals turns into a HIGH
rule-based incident for workload chaos-oom within 1-2 pipeline polls.

Usage (from the repo root):
    python chaos/oom_killed.py              # 6 minutes
    python chaos/oom_killed.py --minutes 8
"""

import argparse

from common import CHAOS_DIR, hms, kubectl, log_injection, pod_status, utc_now, watch

SCENARIO = "oom_killed"
DEPLOYMENT = "chaos-oom"
EXPECTED_KIND = "OOMKilled"  # failure_kinds.OOM_KILLED
MANIFEST = CHAOS_DIR / "oom-deployment.yaml"


def cleanup():
    print(f"Cleaning up: deleting deployment/{DEPLOYMENT}...")
    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)
    kubectl("wait", "--for=delete", "pod", "-l", f"app={DEPLOYMENT}", "--timeout=60s", check=False)
    print("Cleanup done.")


def main():
    parser = argparse.ArgumentParser(description="OOMKilled chaos scenario.")
    parser.add_argument("--minutes", type=float, default=6,
                        help="How long to keep the leaking pod running (first OOM kill after ~4 min)")
    args = parser.parse_args()

    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)  # leftover
    start = utc_now()
    completed = False
    print(f"Starting OOMKilled scenario at {hms(start)} UTC for {args.minutes:g} min")
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
