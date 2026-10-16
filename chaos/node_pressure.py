"""
chaos/node_pressure.py

Scenario (e): node memory pressure on node 2.

Checks that node 2's kubelet evicts on memory (memory.available in
evictionHard -- not set in minikube's default config), then starts
chaos/node-stress.yaml (a BestEffort pod pinned to node 2 that takes
~1.6 GiB), watches the node condition and the pod for --minutes, then
deletes the Deployment and evicted pods -- always, even on Ctrl+C or error
-- and logs the injection to chaos/injections.jsonl.

Expected: kube_node_status_condition MemoryPressure=true for minikube-m02,
the stress pod Evicted, its replacement Pending/unschedulable -- a HIGH
rule-based incident for node minikube-m02 / workload chaos-node-stress.

Usage (from the repo root):
    python chaos/node_pressure.py              # 4 minutes
    python chaos/node_pressure.py --minutes 3
"""

import argparse
import json

from common import CHAOS_DIR, hms, kubectl, log_injection, pod_status, utc_now, watch

SCENARIO = "node_pressure"
NODE = "minikube-m02"
DEPLOYMENT = "chaos-node-stress"
TARGET = f"{NODE}|{DEPLOYMENT}"  # node-level and pod-level signals both count
EXPECTED_KIND = "node_pressure"
MANIFEST = CHAOS_DIR / "node-stress.yaml"


def eviction_threshold() -> str | None:
    """Node 2 kubelet's evictionHard memory.available, via the API server's configz proxy."""
    raw = kubectl("get", "--raw", f"/api/v1/nodes/{NODE}/proxy/configz", check=False)
    try:
        return json.loads(raw)["kubeletconfig"].get("evictionHard", {}).get("memory.available")
    except (ValueError, KeyError):
        return None


def node_condition() -> str:
    out = kubectl("get", "node", NODE, "-o",
                  'jsonpath={range .status.conditions[?(@.status=="True")]}{.type} {end}', check=False)
    return out.strip() or "unknown"


def cleanup():
    print(f"Cleaning up: deleting deployment/{DEPLOYMENT} and its evicted pods...")
    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)
    kubectl("delete", "pods", "-l", f"app={DEPLOYMENT}", "--ignore-not-found", "--wait=true", check=False)
    print("Cleanup done. MemoryPressure clears once the kubelet sees memory free again.")


def main():
    parser = argparse.ArgumentParser(description="Node memory-pressure chaos scenario.")
    parser.add_argument("--minutes", type=float, default=4, help="How long to keep the stress pod around")
    args = parser.parse_args()

    threshold = eviction_threshold()
    if not threshold:
        raise SystemExit(
            f"{NODE}'s kubelet has no memory.available eviction threshold, so it can never report "
            "MemoryPressure -- the node container would just be OOM-killed. Not injecting.\n"
            "Set it up first: see RUNBOOK.md, 'Node pressure setup'."
        )
    print(f"{NODE} evicts when memory.available < {threshold}.")

    kubectl("delete", "deployment", DEPLOYMENT, "--ignore-not-found", "--wait=true", check=False)  # leftover
    start = utc_now()
    completed = False
    print(f"Starting node-pressure scenario at {hms(start)} UTC for {args.minutes:g} min")
    try:
        kubectl("apply", "-f", str(MANIFEST))
        watch(args.minutes * 60, lambda: f"node: {node_condition()} | {pod_status(f'app={DEPLOYMENT}')}")
        completed = True
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        cleanup()
        log_injection(SCENARIO, start, utc_now(), TARGET, EXPECTED_KIND, completed)


if __name__ == "__main__":
    main()
