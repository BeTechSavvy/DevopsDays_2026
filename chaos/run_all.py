"""
chaos/run_all.py

Runs the chaos scenarios one after another, with a cool-down gap between
them, several rounds over -- the experiment eval/evaluate.py scores. Each
scenario script cleans up and appends its own line to chaos/injections.jsonl.

Keep `python pipeline.py --loop` (from ml-engine/) running the whole time,
with baseline traffic on (chaos/baseline-traffic.yaml) and port-forwards.ps1
running (error_spike goes through the Flask port-forward). node_pressure
needs chaos/setup-node-pressure.ps1 and db_failure needs demo-db/ deployed;
see RUNBOOK.md. The gap must be
longer than the correlation window plus a couple of polls, so one
scenario's tail can't merge into the next one's incident.

Ctrl+C stops after the running scenario has cleaned up (it gets the
Ctrl+C too and runs its own cleanup).

Usage (from the repo root):
    python chaos/run_all.py                         # all scenarios, 3 rounds, 6-min gaps
    python chaos/run_all.py --repeats 1 --gap 4
    python chaos/run_all.py --only oom_killed crashloop
    python chaos/run_all.py --dry-run               # print the schedule only
"""

import argparse
import subprocess
import sys
import time
from datetime import timedelta

from common import CHAOS_DIR, hms, utc_now

# name -> script, in run order
SCENARIOS = {
    "cpu_exhaustion": "cpu_exhaustion.py",
    "oom_killed": "oom_killed.py",
    "crashloop": "crashloop.py",
    "failed_rollout": "failed_rollout.py",
    "node_pressure": "node_pressure.py",
    "db_failure": "db_failure.py",
    # validation row: /spike-errors through the Flask port-forward (:30001)
    "error_spike": "../ml-engine/traffic_generator.py",
}
# scenarios that take --minutes (cpu_exhaustion's length is fixed by its Job)
TAKES_MINUTES = {"oom_killed", "crashloop", "failed_rollout", "node_pressure", "db_failure"}


def run_scenario(name: str, minutes: float | None) -> int:
    cmd = [sys.executable, str((CHAOS_DIR / SCENARIOS[name]).resolve())]
    if minutes is not None and name in TAKES_MINUTES:
        cmd += ["--minutes", str(minutes)]
    proc = subprocess.Popen(cmd)
    try:
        return proc.wait()
    except KeyboardInterrupt:
        # the child got the same Ctrl+C; let it finish its cleanup + log line
        print("\nWaiting for the scenario to clean up...")
        proc.wait()
        raise


def main():
    parser = argparse.ArgumentParser(description="Run all chaos scenarios in sequence.")
    parser.add_argument("--repeats", type=int, default=3, help="Rounds over all scenarios (default 3)")
    parser.add_argument("--gap", type=float, default=6, help="Cool-down minutes between scenarios (default 6)")
    parser.add_argument("--minutes", type=float, default=None, help="Override each scenario's fault duration")
    parser.add_argument("--only", nargs="+", choices=list(SCENARIOS), help="Run just these scenarios")
    parser.add_argument("--dry-run", action="store_true", help="Print the schedule and exit")
    args = parser.parse_args()

    names = args.only or list(SCENARIOS)
    schedule = [(r + 1, n) for r in range(args.repeats) for n in names]
    print(f"{len(schedule)} runs: {args.repeats} round(s) of {', '.join(names)}; {args.gap:g}-min gaps.")
    if args.dry_run:
        for i, (r, n) in enumerate(schedule, 1):
            print(f"  {i:2d}. round {r}: {n}")
        return

    failures = []
    try:
        for i, (r, name) in enumerate(schedule, 1):
            print(f"\n=== [{hms()} UTC] run {i}/{len(schedule)} (round {r}): {name} ===")
            code = run_scenario(name, args.minutes)
            if code != 0:
                failures.append((i, name, code))
                print(f"WARNING: {name} exited with code {code}.")
            if i < len(schedule):
                print(f"Cool-down: {args.gap:g} min (until {hms(utc_now() + timedelta(minutes=args.gap))} UTC)...")
                time.sleep(args.gap * 60)
    except KeyboardInterrupt:
        print("\nStopped by Ctrl+C.")
        raise SystemExit(130)

    print(f"\nAll runs done at {hms()} UTC. Failures: {failures or 'none'}")
    print("Next: python eval/evaluate.py")


if __name__ == "__main__":
    main()
