"""
chaos/baseline_traffic.py

Steady, light /work traffic so the anomaly detector has a realistic
"normal" to learn before a chaos scenario is triggered (/work fails ~10%
of the time, which is the normal error rate). Stops by itself.

Usage (Flask port-forward on 30001 must already be running):
    python chaos/baseline_traffic.py              # 12 minutes
    python chaos/baseline_traffic.py --minutes 20

Leave it running in its own terminal while you start
`pipeline.py --loop` and trigger a scenario.
"""

import argparse
import os
import time

import requests

BASE_URL = os.environ.get("FLASK_URL", "http://127.0.0.1:30001")


def main():
    parser = argparse.ArgumentParser(description="Send steady /work traffic to the Flask app.")
    parser.add_argument("--minutes", type=float, default=12, help="How long to run")
    parser.add_argument("--delay", type=float, default=0.3, help="Seconds between requests")
    args = parser.parse_args()

    end = time.time() + args.minutes * 60
    sent = failed = 0
    print(f"Sending /work traffic to {BASE_URL} for {args.minutes:g} min. Ctrl+C to stop.")
    try:
        while time.time() < end:
            try:
                requests.get(f"{BASE_URL}/work", timeout=5)
            except requests.RequestException:
                failed += 1
            sent += 1
            if sent % 500 == 0:
                print(f"  {sent} requests sent ({failed} connection failures)")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopped early.")
    print(f"Done: {sent} requests sent, {failed} connection failures.")


if __name__ == "__main__":
    main()
