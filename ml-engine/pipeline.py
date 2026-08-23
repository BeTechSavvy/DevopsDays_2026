"""
pipeline.py

Ties the pieces together end-to-end against LIVE data, the same flow
each module's __main__ block already ran against generate_mock_metrics():

    PrometheusClient -> AnomalyDetector -> CorrelationEngine -> IncidentStore

Usage:
    python pipeline.py                 # one-shot pass over the baseline window
    python pipeline.py --loop          # keeps polling for new anomalies
    python pipeline.py --loop --interval 30

Note: RCA diagnosis (rca_engine.py) is intentionally NOT wired in here yet --
that's the "diagnose ALL incidents" fix already on your list, kept separate
on purpose so this stays focused on the live-data swap.
"""

import argparse
import time
from datetime import timedelta

from anomaly_detector import AnomalyDetector
from correlation_engine import CorrelationEngine
from incident_store import IncidentStore
from prometheus_source import PrometheusClient

FEATURE_COLUMNS = ["cpu_percent", "memory_percent", "error_rate"]


def run_once(client, detector, engine, store, minutes_back, since=None):
    df = client.get_metrics_dataframe(minutes_back=minutes_back)
    if since is not None:
        df = df[df["timestamp"] > since]

    if df.empty:
        print("No new data points from Prometheus this cycle.")
        return since

    anomalies = detector.score_batch(df)
    incidents = engine.correlate(anomalies)

    if incidents:
        saved = store.save_many(incidents)
        print(f"[{df['timestamp'].max()}] {len(anomalies)} anomalies -> {saved} incidents saved.")
    else:
        print(f"[{df['timestamp'].max()}] {len(df)} points scored, no anomalies.")

    return df["timestamp"].max()


def main():
    parser = argparse.ArgumentParser(description="Run the Chaos Healer pipeline against live Prometheus data.")
    parser.add_argument("--loop", action="store_true", help="Keep polling instead of running once")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between polls in --loop mode")
    parser.add_argument("--baseline-minutes", type=int, default=30, help="History window used to fit 'normal'")
    args = parser.parse_args()

    client = PrometheusClient()
    if not client.is_reachable():
        print("Cannot reach Prometheus.")
        print("Run: kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090")
        raise SystemExit(1)

    store = IncidentStore()
    if not store.is_connected():
        print("Cannot reach MongoDB. Is it running?")
        raise SystemExit(1)

    print(f"Fitting detector on the last {args.baseline_minutes} minutes as the 'normal' baseline...")
    baseline_df = client.get_metrics_dataframe(minutes_back=args.baseline_minutes)
    if len(baseline_df) < 20:
        print(
            f"Only {len(baseline_df)} data points available -- that's thin for a baseline. "
            "Let traffic build up longer, or hit /work a bunch of times first, e.g.:\n"
            "  for i in $(seq 1 100); do curl -s http://localhost:30001/work > /dev/null; sleep 1; done"
        )

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(baseline_df, feature_columns=FEATURE_COLUMNS)
    engine = CorrelationEngine(time_window=timedelta(minutes=3))

    last_seen = run_once(client, detector, engine, store, minutes_back=args.baseline_minutes)

    if not args.loop:
        return

    print(f"Polling every {args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(args.interval)
            minutes_back = max(2, args.interval // 60 + 2)
            last_seen = run_once(client, detector, engine, store, minutes_back=minutes_back, since=last_seen)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()