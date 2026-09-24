"""
pipeline.py

Ties the pieces together end-to-end against LIVE data, the same flow
each module's __main__ block already ran against generate_mock_metrics():

    PrometheusClient -> AnomalyDetector -> CorrelationEngine -> IncidentStore -> RCAEngine
                     -> k8s_signals ----------^

Kubernetes state signals (restarts, OOMKilled, CrashLoopBackOff, image pull
errors, unavailable replicas) are attached to overlapping ML incidents, and
also raise rule-based incidents on their own when the ML model flags nothing.
An incident overlapping one already stored is merged into it, not duplicated.

Every new incident is sent to RCAEngine (Gemini + ChromaDB) and the result is
attached to its MongoDB document under `diagnosis`. RCA is best-effort: if
Gemini fails, the incident is kept without a diagnosis.

Usage:
    python pipeline.py                 # one-shot pass over the baseline window
    python pipeline.py --loop          # keeps polling for new anomalies
    python pipeline.py --loop --interval 30
    python pipeline.py --no-rca        # skip Gemini diagnosis (e.g. quota ran out)
"""

import argparse
import time
from datetime import timedelta

import requests

from anomaly_detector import AnomalyDetector
from correlation_engine import CorrelationEngine, Incident
from incident_store import IncidentStore
from k8s_signals import attach_k8s_signals, fetch_k8s_signals
from prometheus_source import PrometheusClient

FEATURE_COLUMNS = ["cpu_percent", "memory_percent", "error_rate"]
MIN_BASELINE_ROWS = 20


def create_rca_engine():
    """Builds the RCAEngine once at startup. Returns None if it can't be set up."""
    try:
        from rca_engine import RCAEngine  # heavy imports (chromadb, langchain), so only when RCA is on
        return RCAEngine()
    except Exception as e:
        print(f"WARNING: RCA disabled, could not start RCAEngine ({_short_error(e)}).")
        return None


def _short_error(e, limit=150):
    """Gemini errors embed a full JSON payload; keep the warning to one short line."""
    text = str(e).splitlines()[0] if str(e) else ""
    if len(text) > limit:
        text = text[:limit] + "..."
    return f"{type(e).__name__}: {text}"


def diagnose_incidents(rca, store, incidents):
    """Diagnoses each incident and attaches the result. Never raises."""
    diagnosed = 0
    for incident in incidents:
        try:
            diagnosis = rca.diagnose(incident, persist=False)
        except Exception as e:
            print(f"WARNING: RCA failed for incident {incident.incident_id} ({_short_error(e)}). Saved without diagnosis.")
            continue

        if "raw_response" in diagnosis:
            # rca_engine's fallback when Gemini's reply wasn't valid JSON
            print(f"WARNING: RCA for incident {incident.incident_id} returned unparseable output. Saved without diagnosis.")
            continue

        try:
            store.save_diagnosis(incident.incident_id, diagnosis)
        except Exception as e:
            print(f"WARNING: could not save diagnosis for incident {incident.incident_id} ({_short_error(e)}).")
            continue

        diagnosed += 1
        print(f"  Diagnosed {incident.incident_id}: {diagnosis.get('summary', '')}")
    return diagnosed


def run_once(client, detector, engine, store, minutes_back, since=None, rca=None):
    df = client.get_metrics_dataframe(minutes_back=minutes_back)
    if since is not None:
        df = df[df["timestamp"] > since]
    signals = fetch_k8s_signals(client, minutes_back=minutes_back, since=since)

    if df.empty and not signals:
        print("No new data points from Prometheus this cycle.")
        return since

    anomalies = detector.score_batch(df) if not df.empty else []
    incidents = engine.correlate(anomalies)
    rule_incidents = attach_k8s_signals(incidents, signals, engine.time_window)
    incidents += rule_incidents

    last_seen = df["timestamp"].max() if not df.empty else signals[-1].timestamp

    if not incidents:
        print(f"[{last_seen}] {len(df)} points scored, no anomalies, no Kubernetes signals.")
        return last_seen

    new, merged, to_diagnose = 0, 0, []
    for incident in incidents:
        doc, needs_diagnosis = store.save_or_merge(incident, engine.time_window)
        if doc["incident_id"] == incident.incident_id:
            new += 1
        else:
            merged += 1
        if needs_diagnosis:
            to_diagnose.append(Incident.from_dict(doc))

    print(
        f"[{last_seen}] {len(anomalies)} anomalies, {len(signals)} k8s signal samples "
        f"({len(rule_incidents)} rule-only incidents) -> {new} new, {merged} merged into existing incidents."
    )
    if rca is not None and to_diagnose:
        diagnosed = diagnose_incidents(rca, store, to_diagnose)
        print(f"RCA: {diagnosed}/{len(to_diagnose)} incidents diagnosed.")

    return last_seen


def main():
    parser = argparse.ArgumentParser(description="Run the Chaos Healer pipeline against live Prometheus data.")
    parser.add_argument("--loop", action="store_true", help="Keep polling instead of running once")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between polls in --loop mode")
    parser.add_argument("--baseline-minutes", type=int, default=30, help="History window used to fit 'normal'")
    parser.add_argument("--no-rca", action="store_true", help="Skip Gemini RCA diagnosis of new incidents")
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

    missing = [c for c in FEATURE_COLUMNS if c not in baseline_df.columns]
    if missing:
        print(
            f"Baseline is missing {', '.join(missing)} -- Prometheus returned no data for "
            "that query (see the WARNING above for the exact PromQL). Can't fit without "
            f"all of {FEATURE_COLUMNS}."
        )
        raise SystemExit(1)

    if len(baseline_df) < MIN_BASELINE_ROWS:
        print(
            f"Only {len(baseline_df)} data points available -- need at least {MIN_BASELINE_ROWS} "
            "to fit a baseline. Let traffic build up longer, or hit /work a bunch of times first, e.g.:\n"
            "  for i in $(seq 1 100); do curl -s http://localhost:30001/work > /dev/null; sleep 1; done"
        )
        raise SystemExit(1)

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(baseline_df, feature_columns=FEATURE_COLUMNS)
    engine = CorrelationEngine(time_window=timedelta(minutes=3))

    if args.no_rca:
        print("RCA disabled (--no-rca). Incidents will be saved without a diagnosis.")
        rca = None
    else:
        rca = create_rca_engine()

    last_seen = run_once(client, detector, engine, store, minutes_back=args.baseline_minutes, rca=rca)

    if not args.loop:
        return

    print(f"Polling every {args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(args.interval)
            minutes_back = max(2, args.interval // 60 + 2)
            try:
                last_seen = run_once(client, detector, engine, store, minutes_back=minutes_back, since=last_seen, rca=rca)
            except (requests.RequestException, RuntimeError, KeyError) as e:
                # last_seen is left unchanged, so the next poll re-covers this window
                print(f"Poll failed ({_short_error(e)}). Retrying in {args.interval}s.")
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()