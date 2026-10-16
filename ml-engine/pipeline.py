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

The detector is fitted on an EARLIER window ([now-60m, now-30m] by default)
and only scores data after it, so it never judges the points it learned
"normal" from. In --loop mode it refits every --refit-every cycles, unless
a stored incident falls inside the baseline window.

Usage:
    python pipeline.py                 # one-shot pass over the last 30 minutes
    python pipeline.py --loop          # keeps polling for new anomalies
    python pipeline.py --loop --interval 30
    python pipeline.py --baseline-start 20 --baseline-end 5   # shorter history, e.g. a fresh cluster
    python pipeline.py --no-rca        # skip Gemini diagnosis (e.g. quota ran out)
"""

import argparse
import time
import traceback
from datetime import timedelta

import pandas as pd

from anomaly_detector import AnomalyDetector
from config import FLASK_URL
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

        if "error" in diagnosis:
            # rca_engine caught a failed Gemini call
            print(f"WARNING: RCA failed for incident {incident.incident_id} ({diagnosis['error'][:150]}). Saved without diagnosis.")
            continue

        if "raw_response" in diagnosis:
            # rca_engine's fallback when Gemini's reply wasn't valid JSON
            print(f"WARNING: RCA for incident {incident.incident_id} returned unparseable output. Saved without diagnosis.")
            continue

        # lets store.save_or_merge tell when a growing incident's diagnosis is stale
        diagnosis["based_on_anomalies"] = len(incident.anomalies)
        try:
            store.save_diagnosis(incident.incident_id, diagnosis)
        except Exception as e:
            print(f"WARNING: could not save diagnosis for incident {incident.incident_id} ({_short_error(e)}).")
            continue

        diagnosed += 1
        print(f"  Diagnosed {incident.incident_id}: {diagnosis.get('summary', '')}")
    return diagnosed


def is_weak(incident):
    """
    A lone low-severity ML anomaly with no Kubernetes signals. The detector
    flags ~5% of perfectly normal points (contamination=0.05), and these were
    all ordinary /work noise, so on their own they don't open an incident.
    """
    return incident.detection == "ml" and len(incident.anomalies) < 2 and incident.max_severity == "low"


def save_incidents(store, incidents, window):
    """
    Saves/merges each incident. Weak ones may only merge into an existing
    incident, never open a new one. Returns (new, merged, skipped, to_diagnose).
    """
    new, merged, skipped, to_diagnose = 0, 0, 0, []
    for incident in incidents:
        doc, needs_diagnosis = store.save_or_merge(incident, window, create=not is_weak(incident))
        if doc is None:
            skipped += 1
            continue
        if doc["incident_id"] == incident.incident_id:
            new += 1
        else:
            merged += 1
        if needs_diagnosis:
            to_diagnose.append(Incident.from_dict(doc))
    return new, merged, skipped, to_diagnose


def fit_baseline(client, store, start_minutes, end_minutes, window, skip_if_incident=False):
    """
    Fits a NEW detector on the earlier window [now-start_minutes, now-end_minutes],
    so the model never scores the data it was trained on. Returns
    (detector, None) on success or (None, reason) if it couldn't fit.

    Stored incidents inside the window are not "normal": with
    skip_if_incident (periodic refits) the refit is skipped and the old
    model kept; otherwise (startup, where there's no old model) those
    rows are left out of the fit.
    """
    end = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(minutes=end_minutes)
    start = end - pd.Timedelta(minutes=start_minutes - end_minutes)
    spans = store.incident_spans(start, end, window)
    if spans and skip_if_incident:
        return None, f"{len(spans)} incident(s) inside the baseline window"

    df = client.get_metrics_dataframe(minutes_back=start_minutes, end_minutes_back=end_minutes)
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        return None, (
            f"baseline is missing {', '.join(missing)} -- Prometheus returned no data for "
            "that query (see the WARNING above for the exact PromQL)"
        )
    for lo, hi in spans:
        df = df[(df["timestamp"] < lo - window) | (df["timestamp"] > hi + window)]
    if len(df) < MIN_BASELINE_ROWS:
        return None, (
            f"only {len(df)} clean data points in [now-{start_minutes}m, now-{end_minutes}m]"
            + (f" after leaving out {len(spans)} incident(s)" if spans else "")
            + f", need at least {MIN_BASELINE_ROWS}"
        )

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(df, feature_columns=FEATURE_COLUMNS)
    note = f" ({len(spans)} incident(s) left out)" if spans else ""
    return detector, f"fitted on {len(df)} points from [now-{start_minutes}m, now-{end_minutes}m]{note}"


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

    new, merged, skipped, to_diagnose = save_incidents(store, incidents, engine.time_window)

    print(
        f"[{last_seen}] {len(anomalies)} anomalies, {len(signals)} k8s signal samples "
        f"({len(rule_incidents)} rule-only incidents) -> {new} new, {merged} merged into existing incidents"
        + (f", {skipped} single weak anomalies ignored." if skipped else ".")
    )
    if rca is not None and to_diagnose:
        diagnosed = diagnose_incidents(rca, store, to_diagnose)
        print(f"RCA: {diagnosed}/{len(to_diagnose)} incidents diagnosed.")

    return last_seen


def main():
    parser = argparse.ArgumentParser(description="Run the Chaos Healer pipeline against live Prometheus data.")
    parser.add_argument("--loop", action="store_true", help="Keep polling instead of running once")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between polls in --loop mode")
    parser.add_argument("--baseline-start", type=int, default=60,
                        help="Baseline window starts this many minutes ago (default 60)")
    parser.add_argument("--baseline-end", type=int, default=30,
                        help="Baseline window ends this many minutes ago (default 30); only newer data is scored")
    parser.add_argument("--refit-every", type=int, default=30,
                        help="In --loop mode, refit the baseline every N cycles (0 = never)")
    parser.add_argument("--no-rca", action="store_true", help="Skip Gemini RCA diagnosis of new incidents")
    args = parser.parse_args()
    if not 0 < args.baseline_end < args.baseline_start:
        parser.error("need 0 < --baseline-end < --baseline-start")

    client = PrometheusClient()
    if not client.is_reachable():
        print(f"Cannot reach Prometheus at {client.base_url}.")
        print("Run: kubectl port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090")
        raise SystemExit(1)

    store = IncidentStore()
    if not store.is_connected():
        print("Cannot reach MongoDB. Is it running?")
        raise SystemExit(1)
    store.backfill_datetimes()

    engine = CorrelationEngine(time_window=timedelta(minutes=3))

    print(f"Fitting detector on [now-{args.baseline_start}m, now-{args.baseline_end}m] as the 'normal' baseline...")
    detector, message = fit_baseline(client, store, args.baseline_start, args.baseline_end, engine.time_window)
    if detector is None:
        print(
            f"Can't fit a baseline: {message}.\n"
            "Let normal traffic build up longer (the cluster needs ~"
            f"{args.baseline_start} min of history), or use a shorter window, e.g. "
            "--baseline-start 20 --baseline-end 5. To generate traffic:\n"
            "  kubectl port-forward svc/flask-service 30001:5000      # own terminal\n"
            "  python chaos/baseline_traffic.py --minutes 25          # from the repo root\n"
            f"(the traffic scripts use FLASK_URL, currently {FLASK_URL})"
        )
        raise SystemExit(1)
    print(f"Baseline {message}.")

    if args.no_rca:
        print("RCA disabled (--no-rca). Incidents will be saved without a diagnosis.")
        rca = None
    else:
        rca = create_rca_engine()

    # score only what came after the baseline window
    last_seen = run_once(client, detector, engine, store, minutes_back=args.baseline_end, rca=rca)

    if not args.loop:
        return

    print(f"Polling every {args.interval}s. Ctrl+C to stop.")
    cycle = 0
    try:
        while True:
            time.sleep(args.interval)
            cycle += 1
            minutes_back = max(2, args.interval // 60 + 2)
            try:
                if args.refit_every and cycle % args.refit_every == 0:
                    refit, message = fit_baseline(client, store, args.baseline_start, args.baseline_end,
                                                  engine.time_window, skip_if_incident=True)
                    if refit is None:
                        print(f"Refit skipped, keeping the current baseline: {message}.")
                    else:
                        detector = refit
                        print(f"Baseline refitted: {message}.")
                last_seen = run_once(client, detector, engine, store, minutes_back=minutes_back, since=last_seen, rca=rca)
            except Exception:
                # Any failure in one cycle (Prometheus, Mongo, a bug) must not stop the loop.
                # last_seen is left unchanged, so the next poll re-covers this window.
                print(f"Poll failed. Retrying in {args.interval}s. Traceback:")
                traceback.print_exc()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()