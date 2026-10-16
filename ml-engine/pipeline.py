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

Every workload in MONITORED_WORKLOADS (default flask-deployment and
demo-db-app) is scored by its own detector. predictor.py adds
predicted_exhaustion warnings (a resource trending to its limit within 10
minutes) as signals of their own.

Before diagnosis, each new or changed incident gets log/event evidence
(evidence.py) and a counterfactual recommendation (counterfactual.py). Then
it is sent to RCAEngine (Gemini + ChromaDB, with the evidence in the prompt)
and the result is attached to its MongoDB document under `diagnosis`. RCA is
best-effort: if Gemini fails, the incident is kept without a diagnosis, but
still has its evidence and counterfactual.

The detector is fitted on an EARLIER window ([now-60m, now-30m] by default)
and only scores data after it, so it never judges the points it learned
"normal" from. Rows of that workload's stored incidents are left out of the
fit. In --loop mode it refits every --refit-every cycles (tracking slow
drift, e.g. a process's memory creeping up), but never while one of that
workload's incidents inside the baseline window is still open.

Usage:
    python pipeline.py                 # one-shot pass over the last 30 minutes
    python pipeline.py --loop          # keeps polling for new anomalies
    python pipeline.py --loop --interval 30
    python pipeline.py --baseline-start 20 --baseline-end 5   # shorter history, e.g. a fresh cluster
    python pipeline.py --no-rca        # skip Gemini diagnosis (e.g. quota ran out)
"""

import argparse
import json
import os
import time
import traceback
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd

import counterfactual
import evidence
import predictor
from anomaly_detector import AnomalyDetector
from config import FLASK_URL
from correlation_engine import CorrelationEngine, Incident
from incident_store import IncidentStore
from k8s_signals import attach_k8s_signals, fetch_k8s_signals
from prometheus_source import PrometheusClient, queries_for

FEATURE_COLUMNS = ["cpu_percent", "memory_percent", "error_rate"]
MIN_BASELINE_ROWS = 20
BASELINE_STEP = "30s"  # prometheus_source's default sample step
CYCLE_LOG = Path(__file__).resolve().parent / "logs" / "cycle_times.jsonl"
WINDOW = timedelta(minutes=3)
# Apps whose cpu/memory/error rate are scored, each against its own baseline
MONITORED_WORKLOADS = os.environ.get("MONITORED_WORKLOADS", "flask-deployment,demo-db-app").split(",")


@dataclass
class Workload:
    """One monitored app: its metric queries, its own detector and correlation engine."""
    name: str
    queries: dict
    engine: CorrelationEngine
    detector: AnomalyDetector | None = None  # None until a baseline could be fitted

    @classmethod
    def named(cls, name: str) -> "Workload":
        return cls(name, queries_for(f"{name}-.*"), CorrelationEngine(time_window=WINDOW, workload=name))


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


def enrich_incidents(client, store, incidents) -> dict:
    """
    For each incident about to be (re-)diagnosed: gathers log/event evidence
    (evidence.py) and a counterfactual recommendation (counterfactual.py),
    stores both on the incident, and returns {incident_id: (evidence, cf)}
    for the RCA prompt. Best-effort per incident; never raises.
    """
    enrichments = {}
    for incident in incidents:
        try:
            doc = store.collection.find_one({"incident_id": incident.incident_id}, {"_id": 0}) or incident.to_dict()
            found = evidence.gather(doc)
            doc["evidence"] = found
            observed = counterfactual.fetch_observed(client, doc)
            observed.update(_observed_from_evidence(found))
            cf = counterfactual.recommend(doc, observed)
            store.collection.update_one({"incident_id": incident.incident_id},
                                        {"$set": {"evidence": found, "counterfactual": cf}})
            enrichments[incident.incident_id] = (found, cf)
        except Exception as e:
            print(f"WARNING: could not gather evidence for incident {incident.incident_id} ({_short_error(e)}).")
    return enrichments


def _observed_from_evidence(found: dict) -> dict:
    """Values the counterfactual rules can quote: the first error line, a failing image name."""
    observed = {}
    if found.get("logs"):
        observed["first_error_line"] = found["logs"][0][:160]
    for event in found.get("events", []):
        if "Failed to pull image" in event and '"' in event:
            observed["image"] = event.split('"')[1]
            break
    return observed


def diagnose_incidents(rca, store, incidents, stats=None, enrichments=None):
    """
    Diagnoses each incident and attaches the result. Never raises. Every
    Gemini call, successful or not, is counted on the incident (llm_calls)
    and in `stats` (llm_calls, llm_seconds) for the cycle-time log.
    `enrichments` (from enrich_incidents) adds the evidence to the prompt and
    the counterfactual to the stored diagnosis.
    """
    enrichments = enrichments or {}
    diagnosed = 0
    for incident in incidents:
        found, cf = enrichments.get(incident.incident_id, (None, None))
        started = time.perf_counter()
        try:
            store.count_llm_call(incident.incident_id)
            diagnosis = rca.diagnose(incident, persist=False, evidence=found)
        except Exception as e:
            print(f"WARNING: RCA failed for incident {incident.incident_id} ({_short_error(e)}). Saved without diagnosis.")
            continue
        finally:
            if stats is not None:
                stats["llm_calls"] = stats.get("llm_calls", 0) + 1
                stats["llm_seconds"] = stats.get("llm_seconds", 0.0) + time.perf_counter() - started

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
        diagnosis["counterfactual"] = cf
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
        # ids are deterministic, so a merge can keep the same id; only a newly
        # created incident carries stored_at (merges keep the original one in Mongo)
        if "stored_at" in doc:
            new += 1
        else:
            merged += 1
        if needs_diagnosis:
            to_diagnose.append(Incident.from_dict(doc))
    return new, merged, skipped, to_diagnose


def fit_baseline(client, store, start_minutes, end_minutes, window, skip_if_open=False, queries=None,
                 workload=None, quiet=False):
    """
    Fits a NEW detector on the earlier window [now-start_minutes, now-end_minutes],
    so the model never scores the data it was trained on. Returns
    (detector, message) on success or (None, reason) if it couldn't fit.

    Stored incidents (of `workload`, or any if None) inside the window are not
    "normal", so their rows are left out of the fit. With skip_if_open
    (periodic refits), the refit is skipped -- old model kept -- while such an
    incident is still OPEN (still being extended, i.e. it ended less than
    `window` ago): its true extent isn't known yet.
    """
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    end = now - pd.Timedelta(minutes=end_minutes)
    start = end - pd.Timedelta(minutes=start_minutes - end_minutes)
    spans = store.incident_spans(start, end, window, workload)
    open_spans = [s for s in spans if s[1] > now - window]
    if open_spans and skip_if_open:
        return None, f"{len(open_spans)} open incident(s) inside the baseline window"

    df = client.get_metrics_dataframe(minutes_back=start_minutes, end_minutes_back=end_minutes, queries=queries,
                                      quiet=quiet)
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        return None, (
            f"baseline is missing {', '.join(missing)} -- Prometheus returned no data for "
            "that query (see the WARNING above for the exact PromQL)"
        )
    # drop each incident's own span +/- one sample -- not +/- the whole correlation
    # window, which made two short blips eat ~12 of 30 minutes and blocked refits
    margin = pd.Timedelta(BASELINE_STEP)
    for lo, hi in spans:
        df = df[(df["timestamp"] < lo - margin) | (df["timestamp"] > hi + margin)]
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


def log_cycle(seconds, stats, ok=True):
    """One line per pipeline cycle in logs/cycle_times.jsonl, read by eval/evaluate.py."""
    CYCLE_LOG.parent.mkdir(exist_ok=True)
    record = {"time": str(pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s")), "seconds": round(seconds, 3),
              "llm_calls": stats.get("llm_calls", 0), "llm_seconds": round(stats.get("llm_seconds", 0.0), 3),
              "ok": ok}
    with open(CYCLE_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def run_once(client, workloads, store, minutes_back, since=None, rca=None, stats=None):
    """
    One detection cycle over every fitted workload. `since` is the previous
    cycle's last-seen timestamp (None on the first run); only newer data
    counts. Returns the new last-seen timestamp.
    """
    incidents, n_points, n_anomalies = [], 0, 0
    latest = []
    for w in workloads:
        if w.detector is None:
            continue
        df = client.get_metrics_dataframe(minutes_back=minutes_back, queries=w.queries)
        if any(c not in df.columns for c in FEATURE_COLUMNS):
            continue
        if since is not None:
            df = df[df["timestamp"] > since]
        if df.empty:
            continue
        anomalies = w.detector.score_batch(df)
        incidents += w.engine.correlate(anomalies)
        n_points, n_anomalies = n_points + len(df), n_anomalies + len(anomalies)
        latest.append(df["timestamp"].max())

    signals = fetch_k8s_signals(client, minutes_back=minutes_back, since=since)
    warnings = predictor.fetch_predictions(client, since=since)
    signals = sorted(signals + warnings, key=lambda s: s.timestamp)

    if not latest and not signals:
        print("No new data points from Prometheus this cycle.")
        return since

    rule_incidents = attach_k8s_signals(incidents, signals, WINDOW)
    incidents += rule_incidents
    last_seen = max(latest) if latest else signals[-1].timestamp

    if not incidents:
        print(f"[{last_seen}] {n_points} points scored, no anomalies, no Kubernetes signals.")
        return last_seen

    new, merged, skipped, to_diagnose = save_incidents(store, incidents, WINDOW)

    print(
        f"[{last_seen}] {n_anomalies} anomalies, {len(signals)} k8s signal samples "
        f"({len(warnings)} exhaustion predictions; {len(rule_incidents)} rule-only incidents) -> "
        f"{new} new, {merged} merged into existing incidents"
        + (f", {skipped} single weak anomalies ignored." if skipped else ".")
    )
    for w in warnings:
        print(f"  PREDICTION: {predictor.describe(w)}")
    if to_diagnose:
        enrichments = enrich_incidents(client, store, to_diagnose)
        if rca is not None:
            diagnosed = diagnose_incidents(rca, store, to_diagnose, stats, enrichments)
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

    workloads = [Workload.named(name.strip()) for name in MONITORED_WORKLOADS if name.strip()]

    def fit(w, skip_if_open=False, quiet=False):
        """Fits (or refits) one workload's baseline; returns the reason if it couldn't."""
        detector, message = fit_baseline(client, store, args.baseline_start, args.baseline_end, WINDOW,
                                         skip_if_open=skip_if_open, queries=w.queries, workload=w.name,
                                         quiet=quiet)
        if detector is not None:
            w.detector = detector
            print(f"  {w.name}: baseline {message}.")
            return None
        return message

    print(f"Fitting detectors on [now-{args.baseline_start}m, now-{args.baseline_end}m] as the 'normal' baseline...")
    for w in workloads:
        problem = fit(w)
        if problem:
            print(f"  {w.name}: can't fit yet ({problem}); will retry every cycle.")
    if all(w.detector is None for w in workloads):
        print(
            "Can't fit a baseline for any workload.\n"
            "Let normal traffic build up longer (the cluster needs ~"
            f"{args.baseline_start} min of history), or use a shorter window, e.g. "
            "--baseline-start 20 --baseline-end 5. To generate traffic:\n"
            "  kubectl apply -f chaos/baseline-traffic.yaml           # in-cluster, from the repo root\n"
            "  python chaos/baseline_traffic.py --minutes 25          # or via the port-forward\n"
            f"(the traffic scripts use FLASK_URL, currently {FLASK_URL})"
        )
        raise SystemExit(1)

    if args.no_rca:
        print("RCA disabled (--no-rca). Incidents will be saved without a diagnosis.")
        rca = None
    else:
        rca = create_rca_engine()

    # score only what came after the baseline window
    last_seen = run_once(client, workloads, store, minutes_back=args.baseline_end, rca=rca)

    if not args.loop:
        return

    print(f"Polling every {args.interval}s. Ctrl+C to stop.")
    cycle = 0
    try:
        while True:
            time.sleep(args.interval)
            cycle += 1
            minutes_back = max(2, args.interval // 60 + 2)
            for w in workloads:
                try:
                    if w.detector is None:
                        fit(w, quiet=True)  # not enough history at startup; keep trying (quietly)
                    elif args.refit_every and cycle % args.refit_every == 0:
                        problem = fit(w, skip_if_open=True)
                        if problem:
                            print(f"  {w.name}: refit skipped, keeping the current baseline: {problem}.")
                except Exception:
                    print(f"Refit of {w.name} failed, keeping the current baseline. Traceback:")
                    traceback.print_exc()

            stats, started = {}, time.perf_counter()
            try:
                last_seen = run_once(client, workloads, store, minutes_back=minutes_back,
                                     since=last_seen, rca=rca, stats=stats)
                log_cycle(time.perf_counter() - started, stats)
            except Exception:
                # Any failure in one cycle (Prometheus, Mongo, a bug) must not stop the loop.
                # last_seen is left unchanged, so the next poll re-covers this window.
                print(f"Poll failed. Retrying in {args.interval}s. Traceback:")
                traceback.print_exc()
                try:
                    log_cycle(time.perf_counter() - started, stats, ok=False)
                except OSError:
                    pass
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()