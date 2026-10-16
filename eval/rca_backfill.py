"""
eval/rca_backfill.py

Diagnoses a SAMPLE of the evaluation's incidents after the fact, for runs
where live RCA was off (`pipeline.py --no-rca`, e.g. because the Gemini
free tier allows only 20 requests per day per model).

For each injection in chaos/injections.jsonl (since --since), it takes the
first incident that detected it (eval/matching.py rules) and, if that
incident has no diagnosis yet, runs the same RCAEngine.diagnose the
pipeline uses -- with the incident's stored evidence and counterfactual --
and saves the result on the incident. One Gemini call per injection at
most; each call also bumps the incident's llm_calls counter.

Then grade the answers in eval/rca_labels.csv and re-run eval/evaluate.py.

Usage (from the repo root):
    python eval/rca_backfill.py --since 2026-10-16T08:38:00Z
    python eval/rca_backfill.py --since ... --model gemini-flash-lite-latest --limit 21
    python eval/rca_backfill.py --since ... --dry-run      # list what would be diagnosed
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
ROOT = EVAL_DIR.parent
sys.path.insert(0, str(ROOT / "ml-engine"))
sys.path.insert(0, str(EVAL_DIR))

from evaluate import INJECTIONS, load_incidents, load_injections  # noqa: E402
from matching import GRACE, match_all, ts  # noqa: E402

CALL_LOG = ROOT / "ml-engine" / "logs" / "backfill_calls.jsonl"


def main():
    parser = argparse.ArgumentParser(description="Diagnose one detected incident per injection, after the run.")
    parser.add_argument("--since", required=True, help="Only injections starting at/after this UTC time")
    parser.add_argument("--model", help="Gemini model to use (default: GEMINI_MODEL from ml-engine/.env)")
    parser.add_argument("--limit", type=int, default=25, help="At most this many Gemini calls")
    parser.add_argument("--delay", type=float, default=5, help="Seconds between calls (free-tier rate limits)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.model:
        os.environ["GEMINI_MODEL"] = args.model  # read by config.py at import

    from correlation_engine import Incident
    from incident_store import IncidentStore

    store = IncidentStore()
    injections = load_injections(INJECTIONS, ts(args.since))
    start = min(i.start for i in injections) - GRACE
    end = max(i.end for i in injections) + 2 * GRACE
    matches, _ = match_all(injections, load_incidents(store, start, end))

    todo = [m for m in matches if m.detected and not m.first.get("diagnosis")]
    print(f"{len(injections)} injections, {sum(m.detected for m in matches)} detected, "
          f"{len(todo)} detections without a diagnosis; diagnosing up to {args.limit}.")
    if args.dry_run:
        for m in todo[:args.limit]:
            print(f"  {m.injection.scenario:15} {m.first['incident_id']}  {m.first['workload']}")
        return

    from rca_engine import RCAEngine
    rca = RCAEngine()
    print(f"Model: {rca.llm.model}")
    CALL_LOG.parent.mkdir(exist_ok=True)

    done = failed = 0
    for m in todo[:args.limit]:
        doc = store.collection.find_one({"incident_id": m.first["incident_id"]}, {"_id": 0})
        incident = Incident.from_dict(doc)
        store.count_llm_call(incident.incident_id)
        started = time.perf_counter()
        diagnosis = rca.diagnose(incident, persist=False, evidence=doc.get("evidence"))
        seconds = time.perf_counter() - started
        with open(CALL_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"incident_id": incident.incident_id, "seconds": round(seconds, 3),
                                "ok": "error" not in diagnosis and "raw_response" not in diagnosis}) + "\n")
        if "error" in diagnosis or "raw_response" in diagnosis:
            failed += 1
            print(f"  {m.injection.scenario:15} {incident.incident_id}: FAILED "
                  f"{diagnosis.get('error', 'unparseable reply')[:120]}")
            if "429" in diagnosis.get("error", ""):
                print("Quota exhausted; stopping. Re-run later (or with --model) to continue.")
                break
        else:
            diagnosis["based_on_anomalies"] = len(incident.anomalies)
            diagnosis["counterfactual"] = doc.get("counterfactual")
            diagnosis["backfilled"] = True
            store.save_diagnosis(incident.incident_id, diagnosis)
            done += 1
            print(f"  {m.injection.scenario:15} {incident.incident_id}: {diagnosis.get('likely_root_cause', '')[:110]}")
        time.sleep(args.delay)

    print(f"Diagnosed {done}, failed {failed}. Next: python eval/evaluate.py --since {args.since}, "
          "then grade eval/rca_labels.csv.")


if __name__ == "__main__":
    main()
