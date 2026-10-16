"""
eval/evaluate.py

Scores the system against the chaos experiments: reads the ground truth in
chaos/injections.jsonl and the incidents stored in MongoDB, then reports

  - per scenario and overall: TP, FP, FN, precision, recall, F1
    (matching rules: eval/matching.py)
  - kind accuracy: did the detected incident show the scenario's expected
    failure kind (ml-engine/failure_kinds.py)
  - MTTD: incident stored_at minus injection start
  - RCA accuracy, graded by hand: diagnosed detections are exported to
    eval/rca_labels.csv with an empty `correct` column; fill in y/n and
    re-run to include the grades (existing grades are kept)
  - Gemini calls per incident (the incident's llm_calls counter)
  - pipeline cycle time with and without Gemini calls
    (ml-engine/logs/cycle_times.jsonl, written by pipeline.py)
  - pipeline process CPU / memory (ml-engine/logs/pipeline_usage.csv,
    written by scripts/sample-pipeline.ps1 while the experiment runs)

Writes eval/results.md, eval/results.json and eval/f1_by_scenario.png.
error_spike (traffic_generator.py) is reported as a separate validation row,
not part of "overall".

Usage (from the repo root, MongoDB running):
    python eval/evaluate.py
    python eval/evaluate.py --since 2026-10-16T08:00:00Z     # ignore older injections/incidents
"""

import argparse
import csv
import json
import statistics
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

EVAL_DIR = Path(__file__).resolve().parent
ROOT = EVAL_DIR.parent
sys.path.insert(0, str(ROOT / "ml-engine"))
sys.path.insert(0, str(EVAL_DIR))

from matching import GRACE, Injection, match_all, score, ts  # noqa: E402

INJECTIONS = ROOT / "chaos" / "injections.jsonl"
CYCLE_LOG = ROOT / "ml-engine" / "logs" / "cycle_times.jsonl"
USAGE_CSV = ROOT / "ml-engine" / "logs" / "pipeline_usage.csv"
LABELS = EVAL_DIR / "rca_labels.csv"
RESULTS_MD = EVAL_DIR / "results.md"
RESULTS_JSON = EVAL_DIR / "results.json"
F1_PNG = EVAL_DIR / "f1_by_scenario.png"
SEPARATE = ("error_spike",)

# The defect found during the 2026-10-16 evaluation run and its fix (see
# write_markdown): injections that started after the fixed pipeline was
# running get their own table. Override with --fixed-since / --fixed-commit.
FIX_SINCE = "2026-10-16T10:52:26Z"
FIX_COMMIT = "ab5773b"
DEFECT_NOTE = [
    "## Defect found during evaluation", "",
    "**What happened.** After a pipeline restart at 09:01:41 UTC, steady low/medium noise on "
    "flask-deployment (its error rate normally swings 7–13% and its memory drifts up slowly) kept one "
    "incident, `d4ac9d90`, open from 09:01:41 until the fix at ~10:52 — 100+ minutes and 230+ anomalies. "
    "Every new flask anomaly merged into it, and because it was always open it blocked every baseline "
    "refit, so the stale baseline kept producing the noise that kept it open.", "",
    "**Runs affected** (all on the code before the fix):", "",
    "- cpu_exhaustion round 2 (09:54): its CPU spike merged into `d4ac9d90`, which the matcher had "
    "already used for an earlier injection, so it scores as a **false negative**.",
    "- error_spike round 1 (09:46): matched to `d4ac9d90`, which started 44 minutes before the injection, "
    "so its MTTD is **negative** and meaningless.",
    "- db_failure round 2 (10:47): the same mechanism on demo-db-app — a noise incident (`46d6ede0`) open "
    "since 10:32 absorbed the injection. It was still detected with the right kind (db_connection), but its "
    "MTTD is negative.",
    "- error_spike round 2 (10:59) started after the fix and is not affected.", "",
    "**Fix** (commit `{commit}`): an incident is closed after 10 minutes without a medium/high anomaly "
    "or Kubernetes signal, or at 30 minutes; a stale low/medium incident no longer blocks a refit, and its "
    "rows are used for the baseline (drift). The pipeline was restarted with the fix at {since} UTC "
    "(in a cool-down), and two extra cpu_exhaustion rounds and one extra error_spike round were run so each "
    "has 3 runs on the fixed code (table above). Results were **not** re-scored and no incident was split "
    "after the fact: the tables above count the affected runs as they happened.",
]
SCENARIO_ORDER = ["cpu_exhaustion", "oom_killed", "crashloop", "failed_rollout",
                  "node_pressure", "db_failure", "error_spike"]


def load_injections(path: Path, since=None, include_incomplete=False) -> list[Injection]:
    if not path.exists():
        return []
    injections = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            inj = Injection.from_record(json.loads(line))
            if (inj.completed or include_incomplete) and (since is None or inj.start >= since):
                injections.append(inj)
    return injections


def load_incidents(store, start, end) -> list[dict]:
    """Incidents starting inside the evaluation period [start, end]."""
    docs = store.collection.find({}, {"_id": 0, "start_dt": 0, "end_dt": 0})
    return [d for d in docs if start <= ts(d["start_time"]) <= end]


# --- RCA grading ------------------------------------------------------------

def _grade(value: str) -> bool | None:
    v = (value or "").strip().lower()
    if v in ("y", "yes", "1", "true", "correct"):
        return True
    if v in ("n", "no", "0", "false", "wrong", "incorrect"):
        return False
    return None


def sync_rca_labels(matches) -> dict:
    """
    Adds every diagnosed detection to rca_labels.csv (keeping existing rows
    and grades), then returns the grades: {incident_id: True/False/None}.
    """
    rows = {}
    if LABELS.exists():
        with open(LABELS, newline="", encoding="utf-8") as f:
            rows = {r["incident_id"]: r for r in csv.DictReader(f)}
    for m in matches:
        doc = m.first
        if doc and doc.get("diagnosis") and doc["incident_id"] not in rows:
            rows[doc["incident_id"]] = {
                "scenario": m.injection.scenario, "incident_id": doc["incident_id"],
                "likely_root_cause": doc["diagnosis"].get("likely_root_cause", ""), "correct": "",
            }
    with open(LABELS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["scenario", "incident_id", "likely_root_cause", "correct"])
        writer.writeheader()
        writer.writerows(rows.values())
    return {i: _grade(r["correct"]) for i, r in rows.items()}


# --- summaries --------------------------------------------------------------

def _stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "mean": None, "median": None, "p95": None, "min": None, "max": None}
    values = sorted(values)
    return {"n": len(values), "mean": statistics.fmean(values), "median": statistics.median(values),
            "p95": values[min(len(values) - 1, round(0.95 * (len(values) - 1)))],
            "min": values[0], "max": values[-1]}


def cycle_times(since) -> dict:
    """Pipeline cycle durations, split by whether the cycle called Gemini."""
    with_llm, without_llm, excluding_llm = [], [], []
    if CYCLE_LOG.exists():
        for line in CYCLE_LOG.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            c = json.loads(line)
            if since is not None and ts(c["time"]) < since:
                continue
            if c.get("llm_calls", 0):
                with_llm.append(c["seconds"])
                excluding_llm.append(c["seconds"] - c.get("llm_seconds", 0))
            else:
                without_llm.append(c["seconds"])
    return {"without_llm": _stats(without_llm), "with_llm": _stats(with_llm),
            "with_llm_minus_llm_time": _stats(excluding_llm)}


def pipeline_usage(start, end, path=USAGE_CSV) -> dict:
    """The pipeline process's CPU % (of one core) and working-set MB, sampled within [start, end]."""
    cpu, mem = [], []
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if start <= ts(row["time"]) <= end:
                    cpu.append(float(row["cpu_percent"]))
                    mem.append(float(row["working_set_mb"]))
    return {"cpu_percent": _stats(cpu), "working_set_mb": _stats(mem)}


def prediction_lead_time(match) -> float | None:
    """
    Seconds from the first predicted_exhaustion warning to the first OOMKilled
    restart, across every incident matched to this injection (the warning may
    have opened its own incident before the kill). None if either is missing
    or the warning came after the kill.
    """
    warned, killed = [], []
    for doc in match.incidents:
        for s in doc.get("k8s_signals", []):
            if s["kind"] == "predicted_exhaustion":
                warned.append(ts(s["first_seen"]))
            elif s["kind"] == "restart" and s["reason"] == "OOMKilled":
                killed.append(ts(s["first_seen"]))
    if not warned or not killed or min(warned) >= min(killed):
        return None
    return (min(killed) - min(warned)).total_seconds()


def fixed_code_flask(matches, false_positives, fixed_since) -> dict:
    """
    The flask-deployment scenarios, restricted to injections that started
    after the fix -- same matching as the main table, just a subset of it.
    """
    from failure_kinds import incident_kinds

    rows = []
    for m in matches:
        inj = m.injection
        if inj.target != "flask-deployment" or inj.start < fixed_since:
            continue
        rows.append({"scenario": inj.scenario, "start": str(inj.start), "detected": m.detected,
                     "incident_id": m.first["incident_id"] if m.detected else None,
                     "kind_correct": (inj.expected_kind in incident_kinds(m.first)) if m.detected else None,
                     "mttd_seconds": m.mttd_seconds()})
    fps = [d["incident_id"] for d in false_positives
           if (d.get("workload") or "flask-deployment") == "flask-deployment" and ts(d["start_time"]) >= fixed_since]
    return {"since": str(fixed_since), "injections": rows, "flask_false_positives": fps}


def per_scenario_extras(matches, grades) -> dict:
    from failure_kinds import incident_kinds

    extras = {}
    for s in {m.injection.scenario for m in matches}:
        ms = [m for m in matches if m.injection.scenario == s]
        detected = [m for m in ms if m.detected]
        # across every incident matched to the injection: node pressure, for one,
        # shows up as a node incident AND an incident for the evicted workload
        kind_ok = [m.injection.expected_kind in set().union(*(incident_kinds(d) for d in m.incidents))
                   for m in detected]
        graded = [grades.get(m.first["incident_id"]) for m in detected]
        graded = [g for g in graded if g is not None]
        mttd = [m.mttd_seconds() for m in detected if m.mttd_seconds() is not None]
        llm = [m.first.get("llm_calls", 0) for m in detected]
        lead = [t for t in (prediction_lead_time(m) for m in detected) if t is not None]
        extras[s] = {
            "prediction_lead_seconds": _stats(lead),
            "kind_accuracy": sum(kind_ok) / len(kind_ok) if kind_ok else None,
            "diagnosed": sum(bool(m.first.get("diagnosis")) for m in detected),
            "rca_graded": len(graded),
            "rca_accuracy": sum(graded) / len(graded) if graded else None,
            "mttd_seconds": _stats(mttd),
            "llm_calls_per_incident": _stats(llm),
        }
    return extras


# --- output -----------------------------------------------------------------

def _fmt(v, pct=False, digits=2):
    if v is None:
        return "–"
    return f"{100 * v:.0f}%" if pct else f"{v:.{digits}f}"


def _order(names):
    return sorted(names, key=lambda s: (SCENARIO_ORDER.index(s) if s in SCENARIO_ORDER else 99, s))


def write_markdown(result: dict) -> None:
    sc, ex, ov = result["scores"]["scenarios"], result["extras"], result["scores"]["overall"]
    lines = [
        "# Evaluation results", "",
        f"Generated {result['generated']} UTC from {result['n_injections']} injections in "
        f"`chaos/injections.jsonl` and {result['n_incidents']} incidents in MongoDB "
        f"({result['period']['start']} → {result['period']['end']}).", "",
        f"A detection (TP) is an incident of the injected workload overlapping "
        f"[start, end + {int(GRACE.total_seconds() // 60)} min]. Duplicates are extra incidents for an "
        "already-detected injection (not counted as FP). error_spike is a separate validation row, "
        "not part of Overall.", "",
        "## Detection", "",
        "| Scenario | Injections | TP | FP | FN | Dup | Precision | Recall | F1 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in _order(sc):
        r = sc[s]
        lines.append(f"| {s} | {r['injections']} | {r['tp']} | {r['fp']} | {r['fn']} | {r['duplicates']} | "
                     f"{_fmt(r['precision'])} | {_fmt(r['recall'])} | {_fmt(r['f1'])} |")
    lines.append(f"| **Overall** | {ov['injections']} | {ov['tp']} | {ov['fp']} | {ov['fn']} | "
                 f"{ov['duplicates']} | {_fmt(ov['precision'])} | {_fmt(ov['recall'])} | {_fmt(ov['f1'])} |")
    if result["scores"]["fp_other"]:
        lines += ["", f"Overall FP includes {result['scores']['fp_other']} incident(s) on workloads no "
                      "scenario targets."]

    lines += ["", "## Diagnosis and timing", "",
              "| Scenario | Kind correct | Diagnosed | RCA correct (graded) | MTTD mean (s) | MTTD max (s) "
              "| Gemini calls / incident (mean, max) |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for s in _order(ex):
        e, tp = ex[s], sc[s]["tp"]
        llm = e["llm_calls_per_incident"]
        lines.append(
            f"| {s} | {_fmt(e['kind_accuracy'], pct=True)} | {e['diagnosed']}/{tp} | "
            f"{_fmt(e['rca_accuracy'], pct=True)} ({e['rca_graded']}) | {_fmt(e['mttd_seconds']['mean'], digits=0)} | "
            f"{_fmt(e['mttd_seconds']['max'], digits=0)} | {_fmt(llm['mean'])}, {_fmt(llm['max'], digits=0)} |")
    rca = result["rca_overall"]
    lines += ["", f"RCA accuracy overall: {_fmt(rca['accuracy'], pct=True)} of {rca['graded']} graded "
                  f"({rca['ungraded']} still ungraded in `eval/rca_labels.csv`)."]
    if "diagnosed" in rca:
        skipped = rca["detected"] - rca["diagnosed"]
        models = ", ".join(f"`{m}`" for m in rca["models"]) or "–"
        lines += ["", f"RCA diagnoses: {rca['diagnosed']} of {rca['detected']} detections diagnosed, "
                      f"{skipped} skipped{' (Gemini free-tier daily quota)' if skipped else ''}; model: {models}. "
                      "The pipeline ran with `--no-rca`; diagnoses were made afterwards by "
                      "`eval/rca_backfill.py --priority-since` (fixed-code runs first, then the latest rounds), "
                      "one call per detection."]
    if rca.get("backfilled"):
        lines += ["", f"{rca['backfilled']} of the diagnoses were made after the run by `eval/rca_backfill.py` "
                      "(live RCA was off: Gemini free-tier quota), one call per detected injection — so for "
                      "those incidents Gemini calls per incident is 1 by construction, not a measurement of "
                      "the pipeline's re-diagnosis policy."]

    with_lead = {s: e["prediction_lead_seconds"] for s, e in ex.items() if e["prediction_lead_seconds"]["n"]}
    lines += ["", "## Failure prediction lead time", "",
              "First `predicted_exhaustion` warning → first OOMKilled restart, per detected injection.", ""]
    if with_lead:
        lines += ["| Scenario | Injections with a warning before the kill | mean (s) | min (s) | max (s) |",
                  "|---|---:|---:|---:|---:|"]
        for s in _order(with_lead):
            lt = with_lead[s]
            lines.append(f"| {s} | {lt['n']}/{sc[s]['tp']} | {_fmt(lt['mean'], digits=0)} | "
                         f"{_fmt(lt['min'], digits=0)} | {_fmt(lt['max'], digits=0)} |")
    else:
        lines.append("No injection had a prediction warning before an OOMKilled restart.")

    ct = result["cycle_times"]
    lines += ["", "## Pipeline cycle time (seconds)", "",
              "All cycles in `ml-engine/logs/cycle_times.jsonl` (every pipeline run logged on this machine).", "",
              "| Cycles | n | mean | median | p95 | max |", "|---|---:|---:|---:|---:|---:|"]
    for label, key in (("without Gemini calls", "without_llm"), ("with Gemini calls", "with_llm"),
                       ("with Gemini calls, minus Gemini time", "with_llm_minus_llm_time")):
        c = ct[key]
        lines.append(f"| {label} | {c['n']} | {_fmt(c['mean'])} | {_fmt(c['median'])} | {_fmt(c['p95'])} | {_fmt(c['max'])} |")
    usage = result.get("pipeline_usage")
    if usage and usage["cpu_percent"]["n"]:
        cpu, mem = usage["cpu_percent"], usage["working_set_mb"]
        lines += ["", "## Pipeline resource usage", "",
                  f"pipeline.py sampled every 60 s during the experiment ({cpu['n']} samples, "
                  "`ml-engine/logs/pipeline_usage.csv`). CPU is % of one core.", "",
                  "| Metric | mean | max |", "|---|---:|---:|",
                  f"| CPU % | {_fmt(cpu['mean'], digits=1)} | {_fmt(cpu['max'], digits=1)} |",
                  f"| Memory (working set, MB) | {_fmt(mem['mean'], digits=0)} | {_fmt(mem['max'], digits=0)} |"]
    fixed = result.get("fixed_code")
    if fixed and fixed["injections"]:
        lines += ["", f"## Flask scenarios on the fixed code only (injections after {fixed['since']} UTC, "
                      f"commit `{result['fix_commit']}`)", "",
                  "A subset of the main table, same matching rules; see *Defect found during evaluation* below.",
                  "", "| Scenario | Injections | Detected | Recall | Kind correct | MTTD mean (s) | MTTD max (s) |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for s in _order({r["scenario"] for r in fixed["injections"]}):
            rs = [r for r in fixed["injections"] if r["scenario"] == s]
            det = [r for r in rs if r["detected"]]
            mttd = _stats([r["mttd_seconds"] for r in det if r["mttd_seconds"] is not None])
            kind = [r["kind_correct"] for r in det]
            lines.append(f"| {s} | {len(rs)} | {len(det)} | {_fmt(len(det) / len(rs))} | "
                         f"{_fmt(sum(kind) / len(kind) if kind else None, pct=True)} | "
                         f"{_fmt(mttd['mean'], digits=0)} | {_fmt(mttd['max'], digits=0)} |")
        lines += ["", f"flask-deployment false positives after the fix: {len(fixed['flask_false_positives'])}"
                      + (f" ({', '.join(fixed['flask_false_positives'])})." if fixed["flask_false_positives"] else ".")]
        lines += [""] + [line.format(commit=result["fix_commit"], since=fixed["since"]) for line in DEFECT_NOTE]

    lines += ["", "## Per injection", "",
              "| # | Scenario | Start → end (UTC) | Detected by | MTTD (s) |", "|---:|---|---|---|---:|"]
    for n, m in enumerate(result["matches"], 1):
        ids = ", ".join(f"`{i}`" for i in m["incident_ids"]) or "**not detected**"
        lines.append(f"| {n} | {m['scenario']} | {m['start'][11:19]} → {m['end'][11:19]} | {ids} | "
                     f"{_fmt(m['mttd_seconds'], digits=0)} |")

    lines += ["", "## Known limitation: demo-db-app baseline noise", "",
              "demo-db-app's metrics are almost flat (CPU ~2%, memory ~11.5%, no errors), so its Isolation "
              "Forest baseline is very tight and small wobbles become low/medium ML-only incidents with no "
              "Kubernetes signals. They overlap no injection, so they count as false positives, attributed "
              "to db_failure (the scenario that targets demo-db-app). Not fixed during the evaluation.",
              "", "## Other notes", "",
              "- **MTTD** is the incident's `stored_at` minus the injection start. When an injection merges into "
              "an incident that noise had already opened, the detection is real but MTTD is negative and says "
              "nothing about detection speed (error_spike round 3 on the fixed code: an incident opened by noise "
              "97 s earlier absorbed it).",
              "- **cpu_exhaustion kind**: the load job drives CPU to 75–85% of the pods' limit, just under the "
              "85% `cpu_saturation` threshold, so the CPU incidents are detected but without that kind. The "
              "threshold was not changed during the evaluation."]
    lines += ["", "![F1 by scenario](f1_by_scenario.png)", ""]
    RESULTS_MD.write_text("\n".join(lines), encoding="utf-8")


def plot_f1(result: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sc = result["scores"]["scenarios"]
    names = _order(sc)
    f1 = [sc[s]["f1"] or 0 for s in names]
    names.append("overall")
    f1.append(result["scores"]["overall"]["f1"] or 0)

    fig, ax = plt.subplots(figsize=(8, 3.6), dpi=200)
    hatches = ["", "//", "\\\\", "xx", "..", "--", "++", "oo"]
    for i, (n, v) in enumerate(zip(names, f1)):
        ax.bar(i, v, color="black" if n == "overall" else "white", edgecolor="black",
               hatch="" if n == "overall" else hatches[i % len(hatches)], linewidth=1)
        ax.text(i, v + 0.02, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(names)), [n.replace("_", "\n") for n in names], fontsize=8)
    ax.set_ylim(0, 1.12)
    ax.set_ylabel("F1 score")
    ax.set_title("Detection F1 by chaos scenario", fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(F1_PNG)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Evaluate detection against chaos/injections.jsonl.")
    parser.add_argument("--since", help="Only injections starting at/after this UTC time")
    parser.add_argument("--margin", type=float, default=6,
                        help="Minutes around the injections that count as the evaluation period (default 6)")
    parser.add_argument("--include-incomplete", action="store_true", help="Also score interrupted injections")
    parser.add_argument("--fixed-since", default=FIX_SINCE,
                        help=f"Flask injections from this UTC time get a fixed-code table (default {FIX_SINCE})")
    parser.add_argument("--fixed-commit", default=FIX_COMMIT, help="The fix's commit, for the report")
    args = parser.parse_args()
    # results.md has arrows and dashes; a redirected Windows console defaults to cp1252
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    from incident_store import IncidentStore

    since = ts(args.since) if args.since else None
    injections = load_injections(INJECTIONS, since, args.include_incomplete)
    if not injections:
        raise SystemExit(f"No injections in {INJECTIONS}. Run the chaos/ scenarios first.")

    store = IncidentStore()
    if not store.is_connected():
        raise SystemExit("Cannot reach MongoDB.")
    margin = timedelta(minutes=args.margin)
    period_start = min(i.start for i in injections) - margin
    period_end = max(i.end for i in injections) + GRACE + margin
    incidents = load_incidents(store, period_start, period_end)

    matches, false_positives = match_all(injections, incidents)
    scores = score(matches, false_positives, SEPARATE)
    grades = sync_rca_labels(matches)
    extras = per_scenario_extras(matches, grades)

    detected_ids = {m.first["incident_id"] for m in matches if m.detected and m.first.get("diagnosis")}
    graded = [grades[i] for i in detected_ids if grades.get(i) is not None]
    result = {
        "generated": str(ts(pd.Timestamp.now(tz="UTC")).floor("s")),
        "period": {"start": str(period_start.floor("s")), "end": str(period_end.floor("s"))},
        "n_injections": len(injections), "n_incidents": len(incidents),
        "scores": scores, "extras": extras,
        "rca_overall": {"graded": len(graded), "ungraded": len(detected_ids) - len(graded),
                        "accuracy": sum(graded) / len(graded) if graded else None,
                        "backfilled": sum(bool(m.first["diagnosis"].get("backfilled"))
                                          for m in matches if m.detected and m.first.get("diagnosis")),
                        "detected": sum(m.detected for m in matches),
                        "diagnosed": len(detected_ids),
                        "models": sorted({m.first["diagnosis"].get("model") or "unrecorded"
                                          for m in matches if m.detected and m.first.get("diagnosis")})},
        # every logged cycle, not just this period: a --no-rca run has no Gemini cycles of its own
        "cycle_times": cycle_times(None),
        "pipeline_usage": pipeline_usage(period_start, period_end),
        "fix_commit": args.fixed_commit,
        "fixed_code": fixed_code_flask(matches, false_positives, ts(args.fixed_since)),
        "matches": [{"scenario": m.injection.scenario, "start": str(m.injection.start), "end": str(m.injection.end),
                     "incident_ids": [d["incident_id"] for d in m.incidents],
                     "mttd_seconds": m.mttd_seconds()} for m in matches],
        "false_positives": [{"incident_id": d["incident_id"], "workload": d.get("workload"),
                             "start_time": d["start_time"], "max_severity": d["max_severity"]}
                            for d in false_positives],
    }
    RESULTS_JSON.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    write_markdown(result)
    plot_f1(result)
    print(RESULTS_MD.read_text(encoding="utf-8"))
    print(f"Wrote {RESULTS_MD.name}, {RESULTS_JSON.name}, {F1_PNG.name}; grade RCA rows in {LABELS.name}.")


if __name__ == "__main__":
    main()
