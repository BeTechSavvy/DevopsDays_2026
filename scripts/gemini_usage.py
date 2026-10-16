"""
scripts/gemini_usage.py

Summarises ml-engine/logs/gemini_usage.jsonl (one line per successful Gemini
call, written by rca_engine.py): total calls, input and output tokens, and the
mean per call -- overall and per model.

    python scripts/gemini_usage.py
    python scripts/gemini_usage.py --since "2026-10-16 08:41"     # UTC
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

LOG = Path(__file__).resolve().parents[1] / "ml-engine" / "logs" / "gemini_usage.jsonl"


def load(path: Path, since: str | None = None) -> list[dict]:
    if not path.exists():
        return []
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [r for r in records if since is None or r["time"] >= since]


def summarise(records: list[dict]) -> dict:
    """{"calls", "input_tokens", "output_tokens", "seconds", "mean_*"}; missing token counts count as 0."""
    n = len(records)
    totals = {k: sum(r.get(k) or 0 for r in records) for k in ("input_tokens", "output_tokens", "seconds")}
    return {"calls": n, **totals, **{f"mean_{k}": (v / n if n else None) for k, v in totals.items()}}


def main():
    parser = argparse.ArgumentParser(description="Gemini calls and tokens from gemini_usage.jsonl.")
    parser.add_argument("--since", help='Only calls at/after this UTC time, e.g. "2026-10-16 08:41"')
    parser.add_argument("--log", type=Path, default=LOG)
    args = parser.parse_args()

    records = load(args.log, args.since)
    if not records:
        print(f"No Gemini calls logged in {args.log}" + (f" since {args.since}" if args.since else "") + ".")
        return

    by_model = defaultdict(list)
    for r in records:
        by_model[r.get("model") or "unknown"].append(r)

    print(f"{'model':28} {'calls':>6} {'input tok':>11} {'output tok':>11} {'mean in':>9} {'mean out':>9} {'mean s':>7}")
    for model, rs in sorted(by_model.items()) + [("TOTAL", records)]:
        s = summarise(rs)
        print(f"{model:28} {s['calls']:>6} {s['input_tokens']:>11,} {s['output_tokens']:>11,} "
              f"{s['mean_input_tokens']:>9,.0f} {s['mean_output_tokens']:>9,.0f} {s['mean_seconds']:>7.1f}")


if __name__ == "__main__":
    main()
