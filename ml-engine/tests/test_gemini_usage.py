"""
tests/test_gemini_usage.py

rca_engine.log_usage writes one JSON line per Gemini call (from LangChain's
usage_metadata, or Gemini's own field names on older versions, and never
raises), and scripts/gemini_usage.py sums them. No Gemini call is made.

Run from ml-engine/:
    python -m pytest tests/test_gemini_usage.py
"""

import json
import os
import sys
from types import SimpleNamespace

ML_ENGINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ML_ENGINE)
sys.path.insert(0, os.path.join(os.path.dirname(ML_ENGINE), "scripts"))

from rca_engine import log_usage  # noqa: E402
from gemini_usage import load, summarise  # noqa: E402


def test_log_usage_writes_one_line_per_call(tmp_path):
    path = tmp_path / "logs" / "gemini_usage.jsonl"
    log_usage(SimpleNamespace(usage_metadata={"input_tokens": 1200, "output_tokens": 300, "total_tokens": 1500}),
              "abc12345", "gemini-flash-lite-latest", 2.3456, path)
    # older langchain: only response_metadata with Gemini's names
    log_usage(SimpleNamespace(usage_metadata=None,
                              response_metadata={"usage_metadata": {"prompt_token_count": 800,
                                                                    "candidates_token_count": 100}}),
              "def67890", "gemini-flash-lite-latest", 1.0, path)
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [(r["incident_id"], r["input_tokens"], r["output_tokens"]) for r in lines] == [
        ("abc12345", 1200, 300), ("def67890", 800, 100)]
    assert lines[0]["model"] == "gemini-flash-lite-latest" and lines[0]["seconds"] == 2.346
    assert set(lines[0]) == {"time", "incident_id", "model", "input_tokens", "output_tokens", "seconds"}

    s = summarise(load(path))
    assert s["calls"] == 2 and s["input_tokens"] == 2000 and s["output_tokens"] == 400
    assert s["mean_input_tokens"] == 1000 and s["mean_output_tokens"] == 200


def test_log_usage_never_raises(tmp_path):
    blocked = tmp_path / "file"
    blocked.write_text("x")
    log_usage(SimpleNamespace(usage_metadata={"input_tokens": 1}), "x", "m", 0.1, blocked / "sub" / "u.jsonl")


def test_since_filter_and_empty(tmp_path):
    path = tmp_path / "u.jsonl"
    path.write_text('{"time": "2026-10-16 08:00:00", "input_tokens": 5, "output_tokens": 1, "seconds": 1}\n'
                    '{"time": "2026-10-16 09:00:00", "input_tokens": 7, "output_tokens": null, "seconds": 3}\n',
                    encoding="utf-8")
    s = summarise(load(path, since="2026-10-16 08:30"))
    assert s["calls"] == 1 and s["input_tokens"] == 7 and s["output_tokens"] == 0
    assert summarise(load(tmp_path / "missing.jsonl"))["calls"] == 0
