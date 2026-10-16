"""
tests/test_evidence.py

Offline checks for evidence.py: log lines that differ only in numbers/ids
collapse to one, ANSI colours are stripped, and Warning events win the
limited slots over routine Normal events.

Run from ml-engine/:
    python -m pytest tests/test_evidence.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import evidence  # noqa: E402


def test_dedupe_collapses_repeats_and_strips_colours():
    lines = [
        "2026-10-16 08:02:15,635 ERROR database error: cannot reach redis at demo-redis:6379: Error 111",
        "2026-10-16 08:02:16,143 ERROR database error: cannot reach redis at demo-redis:6379: Error 111",
        '10.244.0.14 - - [16/Oct/2026 07:04:26] "\x1b[35m\x1b[1mGET /work HTTP/1.1\x1b[0m" 500 -',
    ]
    out = evidence.dedupe(lines)
    assert len(out) == 2
    assert out[1] == '10.244.0.14 - - [16/Oct/2026 07:04:26] "GET /work HTTP/1.1" 500 -'


def test_dedupe_respects_limit():
    lines = [f"FATAL error number {i} in module m{chr(97 + i)}" for i in range(20)]
    assert len(evidence.dedupe(lines, limit=8)) == 8


def test_warning_events_come_first():
    t = pd.Timestamp("2026-10-16 07:47:00")
    # distinct names in letters: names differing only in digits dedupe to one message by design
    events = [("Normal", t + pd.Timedelta(seconds=i), f"Normal Started Pod/pod-{chr(97 + i)}: Container started")
              for i in range(8)]
    events.append(("Warning", t + pd.Timedelta(minutes=1),
                   'Warning Failed Pod/chaos-rollout-x: Failed to pull image "flask-demo:v2-does-not-exist"'))
    ordered = evidence.order_events(events)
    assert len(ordered) == evidence.MAX_EVENTS
    assert ordered[0].startswith("Warning Failed")


def test_describe_builds_prompt_block():
    text = evidence.describe({"events": ["Warning BackOff Pod/a: Back-off"], "logs": ["FATAL: bad config"]})
    assert "Kubernetes events" in text and "FATAL: bad config" in text
    assert evidence.describe({"events": [], "logs": []}) == ""
