"""Shared pytest setup: tests never write to the real ml-engine/logs/gemini_usage.jsonl."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(autouse=True)
def _gemini_usage_log_in_tmp(tmp_path, monkeypatch):
    import rca_engine
    monkeypatch.setattr(rca_engine, "USAGE_LOG", tmp_path / "gemini_usage.jsonl")
