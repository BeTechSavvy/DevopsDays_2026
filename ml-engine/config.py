"""
config.py

One place for the connection settings every module needs. Values come from
environment variables, or from ml-engine/.env (see .env.example), falling
back to the defaults below -- the values the project has always used.

Loaded by path, not from the current directory, so the chaos/ and eval/
scripts that run from the repo root pick up the same .env.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

ML_ENGINE_DIR = Path(__file__).resolve().parent
load_dotenv(ML_ENGINE_DIR / ".env")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "chaos_healer")
API_PORT = int(os.environ.get("API_PORT", 5050))
# PROM_URL is the older name, still honoured
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", os.environ.get("PROM_URL", "http://localhost:9090"))
LOKI_URL = os.environ.get("LOKI_URL", "http://localhost:3100")
FLASK_URL = os.environ.get("FLASK_URL", "http://127.0.0.1:30001")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")
