"""
rca_engine.py

The "AI Intelligence Layer > Root Cause Analysis" and the entire
"LLM Explanation Layer" from the architecture diagram, combined.

Given an Incident (from correlation_engine.py), this module:
  1. Retrieves similar PAST incidents from ChromaDB (the "R" in RAG) —
     grounding the explanation in real history instead of the LLM
     guessing blindly.
  2. Sends the current incident + retrieved context to Gemini (the "AG"
     in RAG) to generate a plain-English summary, likely root cause,
     a confidence-worded explanation, and a suggested fix.
  3. Stores the current incident into ChromaDB too, so future incidents
     can be compared against it — the system gets smarter over time.

This is explicitly ADVISORY ONLY, matching the diagram's label
"LLM Explanation Layer - Advisory / Human-Reviewed (No Automation)".
Nothing in this file takes any healing action on its own.
"""

import os
import json
import time
from datetime import datetime, timezone

import chromadb
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from config import GEMINI_MODEL, ML_ENGINE_DIR  # config loads GOOGLE_API_KEY from ml-engine/.env
from correlation_engine import Incident
from evidence import describe as describe_evidence
from incident_store import IncidentStore
from k8s_signals import describe_signal

CHROMA_PATH = str(ML_ENGINE_DIR / "chroma_store")  # same folder whatever the working directory
USAGE_LOG = ML_ENGINE_DIR / "logs" / "gemini_usage.jsonl"  # one line per successful call; scripts/gemini_usage.py


def log_usage(response, incident_id: str, model: str, seconds: float, path=None) -> None:
    """Appends the call's token counts. Best effort: never lets logging break a diagnosis."""
    try:
        usage = getattr(response, "usage_metadata", None) or {}
        if not usage:  # older langchain-google-genai: only Gemini's own field names
            raw = (getattr(response, "response_metadata", None) or {}).get("usage_metadata") or {}
            usage = {"input_tokens": raw.get("prompt_token_count"), "output_tokens": raw.get("candidates_token_count")}
        record = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "incident_id": incident_id,
                  "model": model, "input_tokens": usage.get("input_tokens"),
                  "output_tokens": usage.get("output_tokens"), "seconds": round(seconds, 3)}
        path = path or USAGE_LOG   # looked up per call, so tests can point it elsewhere
        path.parent.mkdir(exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        print(f"Warning: couldn't log Gemini usage: {e}")
COLLECTION_NAME = "past_incidents"

DETECTION_LABELS = {
    "ml": "Isolation Forest on cpu/memory/error-rate",
    "rule": "Kubernetes state rules (the ML model flagged nothing)",
    "ml+rule": "Isolation Forest on cpu/memory/error-rate AND Kubernetes state rules",
}


SYSTEM_PROMPT = """You are an SRE (Site Reliability Engineering) assistant helping
diagnose Kubernetes cluster incidents. You will be given:
1. Details of a CURRENT incident (anomalous metrics detected by an ML model),
   possibly with Kubernetes state signals from kube-state-metrics: container
   restarts and their termination reason (e.g. OOMKilled, Error), containers
   stuck waiting (e.g. CrashLoopBackOff, ImagePullBackOff, ErrImagePull),
   deployments with unavailable replicas, node pressure (MemoryPressure,
   DiskPressure), evicted or unschedulable pods, and PREDICTION warnings
   (a resource trending to its limit, with the predicted seconds left)
2. Evidence, when available: Kubernetes events and error lines from the
   affected pods' logs (Loki)
3. Similar PAST incidents retrieved from history, if any exist

Your job is to produce a clear, human-readable diagnosis. Be concise and
practical - an engineer will read this under time pressure during an
active incident. Do not invent facts not supported by the data given.
Kubernetes signals, events and log lines are direct evidence of what the
cluster observed - when present, name them explicitly in the root cause
(e.g. which container was OOMKilled, the exact error a log line shows)
rather than inferring the cause from metrics alone. A PREDICTION alone means
the failure has not happened yet; say so.

Respond ONLY in this exact JSON format, nothing else:
{
  "summary": "one or two sentence plain-English recap of what happened",
  "likely_root_cause": "your best hypothesis for why this happened",
  "confidence_reasoning": "brief note on how certain this diagnosis is and why",
  "suggested_fix": "a concrete, actionable next step for the engineer to try"
}
"""


class RCAEngine:
    def __init__(self, google_api_key: str = None):
        api_key = google_api_key or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError(
                "No GOOGLE_API_KEY found. Add it to your .env file in ml-engine/."
            )

        self.llm = ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=api_key,
            temperature=0.2,  # low temperature: we want grounded, consistent diagnoses, not creative ones
        )

        # Local ChromaDB instance - persists to disk in ./chroma_store
        self.chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
        self.collection = self.chroma_client.get_or_create_collection(COLLECTION_NAME)

        # MongoDB for persisting the incident + its diagnosis together.
        # Wrapped in a try so a down/missing Mongo doesn't crash diagnosis -
        # you still get your answer, it just won't be saved.
        try:
            self.store = IncidentStore()
            self.mongo_available = self.store.is_connected()
        except Exception:
            self.store = None
            self.mongo_available = False

        if not self.mongo_available:
            print("Warning: MongoDB not reachable. Diagnoses will not be persisted.")

    def _incident_to_text(self, incident: Incident, evidence: dict | None = None) -> str:
        """
        Converts an Incident into a plain-text description, both for
        storing in Chroma (which needs text to embed) and for feeding
        into the LLM prompt.
        """
        metrics_lines = []
        for a in incident.anomalies:
            metrics_lines.append(
                f"  - at {a.timestamp}: {a.metric_snapshot} (severity={a.severity})"
            )
        anomalies_block = (
            "Anomalies:\n" + "\n".join(metrics_lines)
            if metrics_lines
            else "Anomalies: none - the ML model saw nothing unusual in cpu/memory/error-rate."
        )
        signals_block = (
            "\nKubernetes signals:\n" + "\n".join(f"  - {describe_signal(s)}" for s in incident.k8s_signals)
            if incident.k8s_signals
            else ""
        )
        evidence_text = describe_evidence(evidence) if evidence else ""
        evidence_block = f"\n{evidence_text}" if evidence_text else ""
        return (
            f"Incident {incident.incident_id}: {incident.start_time} to {incident.end_time}\n"
            f"Workload: {incident.workload}\n"
            f"Detected by: {DETECTION_LABELS.get(incident.detection, incident.detection)}\n"
            f"Max severity: {incident.max_severity}, avg confidence: {incident.avg_confidence:.2f}\n"
            f"{anomalies_block}{signals_block}{evidence_block}"
        )

    def _retrieve_similar_incidents(self, incident_text: str, n_results: int = 3) -> list[str]:
        """
        Queries ChromaDB for past incidents with similar text/metric
        patterns. Returns empty list if the store is still empty
        (e.g. this is the very first incident ever seen).
        """
        count = self.collection.count()
        if count == 0:
            return []

        results = self.collection.query(
            query_texts=[incident_text],
            n_results=min(n_results, count),
        )
        return results["documents"][0] if results["documents"] else []

    def _store_incident(self, incident: Incident, incident_text: str):
        """Saves this incident into Chroma so future incidents can retrieve it."""
        # upsert: a merged incident can be re-diagnosed under the same id
        self.collection.upsert(
            documents=[incident_text],
            ids=[incident.incident_id],
            metadatas=[{
                "severity": incident.max_severity,
                "confidence": incident.avg_confidence,
                "start_time": str(incident.start_time),
            }],
        )

    def diagnose(self, incident: Incident, persist: bool = True, evidence: dict | None = None) -> dict:
        """
        Main entry point. Returns a dict with summary, likely_root_cause,
        confidence_reasoning, and suggested_fix - ready to hand to the
        React dashboard.

        persist=False skips the MongoDB write, for callers (pipeline.py)
        that check the result first and save it themselves. Either way, the
        incident is added to Chroma only when Gemini returned a valid
        diagnosis -- a failed call or unparseable reply is never stored as
        history for future incidents to retrieve.

        If the Gemini call itself fails, the returned dict has an "error"
        key (and no diagnosis fields); it never raises for that.

        `evidence` (evidence.gather) adds K8s events and log lines to the prompt.
        """
        incident_text = self._incident_to_text(incident, evidence)
        similar_past = self._retrieve_similar_incidents(incident_text)

        context_block = (
            "No similar past incidents found - this may be a novel issue."
            if not similar_past
            else "Similar past incidents:\n" + "\n---\n".join(similar_past)
        )

        user_prompt = (
            f"CURRENT INCIDENT:\n{incident_text}\n\n"
            f"{context_block}\n\n"
            f"Provide your diagnosis in the required JSON format."
        )

        try:
            started = time.perf_counter()
            response = self.llm.invoke([
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=user_prompt),
            ])
            log_usage(response, incident.incident_id, getattr(self.llm, "model", None), time.perf_counter() - started)
        except Exception as e:
            # quota, network, bad model name... the caller keeps the incident undiagnosed
            return {
                "incident_id": incident.incident_id,
                "error": f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"[:300],
            }

        try:
            # Gemini/LangChain sometimes returns response.content as a list of
            # content blocks instead of a plain string - normalize either way
            content = response.content
            if isinstance(content, list):
                content = "".join(
                    block.get("text", "") if isinstance(block, dict) else str(block)
                    for block in content
                )

            # Strip markdown code fences if Gemini wraps the JSON in them
            raw = content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            diagnosis = json.loads(raw.strip())
            if not isinstance(diagnosis, dict):
                raise ValueError("diagnosis is not a JSON object")
        except (ValueError, IndexError):  # json.JSONDecodeError is a ValueError
            diagnosis = {
                "summary": "Could not parse LLM response as JSON.",
                "likely_root_cause": "unknown",
                "confidence_reasoning": "N/A",
                "suggested_fix": "N/A",
                "raw_response": str(response.content),
            }

        diagnosis["incident_id"] = incident.incident_id
        diagnosis["similar_incidents_found"] = len(similar_past)

        # Store this incident AFTER diagnosing, so it doesn't retrieve itself,
        # and only a real diagnosis -- not the parse-failure fallback
        if "raw_response" not in diagnosis:
            self._store_incident(incident, incident_text)

        # Persist to MongoDB: the raw incident first (so it exists as a
        # document), then attach the diagnosis to it.
        if persist and self.mongo_available:
            self.store.save_incident(incident)
            self.store.save_diagnosis(incident.incident_id, diagnosis)

        return diagnosis


if __name__ == "__main__":
    from datetime import timedelta
    from anomaly_detector import AnomalyDetector, generate_mock_metrics
    from correlation_engine import CorrelationEngine

    # Build one incident to diagnose, same pipeline as before
    data = generate_mock_metrics()
    feature_cols = ["cpu_percent", "memory_percent", "error_rate"]

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(data, feature_columns=feature_cols)
    anomalies = detector.score_batch(data)

    engine = CorrelationEngine(time_window=timedelta(minutes=3))
    incidents = engine.correlate(anomalies)

    # Diagnose the highest-severity incident as a demo
    high_severity = [i for i in incidents if i.max_severity == "high"]
    target = high_severity[0] if high_severity else incidents[0]

    print(f"Diagnosing incident {target.incident_id}...\n")

    rca = RCAEngine()
    result = rca.diagnose(target)

    print(json.dumps(result, indent=2))