"""
anomaly_detector.py

The "brain" of Chaos Healer's detection layer.

Takes a stream of metrics (CPU %, memory %, request error rate, etc.)
and learns what "normal" looks like using Isolation Forest, an
unsupervised anomaly detection model. Anything that deviates sharply
from the learned pattern gets flagged as an anomaly, with a score
indicating how confident the model is.

This module is data-source agnostic: it doesn't care whether the
metrics come from a live Prometheus instance or a mock generator.
That separation is intentional, so we can build and test the model
logic before the real Kubernetes cluster is up and feeding data.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from dataclasses import dataclass
from typing import Optional

_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}

# Isolation Forest scores flatten out around -0.1 for any point far outside
# the training range, so the model alone rarely says "high" -- a pod pinned
# at 94% CPU came out "medium". Any metric past these hard limits is high
# regardless of the model's score.
METRIC_HIGH_THRESHOLDS = {
    "cpu_percent": 85,
    "memory_percent": 85,
    "error_rate": 25,
}


@dataclass
class Anomaly:
    """A single detected anomaly, ready to hand off to the diagnosis pipeline."""
    timestamp: pd.Timestamp
    metric_snapshot: dict
    anomaly_score: float  # more negative = more anomalous (raw Isolation Forest score)
    confidence: float     # 0-1, normalized for easier downstream use
    severity: str         # "low" | "medium" | "high"


class AnomalyDetector:
    """
    Wraps scikit-learn's IsolationForest with the specific workflow
    Chaos Healer needs: fit on a window of "normal" history, then
    score new incoming data points as they arrive.
    """

    def __init__(self, contamination: float = 0.05, random_state: int = 42):
        """
        contamination: expected proportion of anomalies in training data.
        0.05 means we assume ~5% of historical data was already abnormal.
        Tune this down if your training window is very clean.
        """
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=150,
        )
        self.feature_columns: Optional[list] = None
        self.is_fitted = False

    def fit(self, historical_df: pd.DataFrame, feature_columns: list):
        """
        Train on a window of historical metrics considered representative
        of normal cluster behavior.

        historical_df: DataFrame with a 'timestamp' column plus metric columns
        feature_columns: which columns to actually feed the model
                          e.g. ['cpu_percent', 'memory_percent', 'error_rate']
        """
        self.feature_columns = feature_columns
        X = historical_df[feature_columns].values
        self.model.fit(X)
        self.is_fitted = True

    def score_batch(self, new_df: pd.DataFrame) -> list[Anomaly]:
        """
        Score a batch of new metric rows. Returns only the ones flagged
        as anomalies (predict == -1), each wrapped as an Anomaly object.
        """
        if not self.is_fitted:
            raise RuntimeError("Call fit() before score_batch(). The model has no notion of 'normal' yet.")

        X = new_df[self.feature_columns].values
        predictions = self.model.predict(X)       # 1 = normal, -1 = anomaly
        raw_scores = self.model.decision_function(X)  # higher = more normal

        anomalies = []
        for i, (pred, score) in enumerate(zip(predictions, raw_scores)):
            if pred == -1:
                row = new_df.iloc[i]
                snapshot = {col: row[col] for col in self.feature_columns}
                anomalies.append(
                    Anomaly(
                        timestamp=row["timestamp"],
                        metric_snapshot=snapshot,
                        anomaly_score=float(score),
                        confidence=self._score_to_confidence(score),
                        severity=max(
                            self._score_to_severity(score),
                            self._metric_severity(snapshot),
                            key=_SEVERITY_RANK.__getitem__,
                        ),
                    )
                )
        return anomalies

    @staticmethod
    def _metric_severity(snapshot: dict) -> str:
        """'high' if any metric is past its hard limit, else 'low' (defer to the model)."""
        for col, limit in METRIC_HIGH_THRESHOLDS.items():
            if snapshot.get(col, 0) > limit:
                return "high"
        return "low"

    @staticmethod
    def _score_to_confidence(raw_score: float) -> float:
        """
        Isolation Forest's decision_function returns roughly [-0.5, 0.5].
        More negative = more anomalous. We flip and squash it into a
        clean 0-1 confidence range for the trust-tier system downstream.
        """
        confidence = (0.5 - raw_score) / 1.0
        return float(np.clip(confidence, 0.0, 1.0))

    @staticmethod
    def _score_to_severity(raw_score: float) -> str:
        if raw_score < -0.2:
            return "high"
        elif raw_score < -0.05:
            return "medium"
        return "low"


def generate_mock_metrics(n_normal: int = 500, n_anomalies: int = 15, seed: int = 7) -> pd.DataFrame:
    """
    Generates fake but realistic-looking Prometheus-style metrics so we
    can build and test the detector before the real cluster is running.

    Normal behavior: CPU/memory hover in a stable band with small noise.
    Injected anomalies: sudden spikes (simulating a real incident, e.g.
    a memory leak or CPU-bound crash loop).
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range("2026-08-14", periods=n_normal, freq="1min")

    cpu = rng.normal(loc=35, scale=5, size=n_normal).clip(0, 100)
    memory = rng.normal(loc=50, scale=6, size=n_normal).clip(0, 100)
    error_rate = rng.normal(loc=0.5, scale=0.2, size=n_normal).clip(0, 100)

    df = pd.DataFrame({
        "timestamp": timestamps,
        "cpu_percent": cpu,
        "memory_percent": memory,
        "error_rate": error_rate,
    })

    # Inject anomaly spikes at random points to simulate real incidents
    anomaly_idxs = rng.choice(n_normal, size=n_anomalies, replace=False)
    for idx in anomaly_idxs:
        df.loc[idx, "cpu_percent"] = rng.uniform(85, 100)
        df.loc[idx, "memory_percent"] = rng.uniform(90, 100)
        df.loc[idx, "error_rate"] = rng.uniform(15, 40)

    return df


if __name__ == "__main__":
    # Quick sanity check: train on mostly-normal data, then see if the
    # model correctly flags the injected spikes.
    data = generate_mock_metrics()
    feature_cols = ["cpu_percent", "memory_percent", "error_rate"]

    detector = AnomalyDetector(contamination=0.05)
    detector.fit(data, feature_columns=feature_cols)

    anomalies = detector.score_batch(data)

    print(f"Total data points: {len(data)}")
    print(f"Anomalies detected: {len(anomalies)}\n")

    for a in anomalies[:10]:
        print(
            f"[{a.timestamp}] severity={a.severity:6s} "
            f"confidence={a.confidence:.2f} "
            f"metrics={a.metric_snapshot}"
        )