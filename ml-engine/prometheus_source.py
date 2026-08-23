"""
prometheus_source.py

Live drop-in replacement for anomaly_detector.generate_mock_metrics().
Pulls real CPU / memory / error-rate data from Prometheus and returns
it in the exact same DataFrame shape the rest of the pipeline already
expects:

    timestamp | cpu_percent | memory_percent | error_rate

Deliberately named prometheus_source.py, NOT prometheus_client.py --
the official `prometheus_client` pip package (a dependency of
prometheus_flask_exporter) has that exact module name, and a local
file with the same name would shadow it for anything run from this
directory.

Requires Prometheus to be reachable, e.g.:
    kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090

Then either use the default (http://localhost:9090) or override via
the PROM_URL env var if you're reaching it a different way.
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")
NAMESPACE = os.environ.get("PROM_NAMESPACE", "default")
POD_REGEX = os.environ.get("PROM_POD_REGEX", "flask-deployment-.*")

# --- PromQL queries -----------------------------------------------------
# cpu_percent / memory_percent are usage-as-a-fraction-of-limit, which is
# why deployment.yaml needs resources.limits set -- without a limit,
# "percent" has nothing to be a percent OF.

CPU_QUERY = (
    f'100 * sum(rate(container_cpu_usage_seconds_total{{namespace="{NAMESPACE}", '
    f'pod=~"{POD_REGEX}", cpu="total"}}[2m])) '
    f'/ sum(kube_pod_container_resource_limits{{namespace="{NAMESPACE}", '
    f'pod=~"{POD_REGEX}", resource="cpu"}})'
)

MEMORY_QUERY = (
    f'100 * sum(container_memory_working_set_bytes{{namespace="{NAMESPACE}", '
    f'pod=~"{POD_REGEX}"}}) '
    f'/ sum(kube_pod_container_resource_limits{{namespace="{NAMESPACE}", '
    f'pod=~"{POD_REGEX}", resource="memory"}})'
)

# flask_http_request_total{status=...} comes from prometheus_flask_exporter
# in app.py. clamp_min avoids a divide-by-zero (and therefore missing data
# point) whenever there's a quiet window with no traffic at all.
ERROR_RATE_QUERY = (
    '100 * sum(rate(flask_http_request_total{status=~"5.."}[2m])) '
    '/ clamp_min(sum(rate(flask_http_request_total[2m])), 1e-9)'
)

QUERIES = {
    "cpu_percent": CPU_QUERY,
    "memory_percent": MEMORY_QUERY,
    "error_rate": ERROR_RATE_QUERY,
}


class PrometheusClient:
    """Thin wrapper around Prometheus's HTTP query API."""

    def __init__(self, base_url: str = PROM_URL, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def is_reachable(self) -> bool:
        try:
            r = requests.get(f"{self.base_url}/-/ready", timeout=self.timeout)
            return r.status_code == 200
        except requests.RequestException:
            return False

    def query_range(self, promql: str, start: datetime, end: datetime, step: str = "30s") -> list[dict]:
        """Runs a PromQL range query, returns Prometheus's raw 'result' list."""
        resp = requests.get(
            f"{self.base_url}/api/v1/query_range",
            params={
                "query": promql,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        return payload["data"]["result"]

    @staticmethod
    def _series_to_frame(result: list[dict], column: str) -> pd.DataFrame:
        """
        Converts a single-series Prometheus result into a timestamp/value
        frame. Explicit dtypes even when empty, so an empty series doesn't
        crash merge_asof downstream with a dtype mismatch -- it just
        contributes zero rows, which the caller reports clearly instead.
        """
        if not result:
            return pd.DataFrame({
                "timestamp": pd.Series(dtype="datetime64[ns]"),
                column: pd.Series(dtype="float64"),
            })
        values = result[0]["values"]  # [[unix_ts, "value_str"], ...]
        df = pd.DataFrame(values, columns=["timestamp", column])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_localize(None)
        df[column] = pd.to_numeric(df[column], errors="coerce")
        return df

    def get_metrics_dataframe(self, minutes_back: int = 30, step: str = "30s") -> pd.DataFrame:
        """
        Pulls cpu_percent / memory_percent / error_rate over the last
        `minutes_back` minutes and merges them into ONE DataFrame shaped
        exactly like generate_mock_metrics() -- a drop-in replacement
        for everything downstream (AnomalyDetector, CorrelationEngine, etc).
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=minutes_back)

        frames = {}
        for column, promql in QUERIES.items():
            result = self.query_range(promql, start, end, step=step)
            frame = self._series_to_frame(result, column)
            if frame.empty:
                print(f"WARNING: query for '{column}' returned no data points. "
                      f"PromQL: {promql}")
            frames[column] = frame

        non_empty = [f for f in frames.values() if not f.empty]
        if not non_empty:
            print("All three queries returned empty. Nothing to merge yet -- "
                  "check the warnings above, and confirm Prometheus has enough history/traffic.")
            return pd.DataFrame(columns=["timestamp", *QUERIES.keys()])

        merged = non_empty[0]
        for frame in non_empty[1:]:
            merged = pd.merge_asof(
                merged.sort_values("timestamp"),
                frame.sort_values("timestamp"),
                on="timestamp",
                direction="nearest",
                tolerance=pd.Timedelta(step),
            )

        return merged.dropna().reset_index(drop=True)


if __name__ == "__main__":
    # Quick sanity check, mirrors the __main__ block style already used
    # in anomaly_detector.py / correlation_engine.py.
    client = PrometheusClient()

    if not client.is_reachable():
        print(f"Cannot reach Prometheus at {PROM_URL}.")
        print("Run: kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090")
        raise SystemExit(1)

    df = client.get_metrics_dataframe(minutes_back=15)
    print(f"Pulled {len(df)} real data points from Prometheus:\n")
    print(df.tail(10) if len(df) else "(empty -- check namespace/pod regex or generate some traffic first)")

