"""
Pulls 15-minute sliding windows of Prometheus metrics for a target pod/service.
Captures CPU, memory, network, HTTP latency, and resource quota signals.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional
import aiohttp

# Allow override via env var for running outside the cluster
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
WINDOW_MINUTES = 15
STEP_SECONDS = 15  # resolution

logger = logging.getLogger(__name__)


async def check_prometheus_reachable() -> bool:
    """Quick connectivity check — logs a clear error if Prometheus is unreachable."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{PROMETHEUS_URL}/-/healthy",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    return True
                logger.warning(f"Prometheus at {PROMETHEUS_URL} returned HTTP {resp.status}")
                return False
    except Exception as e:
        logger.error(f"Prometheus unreachable at {PROMETHEUS_URL}: {e}. Set PROMETHEUS_URL env var or start port-forward: kubectl port-forward -n monitoring svc/prometheus-stack-kube-prom-prometheus 9090:9090")
        return False


async def _query_range(session: aiohttp.ClientSession, query: str, start: float, end: float) -> list[dict]:
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": STEP_SECONDS,
    }
    try:
        async with session.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                logger.warning(f"Prometheus range query returned {resp.status} for: {query[:80]}")
                return []
            data = await resp.json()
            return data.get("data", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Prometheus range query failed ({PROMETHEUS_URL}): {e} | query: {query[:80]}")
        return []


async def _query_instant(session: aiohttp.ClientSession, query: str) -> list[dict]:
    try:
        async with session.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.warning(f"Prometheus instant query returned {resp.status} for: {query[:80]}")
                return []
            data = await resp.json()
            return data.get("data", {}).get("result", [])
    except Exception as e:
        logger.warning(f"Prometheus instant query failed ({PROMETHEUS_URL}): {e} | query: {query[:80]}")
        return []


def _extract_series(results: list[dict], label_key: str = "pod") -> dict:
    """Flatten range query results into {label: [(timestamp, value), ...]}."""
    series = {}
    for r in results:
        key = r.get("metric", {}).get(label_key, "unknown")
        series[key] = [(float(ts), float(val)) for ts, val in r.get("values", [])]
    return series


async def collect_metrics_window(
    namespace: str,
    service: str,
    fault_start: Optional[datetime] = None,
    window_minutes: int = WINDOW_MINUTES,
) -> dict:
    """
    Collect all metric signals for a service over a time window.
    If fault_start is given, window ends at fault_start + window_minutes.
    Otherwise uses now - window_minutes to now.
    """
    now = datetime.now(timezone.utc)
    if fault_start:
        end_ts = (fault_start + timedelta(minutes=window_minutes)).timestamp()
        start_ts = fault_start.timestamp()
    else:
        end_ts = now.timestamp()
        start_ts = (now - timedelta(minutes=window_minutes)).timestamp()

    pod_selector = f'namespace="{namespace}", pod=~"{service}-.*"'

    queries = {
        # CPU
        "cpu_usage_millicores": f'rate(container_cpu_usage_seconds_total{{{pod_selector}, container!=""}}[2m]) * 1000',
        "cpu_throttled_ratio": f'rate(container_cpu_throttled_seconds_total{{{pod_selector}}}[2m]) / rate(container_cpu_usage_seconds_total{{{pod_selector}, container!=""}}[2m])',
        # Memory
        "memory_working_set_mib": f'container_memory_working_set_bytes{{{pod_selector}, container!=""}} / 1048576',
        "memory_rss_mib": f'container_memory_rss{{{pod_selector}, container!=""}} / 1048576',
        "memory_limit_mib": f'container_spec_memory_limit_bytes{{{pod_selector}, container!=""}} / 1048576',
        # Restarts
        "restart_count": f'kube_pod_container_status_restarts_total{{{pod_selector}}}',
        "restart_rate_10m": f'increase(kube_pod_container_status_restarts_total{{{pod_selector}}}[10m])',
        # Network
        "network_rx_bytes": f'rate(container_network_receive_bytes_total{{namespace="{namespace}", pod=~"{service}-.*"}}[2m])',
        "network_tx_bytes": f'rate(container_network_transmit_bytes_total{{namespace="{namespace}", pod=~"{service}-.*"}}[2m])',
        "network_rx_errors": f'rate(container_network_receive_errors_total{{namespace="{namespace}", pod=~"{service}-.*"}}[2m])',
        "network_tx_errors": f'rate(container_network_transmit_errors_total{{namespace="{namespace}", pod=~"{service}-.*"}}[2m])',
    }

    async with aiohttp.ClientSession() as session:
        tasks = {
            name: _query_range(session, q, start_ts, end_ts)
            for name, q in queries.items()
        }
        results = {}
        for name, coro in tasks.items():
            results[name] = await coro

        # Instant queries (current state)
        instant = {
            "pod_phase": await _query_instant(session, f'kube_pod_status_phase{{namespace="{namespace}", pod=~"{service}-.*"}}'),
            "oom_events": await _query_instant(session, f'kube_pod_container_status_last_terminated_reason{{namespace="{namespace}", pod=~"{service}-.*", reason="OOMKilled"}}'),
            "resource_quota_used": await _query_instant(session, f'kube_resourcequota{{namespace="{namespace}", type="used"}}'),
            "resource_quota_hard": await _query_instant(session, f'kube_resourcequota{{namespace="{namespace}", type="hard"}}'),
            "hpa_current_replicas": await _query_instant(session, f'kube_horizontalpodautoscaler_status_current_replicas{{namespace="{namespace}"}}'),
            "hpa_max_replicas": await _query_instant(session, f'kube_horizontalpodautoscaler_spec_max_replicas{{namespace="{namespace}"}}'),
            "node_memory_pressure": await _query_instant(session, 'kube_node_status_condition{condition="MemoryPressure", status="true"}'),
            "node_cpu_pressure": await _query_instant(session, 'kube_node_status_condition{condition="PIDPressure", status="true"}'),
            "node_disk_pressure": await _query_instant(session, 'kube_node_status_condition{condition="DiskPressure", status="true"}'),
        }

    # Summarize time-series: extract min/max/mean/last per metric per pod
    window_summary = {}
    for metric_name, raw_results in results.items():
        series = _extract_series(raw_results)
        for pod, datapoints in series.items():
            if not datapoints:
                continue
            values = [v for _, v in datapoints]
            if pod not in window_summary:
                window_summary[pod] = {}
            window_summary[pod][metric_name] = {
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
                "last": values[-1],
                "trend": values[-1] - values[0] if len(values) > 1 else 0.0,
                "samples": len(values),
            }

    return {
        "service": service,
        "namespace": namespace,
        "window_start": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
        "window_end": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        "window_minutes": window_minutes,
        "pod_metrics": window_summary,
        "instant_state": instant,
    }
