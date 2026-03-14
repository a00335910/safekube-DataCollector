"""Jaeger HTTP API client for querying distributed traces."""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
import aiohttp

from src.common.logger import get_logger
from src.common.config import settings

logger = get_logger("telemetry.tracing_client")

# Jaeger query API base URL — in-cluster address
JAEGER_BASE_URL = "http://jaeger.monitoring:16686"

# How far back to look by default (microseconds)
DEFAULT_LOOKBACK_SECONDS = 3600


def _now_us() -> int:
    """Current time in microseconds (Jaeger's time unit)."""
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def _seconds_ago_us(seconds: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(seconds=seconds)).timestamp() * 1_000_000)


class TracingClient:
    """Query Jaeger for traces relevant to SafeKube incident diagnosis."""

    def __init__(self, base_url: str = JAEGER_BASE_URL):
        self.base_url = base_url.rstrip("/")

    # ------------------------------------------------------------------
    # Core HTTP helper
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict) -> Optional[dict]:
        url = f"{self.base_url}{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    logger.warning(
                        f"Jaeger API {url} returned {resp.status}",
                        extra={"incident_id": None},
                    )
                    return None
        except Exception as e:
            logger.error(f"Jaeger query failed ({url}): {e}", extra={"incident_id": None})
            return None

    # ------------------------------------------------------------------
    # Public query methods
    # ------------------------------------------------------------------

    async def get_traces_for_service(
        self,
        service: str,
        lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
        limit: int = 20,
    ) -> list[dict]:
        """Return recent traces that involve *service* as root span."""
        data = await self._get(
            "/api/traces",
            {
                "service": service,
                "start": _seconds_ago_us(lookback_seconds),
                "end": _now_us(),
                "limit": limit,
            },
        )
        if data is None:
            return []
        return data.get("data", [])

    async def get_error_traces(
        self,
        service: str,
        lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
        limit: int = 20,
    ) -> list[dict]:
        """Return traces that contain at least one error span for *service*."""
        data = await self._get(
            "/api/traces",
            {
                "service": service,
                "tags": '{"error":"true"}',
                "start": _seconds_ago_us(lookback_seconds),
                "end": _now_us(),
                "limit": limit,
            },
        )
        if data is None:
            return []
        return data.get("data", [])

    async def get_dependency_graph(self) -> list[dict]:
        """Return the service dependency graph as computed by Jaeger.

        Each entry is:
          {"parent": "frontend", "child": "cartservice", "callCount": 42}
        """
        end_us = _now_us()
        start_us = _seconds_ago_us(DEFAULT_LOOKBACK_SECONDS)
        data = await self._get(
            "/api/dependencies",
            {"endTs": end_us // 1000, "lookback": DEFAULT_LOOKBACK_SECONDS * 1000},
        )
        if data is None:
            return []
        return data.get("data", [])

    async def find_root_cause_service(
        self,
        service: str,
        lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
    ) -> Optional[str]:
        """Heuristic: find the downstream service with the most errors in recent traces.

        Walks error traces for *service*, inspects every span, counts error
        occurrences per service name, and returns the service with the highest count.
        Returns None if no error traces are found.
        """
        traces = await self.get_error_traces(service, lookback_seconds)
        if not traces:
            return None

        error_counts: dict[str, int] = {}
        for trace in traces:
            for span in trace.get("spans", []):
                if not _span_has_error(span):
                    continue
                svc = _span_service_name(span, trace)
                if svc:
                    error_counts[svc] = error_counts.get(svc, 0) + 1

        if not error_counts:
            return None

        root_cause_svc = max(error_counts, key=lambda s: error_counts[s])
        logger.info(
            f"Error counts by service for traces from '{service}': {error_counts}. "
            f"Root cause candidate: '{root_cause_svc}'",
            extra={"incident_id": None},
        )
        return root_cause_svc

    async def get_service_latency_stats(
        self,
        service: str,
        lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
        limit: int = 100,
    ) -> dict:
        """Return p50/p95/p99 latency in ms for root spans of *service*."""
        traces = await self.get_traces_for_service(service, lookback_seconds, limit)
        durations_us = []
        for trace in traces:
            spans = trace.get("spans", [])
            if not spans:
                continue
            # Root span has no references (or references with refType != CHILD_OF)
            root = _find_root_span(spans)
            if root:
                durations_us.append(root.get("duration", 0))

        if not durations_us:
            return {"p50_ms": None, "p95_ms": None, "p99_ms": None, "sample_count": 0}

        durations_us.sort()
        n = len(durations_us)
        return {
            "p50_ms": durations_us[int(n * 0.50)] / 1000,
            "p95_ms": durations_us[int(n * 0.95)] / 1000,
            "p99_ms": durations_us[int(n * 0.99)] / 1000,
            "sample_count": n,
        }


# ------------------------------------------------------------------
# Private span helpers
# ------------------------------------------------------------------

def _span_has_error(span: dict) -> bool:
    for tag in span.get("tags", []):
        if tag.get("key") == "error" and str(tag.get("value", "")).lower() in ("true", "1"):
            return True
    return False


def _span_service_name(span: dict, trace: dict) -> Optional[str]:
    """Resolve the service name for a span via the trace's process map."""
    process_id = span.get("processID")
    if not process_id:
        return None
    processes = trace.get("processes", {})
    process = processes.get(process_id, {})
    # Check serviceName tag first, then process.serviceName
    for tag in process.get("tags", []):
        if tag.get("key") == "service.name":
            return tag.get("value")
    return process.get("serviceName")


def _find_root_span(spans: list[dict]) -> Optional[dict]:
    """Return the span with no CHILD_OF reference (i.e. the trace root)."""
    for span in spans:
        refs = span.get("references", [])
        if not any(r.get("refType") == "CHILD_OF" for r in refs):
            return span
    return spans[0] if spans else None


# Singleton
tracing_client = TracingClient()
