"""
Extracts and classifies log lines from pod containers during a fault window.
Looks for error patterns, OOM signals, probe failures, image pull errors, etc.
"""

import re
import asyncio
from datetime import datetime, timezone
from kubernetes import client, config

# Error pattern registry — maps regex to a signal name
ERROR_PATTERNS = {
    "oom_killed":           re.compile(r"OOMKill|killed.*memory|out of memory|oom-kill", re.I),
    "crash_exit":           re.compile(r"exit code [^0]|signal: killed|segfault|panic:", re.I),
    "image_pull":           re.compile(r"ErrImagePull|ImagePullBackOff|Back-off pulling image|failed to pull", re.I),
    "readiness_failed":     re.compile(r"Readiness probe failed|readiness probe.*failed", re.I),
    "liveness_failed":      re.compile(r"Liveness probe failed|liveness probe.*failed", re.I),
    "dns_error":            re.compile(r"dial.*no such host|DNS.*NXDOMAIN|lookup.*no such host|name.*not resolve", re.I),
    "connection_refused":   re.compile(r"connection refused|ECONNREFUSED|connect: connection refused", re.I),
    "timeout":              re.compile(r"context deadline exceeded|timeout|timed out", re.I),
    "permission_denied":    re.compile(r"permission denied|forbidden|unauthorized|403|401", re.I),
    "missing_resource":     re.compile(r"not found|no such file|secret.*not found|configmap.*not found", re.I),
    "resource_exhausted":   re.compile(r"resource exhausted|quota exceeded|limit exceeded|too many requests|429", re.I),
    "network_error":        re.compile(r"network.*unreachable|no route to host|broken pipe|reset by peer", re.I),
    "disk_error":           re.compile(r"disk.*full|no space left|I/O error|input/output error", re.I),
    "http_5xx":             re.compile(r'"(status|code)":\s*5\d{2}|HTTP 5\d{2}|500|503|502|504', re.I),
    "grpc_error":           re.compile(r"rpc error|grpc.*unavailable|grpc.*deadline|UNAVAILABLE|DEADLINE_EXCEEDED", re.I),
    "clock_skew":           re.compile(r"clock skew|time.*drift|certificate.*expired|x509.*expired", re.I),
    "thread_exhaustion":    re.compile(r"thread.*exhausted|goroutine.*leak|too many open files|EMFILE", re.I),
}

SEVERITY_KEYWORDS = {
    "error":   re.compile(r'\b(error|err|ERROR|ERR)\b'),
    "warning": re.compile(r'\b(warn|warning|WARN|WARNING)\b'),
    "fatal":   re.compile(r'\b(fatal|FATAL|panic|PANIC|critical|CRITICAL)\b'),
}


def _classify_line(line: str) -> dict:
    signals = [name for name, pattern in ERROR_PATTERNS.items() if pattern.search(line)]
    severity = "info"
    for sev, pattern in SEVERITY_KEYWORDS.items():
        if pattern.search(line):
            severity = sev
            break
    return {"signals": signals, "severity": severity}


def _load_k8s():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


async def extract_logs(
    namespace: str,
    service: str,
    since_seconds: int = 900,  # 15 minutes
    max_lines: int = 500,
) -> dict:
    """Extract and classify logs for all pods of a service."""
    loop = asyncio.get_event_loop()

    def _fetch():
        _load_k8s()
        v1 = client.CoreV1Api()
        pods = v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"app={service}",
        ).items

        pod_logs = {}
        for pod in pods:
            pod_name = pod.metadata.name
            container_name = pod.spec.containers[0].name
            try:
                raw = v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=namespace,
                    container=container_name,
                    since_seconds=since_seconds,
                    tail_lines=max_lines,
                )
            except client.exceptions.ApiException:
                raw = ""

            lines = raw.splitlines()
            classified = []
            signal_counts = {name: 0 for name in ERROR_PATTERNS}

            for line in lines:
                result = _classify_line(line)
                if result["signals"] or result["severity"] in ("error", "warning", "fatal"):
                    classified.append({
                        "line": line[:300],  # truncate long lines
                        "signals": result["signals"],
                        "severity": result["severity"],
                    })
                for sig in result["signals"]:
                    signal_counts[sig] += 1

            pod_logs[pod_name] = {
                "total_lines": len(lines),
                "classified_lines": classified[:100],  # top 100 notable lines
                "signal_counts": {k: v for k, v in signal_counts.items() if v > 0},
                "error_line_count": sum(1 for l in lines if _classify_line(l)["severity"] == "error"),
                "warning_line_count": sum(1 for l in lines if _classify_line(l)["severity"] == "warning"),
            }

        return pod_logs

    pod_logs = await loop.run_in_executor(None, _fetch)

    # Aggregate signals across all pods
    aggregate_signals = {}
    for pod_data in pod_logs.values():
        for sig, count in pod_data["signal_counts"].items():
            aggregate_signals[sig] = aggregate_signals.get(sig, 0) + count

    return {
        "service": service,
        "namespace": namespace,
        "since_seconds": since_seconds,
        "pod_logs": pod_logs,
        "aggregate_signals": aggregate_signals,
        "dominant_signal": max(aggregate_signals, key=aggregate_signals.get) if aggregate_signals else None,
    }
