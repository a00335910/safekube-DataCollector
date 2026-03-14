"""
Auto-labeler: attaches ground-truth fault labels to collected data.
When Chaos Mesh injects a fault, we know exactly what it is — no manual labeling needed.
Also extracts feature vectors compatible with the SetFit classifier input format.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class FaultLabel:
    fault_type: str               # e.g. "oomkilled", "network_latency_high"
    fault_category: str           # pod / resource / network / http / time / k8s_api / io
    target_service: str
    target_namespace: str
    injection_time: datetime
    duration_seconds: int
    chaos_experiment_name: str
    chaos_kind: str               # PodChaos, StressChaos, etc.
    severity: str                 # low / medium / high / critical
    expected_signals: list[str] = field(default_factory=list)
    confidence: float = 1.0       # always 1.0 for injected faults


# Maps fault_type → metadata for auto-labeling
FAULT_METADATA = {
    # PodChaos
    "pod_kill":                 {"category": "pod",      "severity": "high",     "chaos_kind": "PodChaos",    "signals": ["crash_exit", "connection_refused"]},
    "pod_failure":              {"category": "pod",      "severity": "high",     "chaos_kind": "PodChaos",    "signals": ["crash_exit"]},
    "container_kill":           {"category": "pod",      "severity": "medium",   "chaos_kind": "PodChaos",    "signals": ["crash_exit", "connection_refused"]},
    "oomkilled":                {"category": "pod",      "severity": "critical", "chaos_kind": "StressChaos", "signals": ["oom_killed", "crash_exit"]},
    "crashloop_backoff":        {"category": "pod",      "severity": "high",     "chaos_kind": "PodChaos",    "signals": ["crash_exit", "connection_refused"]},
    "image_pull_backoff":       {"category": "pod",      "severity": "medium",   "chaos_kind": "PodChaos",    "signals": ["image_pull"]},
    "readiness_probe_fail":     {"category": "pod",      "severity": "medium",   "chaos_kind": "PodChaos",    "signals": ["readiness_failed"]},
    "liveness_probe_fail":      {"category": "pod",      "severity": "medium",   "chaos_kind": "PodChaos",    "signals": ["liveness_failed"]},
    "pod_pending":              {"category": "pod",      "severity": "medium",   "chaos_kind": "k8s_api",     "signals": []},
    "pod_eviction":             {"category": "pod",      "severity": "high",     "chaos_kind": "PodChaos",    "signals": ["crash_exit"]},
    # StressChaos
    "cpu_throttling":           {"category": "resource", "severity": "medium",   "chaos_kind": "StressChaos", "signals": ["timeout", "grpc_error"]},
    "cpu_spike":                {"category": "resource", "severity": "medium",   "chaos_kind": "StressChaos", "signals": ["timeout"]},
    "memory_pressure":          {"category": "resource", "severity": "high",     "chaos_kind": "StressChaos", "signals": ["resource_exhausted"]},
    "memory_leak_simulation":   {"category": "resource", "severity": "high",     "chaos_kind": "StressChaos", "signals": ["resource_exhausted", "oom_killed"]},
    "memory_oom_boundary":      {"category": "resource", "severity": "critical", "chaos_kind": "StressChaos", "signals": ["oom_killed"]},
    "cpu_multi_worker_stress":  {"category": "resource", "severity": "high",     "chaos_kind": "StressChaos", "signals": ["timeout", "thread_exhaustion"]},
    "combined_cpu_memory_stress": {"category": "resource", "severity": "critical", "chaos_kind": "StressChaos", "signals": ["timeout", "resource_exhausted"]},
    "worker_thread_exhaustion": {"category": "resource", "severity": "high",     "chaos_kind": "StressChaos", "signals": ["thread_exhaustion", "timeout"]},
    # NetworkChaos
    "dns_failure":              {"category": "network",  "severity": "high",     "chaos_kind": "DNSChaos",    "signals": ["dns_error"]},
    "dns_random_response":      {"category": "network",  "severity": "high",     "chaos_kind": "DNSChaos",    "signals": ["dns_error", "connection_refused"]},
    "network_latency_low":      {"category": "network",  "severity": "low",      "chaos_kind": "NetworkChaos","signals": ["timeout"]},
    "network_latency_high":     {"category": "network",  "severity": "high",     "chaos_kind": "NetworkChaos","signals": ["timeout", "grpc_error"]},
    "network_latency_jitter":   {"category": "network",  "severity": "medium",   "chaos_kind": "NetworkChaos","signals": ["timeout"]},
    "network_packet_loss_low":  {"category": "network",  "severity": "low",      "chaos_kind": "NetworkChaos","signals": ["network_error"]},
    "network_packet_loss_high": {"category": "network",  "severity": "high",     "chaos_kind": "NetworkChaos","signals": ["network_error", "timeout"]},
    "network_partition":        {"category": "network",  "severity": "critical", "chaos_kind": "NetworkChaos","signals": ["connection_refused", "network_error"]},
    "network_bandwidth_limit":  {"category": "network",  "severity": "medium",   "chaos_kind": "NetworkChaos","signals": ["timeout", "network_error"]},
    "network_packet_corrupt":   {"category": "network",  "severity": "medium",   "chaos_kind": "NetworkChaos","signals": ["network_error", "grpc_error"]},
    # HTTPChaos
    "http_abort":               {"category": "http",     "severity": "high",     "chaos_kind": "HTTPChaos",   "signals": ["connection_refused", "http_5xx"]},
    "http_delay":               {"category": "http",     "severity": "medium",   "chaos_kind": "HTTPChaos",   "signals": ["timeout"]},
    "http_error_500":           {"category": "http",     "severity": "high",     "chaos_kind": "HTTPChaos",   "signals": ["http_5xx"]},
    "http_error_503":           {"category": "http",     "severity": "high",     "chaos_kind": "HTTPChaos",   "signals": ["http_5xx"]},
    "http_body_corrupt":        {"category": "http",     "severity": "medium",   "chaos_kind": "HTTPChaos",   "signals": ["grpc_error"]},
    "http_header_corrupt":      {"category": "http",     "severity": "medium",   "chaos_kind": "HTTPChaos",   "signals": ["grpc_error", "permission_denied"]},
    # TimeChaos
    "clock_skew_forward":       {"category": "time",     "severity": "medium",   "chaos_kind": "TimeChaos",   "signals": ["clock_skew", "permission_denied"]},
    "clock_skew_backward":      {"category": "time",     "severity": "medium",   "chaos_kind": "TimeChaos",   "signals": ["clock_skew"]},
    "clock_skew_large":         {"category": "time",     "severity": "high",     "chaos_kind": "TimeChaos",   "signals": ["clock_skew", "permission_denied"]},
    "clock_skew_gradual":       {"category": "time",     "severity": "low",      "chaos_kind": "TimeChaos",   "signals": ["clock_skew"]},
    # K8s API faults
    "resource_quota_exceeded":  {"category": "k8s_api",  "severity": "high",     "chaos_kind": "k8s_api",     "signals": ["resource_exhausted"]},
    "network_policy_block":     {"category": "k8s_api",  "severity": "high",     "chaos_kind": "k8s_api",     "signals": ["connection_refused", "permission_denied"]},
    "missing_secret":           {"category": "k8s_api",  "severity": "high",     "chaos_kind": "k8s_api",     "signals": ["missing_resource", "crash_exit"]},
    "missing_configmap":        {"category": "k8s_api",  "severity": "medium",   "chaos_kind": "k8s_api",     "signals": ["missing_resource"]},
    "hpa_max_capacity":         {"category": "k8s_api",  "severity": "medium",   "chaos_kind": "k8s_api",     "signals": ["resource_exhausted", "timeout"]},
    "misconfigured_env":        {"category": "k8s_api",  "severity": "medium",   "chaos_kind": "k8s_api",     "signals": ["crash_exit", "missing_resource"]},
    "node_not_ready":           {"category": "k8s_api",  "severity": "critical", "chaos_kind": "k8s_api",     "signals": ["connection_refused"]},
    # IOChaos (conditional on host FUSE)
    "disk_io_latency":          {"category": "io",       "severity": "medium",   "chaos_kind": "IOChaos",     "signals": ["disk_error", "timeout"]},
    "disk_io_error":            {"category": "io",       "severity": "high",     "chaos_kind": "IOChaos",     "signals": ["disk_error"]},
    "disk_io_bandwidth_limit":  {"category": "io",       "severity": "medium",   "chaos_kind": "IOChaos",     "signals": ["disk_error", "timeout"]},
}


def create_label(
    fault_type: str,
    target_service: str,
    target_namespace: str,
    injection_time: datetime,
    duration_seconds: int,
    experiment_name: str,
) -> FaultLabel:
    meta = FAULT_METADATA.get(fault_type, {
        "category": "unknown",
        "severity": "medium",
        "chaos_kind": "unknown",
        "signals": [],
    })
    return FaultLabel(
        fault_type=fault_type,
        fault_category=meta["category"],
        target_service=target_service,
        target_namespace=target_namespace,
        injection_time=injection_time,
        duration_seconds=duration_seconds,
        chaos_experiment_name=experiment_name,
        chaos_kind=meta["chaos_kind"],
        severity=meta["severity"],
        expected_signals=meta["signals"],
        confidence=1.0,
    )


def to_setfit_text(label: FaultLabel, events: dict, logs: dict, metrics: dict) -> str:
    """
    Convert a labeled incident into rich, distinguishable text for SetFit training.
    Only uses observable signals — no label-derived fields (no fault_category, no severity).
    """
    # ── Events ────────────────────────────────────────────────────────────────
    event_reasons = list((events.get("reason_counts") or {}).keys())[:5]
    dominant_event = events.get("dominant_reason") or "none"

    # ── Log signals ───────────────────────────────────────────────────────────
    log_signals = list((logs.get("aggregate_signals") or {}).keys())[:5]
    dominant_log = logs.get("dominant_signal") or "none"

    # Top 3 actual error log lines (raw text, not pattern names)
    error_lines = []
    for pod_data in logs.get("pod_logs", {}).values():
        for line in pod_data.get("classified_lines", [])[:3]:
            text = line.get("line", "").strip()
            if text:
                error_lines.append(f'"{text[:80]}"')
        if len(error_lines) >= 3:
            break

    # ── Metrics (all pods, not just first) ───────────────────────────────────
    pod_metrics = metrics.get("pod_metrics", {})
    mem_parts, cpu_parts, restart_parts, net_parts = [], [], [], []

    for pod, m in pod_metrics.items():
        pod_short = pod.split("-")[-1]  # last suffix for brevity

        if "memory_working_set_mib" in m and "memory_limit_mib" in m:
            usage = m["memory_working_set_mib"]
            limit = m["memory_limit_mib"]
            usage_last = usage.get("last", 0)
            limit_last = limit.get("last", 0) or 1
            pct = (usage_last / limit_last) * 100
            trend = usage.get("trend", 0)
            mem_parts.append(f"{pod_short}:{usage_last:.0f}/{limit_last:.0f}MiB({pct:.0f}%)trend:{trend:+.0f}")
        elif "memory_working_set_mib" in m:
            usage = m["memory_working_set_mib"]
            mem_parts.append(f"{pod_short}:{usage.get('last', 0):.0f}MiB trend:{usage.get('trend', 0):+.0f}")

        if "cpu_usage_millicores" in m:
            cpu = m["cpu_usage_millicores"]
            throttle = m.get("cpu_throttled_ratio", {}).get("last", 0) or 0
            cpu_str = f"{pod_short}:{cpu.get('last', 0):.0f}/{cpu.get('max', 0):.0f}m"
            if throttle > 0.05:
                cpu_str += f" throttled:{throttle:.2f}"
            cpu_parts.append(cpu_str)

        if "restart_count" in m:
            restarts = m["restart_count"].get("last", 0)
            delta = m.get("restart_rate_10m", {}).get("last", 0) or 0
            if restarts > 0 or delta > 0:
                restart_parts.append(f"{pod_short}:{restarts:.0f}(+{delta:.0f}/10m)")

        rx_err = m.get("network_rx_errors", {}).get("last", 0) or 0
        tx_err = m.get("network_tx_errors", {}).get("last", 0) or 0
        if rx_err > 0 or tx_err > 0:
            net_parts.append(f"{pod_short}:rx_err:{rx_err:.2f} tx_err:{tx_err:.2f}")

    # ── Instant state ─────────────────────────────────────────────────────────
    instant = metrics.get("instant_state", {})

    oom_count = len(instant.get("oom_events", []))

    node_pressure = []
    for cond, key in [("MemoryPressure", "node_memory_pressure"),
                      ("DiskPressure", "node_disk_pressure"),
                      ("PIDPressure", "node_cpu_pressure")]:
        if instant.get(key):
            node_pressure.append(cond)

    pod_phases = set()
    for r in instant.get("pod_phase", []):
        phase = r.get("metric", {}).get("phase", "")
        if phase and phase != "Running":
            pod_phases.add(phase)

    # ── Assemble text ─────────────────────────────────────────────────────────
    parts = [
        f"events: {', '.join(event_reasons) or 'none'}",
        f"dominant_event: {dominant_event}",
        f"log_signals: {', '.join(log_signals) or 'none'}",
        f"dominant_log: {dominant_log}",
    ]

    if error_lines:
        parts.append(f"log_errors: {' | '.join(error_lines)}")

    parts.append(f"mem: {', '.join(mem_parts) or 'none'}")
    parts.append(f"cpu: {', '.join(cpu_parts) or 'none'}")
    parts.append(f"restarts: {', '.join(restart_parts) or '0'}")

    if net_parts:
        parts.append(f"net_errors: {', '.join(net_parts)}")

    parts.append(f"oom_events: {oom_count}")
    parts.append(f"node_pressure: {', '.join(node_pressure) or 'none'}")

    if pod_phases:
        parts.append(f"pod_phase: {', '.join(pod_phases)}")

    return " | ".join(parts)
