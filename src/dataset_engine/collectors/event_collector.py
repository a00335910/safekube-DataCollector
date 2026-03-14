"""
Collects Kubernetes events for a service/namespace during a fault window.
Events are the richest signal source — they contain reason codes that
directly map to fault types (OOMKilling, BackOff, Failed, Evicted, etc.)
"""

import asyncio
from datetime import datetime, timezone, timedelta
from kubernetes import client, config

# Reason codes → fault type hints
REASON_TO_FAULT_HINT = {
    "OOMKilling":           "oomkilled",
    "OOMKilled":            "oomkilled",
    "BackOff":              "crashloop_backoff",
    "CrashLoopBackOff":     "crashloop_backoff",
    "Failed":               "pod_failure",
    "FailedMount":          "storage_mount_failure",
    "FailedAttachVolume":   "storage_mount_failure",
    "FailedScheduling":     "pod_pending",
    "Evicted":              "pod_eviction",
    "Killing":              "pod_kill",
    "Unhealthy":            "readiness_probe_fail",
    "ProbeWarning":         "liveness_probe_fail",
    "ImagePullBackOff":     "image_pull_backoff",
    "ErrImagePull":         "image_pull_backoff",
    "NodeNotReady":         "node_not_ready",
    "NodeNotSchedulable":   "node_not_ready",
    "Preempting":           "pod_eviction",
    "SystemOOM":            "oomkilled",
    "ContainerGCFailed":    "pod_failure",
    "NetworkNotReady":      "network_partition",
}


def _load_k8s():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def _event_age_seconds(event) -> float:
    """Return how many seconds ago the event last occurred."""
    last_time = event.last_timestamp or event.event_time
    if last_time is None:
        return float("inf")
    now = datetime.now(timezone.utc)
    if hasattr(last_time, "replace"):
        last_time = last_time.replace(tzinfo=timezone.utc)
    return (now - last_time).total_seconds()


async def collect_events(
    namespace: str,
    service: str,
    window_seconds: int = 900,
) -> dict:
    """Collect all K8s events for a service within the window."""
    loop = asyncio.get_event_loop()

    def _fetch():
        _load_k8s()
        v1 = client.CoreV1Api()
        all_events = v1.list_namespaced_event(namespace=namespace).items

        service_events = []
        for event in all_events:
            involved = event.involved_object
            # Match by service name prefix on pod/deployment/replicaset names
            name = (involved.name or "").lower()
            if not (name.startswith(service.lower()) or service.lower() in name):
                continue
            age = _event_age_seconds(event)
            if age > window_seconds:
                continue

            reason = event.reason or ""
            service_events.append({
                "reason": reason,
                "message": (event.message or "")[:500],
                "type": event.type,  # Normal / Warning
                "count": event.count or 1,
                "age_seconds": int(age),
                "involved_kind": involved.kind,
                "involved_name": involved.name,
                "fault_hint": REASON_TO_FAULT_HINT.get(reason),
            })

        return service_events

    events = await loop.run_in_executor(None, _fetch)

    # Aggregate by reason
    reason_counts = {}
    fault_hints = []
    warning_count = 0

    for ev in events:
        reason = ev["reason"]
        reason_counts[reason] = reason_counts.get(reason, 0) + ev["count"]
        if ev["fault_hint"] and ev["fault_hint"] not in fault_hints:
            fault_hints.append(ev["fault_hint"])
        if ev["type"] == "Warning":
            warning_count += ev["count"]

    return {
        "service": service,
        "namespace": namespace,
        "window_seconds": window_seconds,
        "events": events,
        "reason_counts": reason_counts,
        "fault_hints": fault_hints,
        "warning_event_count": warning_count,
        "total_event_count": len(events),
        "dominant_reason": max(reason_counts, key=reason_counts.get) if reason_counts else None,
    }
