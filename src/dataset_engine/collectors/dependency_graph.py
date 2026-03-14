"""
Captures service dependency impact during a fault.
Queries pod states for all Online Boutique services and identifies
which ones are degraded when a target service is faulted.
"""

import asyncio
from datetime import datetime, timezone
from kubernetes import client, config

# Online Boutique service dependency map
# key: service, value: list of services it depends on
SERVICE_DEPENDENCIES = {
    "frontend":             ["productcatalogservice", "currencyservice", "cartservice",
                             "recommendationservice", "shippingservice", "checkoutservice", "adservice"],
    "checkoutservice":      ["cartservice", "shippingservice", "currencyservice",
                             "emailservice", "paymentservice", "productcatalogservice"],
    "cartservice":          ["redis-cart"],
    "productcatalogservice": [],
    "currencyservice":      [],
    "shippingservice":      [],
    "emailservice":         [],
    "paymentservice":       [],
    "recommendationservice": ["productcatalogservice"],
    "adservice":            [],
    "redis-cart":           [],
}

# Services that depend on a given service (reverse map)
def _build_reverse_deps() -> dict:
    reverse = {svc: [] for svc in SERVICE_DEPENDENCIES}
    for svc, deps in SERVICE_DEPENDENCIES.items():
        for dep in deps:
            if dep in reverse:
                reverse[dep].append(svc)
    return reverse

REVERSE_DEPENDENCIES = _build_reverse_deps()


def _load_k8s():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


async def collect_dependency_impact(namespace: str, faulted_service: str) -> dict:
    """
    Snapshot the health of all services, with focus on those
    that depend on (or are depended on by) the faulted service.
    """
    loop = asyncio.get_event_loop()

    def _fetch():
        _load_k8s()
        v1 = client.CoreV1Api()
        apps_v1 = client.AppsV1Api()

        # Get all pods
        pods = v1.list_namespaced_pod(namespace=namespace).items
        # Get all deployments
        deployments = apps_v1.list_namespaced_deployment(namespace=namespace).items

        service_health = {}
        for dep in deployments:
            name = dep.metadata.name
            desired = dep.spec.replicas or 1
            ready = dep.status.ready_replicas or 0
            available = dep.status.available_replicas or 0
            service_health[name] = {
                "desired_replicas": desired,
                "ready_replicas": ready,
                "available_replicas": available,
                "degraded": ready < desired,
                "completely_down": ready == 0,
            }

        # Get pod-level details
        pod_states = {}
        for pod in pods:
            svc = pod.metadata.labels.get("app", "unknown")
            if svc not in pod_states:
                pod_states[svc] = []
            phase = pod.status.phase or "Unknown"
            conditions = {c.type: c.status for c in (pod.status.conditions or [])}
            container_statuses = []
            for cs in (pod.status.container_statuses or []):
                state_info = {"name": cs.name, "ready": cs.ready, "restart_count": cs.restart_count}
                if cs.state.waiting:
                    state_info["waiting_reason"] = cs.state.waiting.reason
                if cs.state.terminated:
                    state_info["terminated_reason"] = cs.state.terminated.reason
                    state_info["exit_code"] = cs.state.terminated.exit_code
                container_statuses.append(state_info)

            pod_states[svc].append({
                "pod_name": pod.metadata.name,
                "phase": phase,
                "ready": conditions.get("Ready") == "True",
                "containers": container_statuses,
            })

        return service_health, pod_states

    service_health, pod_states = await loop.run_in_executor(None, _fetch)

    # Identify impacted services
    direct_deps = SERVICE_DEPENDENCIES.get(faulted_service, [])
    upstream_services = REVERSE_DEPENDENCIES.get(faulted_service, [])

    impacted = {}
    for svc in direct_deps + upstream_services + [faulted_service]:
        health = service_health.get(svc, {})
        if health.get("degraded") or health.get("completely_down"):
            impacted[svc] = {
                "health": health,
                "relationship": "faulted" if svc == faulted_service
                               else "dependency" if svc in direct_deps
                               else "upstream_dependent",
            }

    return {
        "faulted_service": faulted_service,
        "namespace": namespace,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "all_service_health": service_health,
        "pod_states": pod_states,
        "direct_dependencies": direct_deps,
        "upstream_dependents": upstream_services,
        "impacted_services": impacted,
        "cascade_depth": len(impacted),
    }
