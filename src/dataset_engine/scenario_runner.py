"""
SafeKube Dataset Engine — Scenario Runner

Reads a fault scenario YAML, injects each fault via Chaos Mesh or K8s API,
collects labeled telemetry windows, and writes training-ready bundles.

Usage:
    python -m src.dataset_engine.scenario_runner
    python -m src.dataset_engine.scenario_runner --scenario scenarios/all_45_faults.yaml
    python -m src.dataset_engine.scenario_runner --fault oomkilled --service cartservice --runs 5
"""

import asyncio
import argparse
import csv
import uuid
import os
import sys
import yaml
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from kubernetes import client, config as k8s_config

from src.common.logger import get_logger
from src.dataset_engine.labeler import create_label, FAULT_METADATA
from src.dataset_engine.bundle_writer import write_training_row, CSV_PATH
from src.dataset_engine.collectors.metrics_window import collect_metrics_window
from src.dataset_engine.collectors.log_extractor import extract_logs
from src.dataset_engine.collectors.event_collector import collect_events
from src.dataset_engine.collectors.dependency_graph import collect_dependency_impact

logger = get_logger("dataset_engine.runner")

SCENARIOS_DIR = Path(__file__).parent / "scenarios"
DEFAULT_SCENARIO = SCENARIOS_DIR / "all_45_faults.yaml"

CHAOS_MESH_GROUP = "chaos-mesh.org"
CHAOS_MESH_VERSION = "v1alpha1"


def _load_k8s():
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()


def _check_fuse() -> bool:
    return os.path.exists("/dev/fuse")


def _load_completed_runs(csv_path: Path) -> set:
    """Return a set of (fault_type, service) tuples that already have a full run_idx=2 (test) row.

    A (fault_type, service) combo is considered fully done when the test-split row
    exists, meaning all 3 runs completed (run_idx 0, 1, 2).
    """
    done = set()
    if not csv_path.exists():
        return done
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ft = row.get("fault_type", "")
                svc = row.get("target_service", "")
                split = row.get("split", "")
                if ft and svc and split == "test":
                    done.add((ft, svc))
    except Exception:
        pass
    return done


# ─── Chaos Mesh experiment builders ───────────────────────────────────────────

def _build_podchaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    merged_spec = dict(spec)
    # container-kill requires at least one container name; all Online Boutique services use "server"
    if merged_spec.get("action") == "container-kill" and not merged_spec.get("containerNames"):
        merged_spec["containerNames"] = ["server"]
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "PodChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **merged_spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_stresschaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "StressChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_networkchaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "NetworkChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_dnschaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "DNSChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_httpchaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "HTTPChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_timechaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "TimeChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


def _build_iochaos(name: str, namespace: str, service: str, spec: dict, duration: str) -> dict:
    return {
        "apiVersion": f"{CHAOS_MESH_GROUP}/{CHAOS_MESH_VERSION}",
        "kind": "IOChaos",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            **spec,
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {"app": service},
            },
            "duration": duration,
        },
    }


CHAOS_BUILDERS = {
    "PodChaos":     _build_podchaos,
    "StressChaos":  _build_stresschaos,
    "NetworkChaos": _build_networkchaos,
    "DNSChaos":     _build_dnschaos,
    "HTTPChaos":    _build_httpchaos,
    "TimeChaos":    _build_timechaos,
    "IOChaos":      _build_iochaos,
}

KIND_TO_PLURAL = {
    "PodChaos":     "podchaos",
    "StressChaos":  "stresschaos",
    "NetworkChaos": "networkchaos",
    "DNSChaos":     "dnschaos",
    "HTTPChaos":    "httpchaos",
    "TimeChaos":    "timechaos",
    "IOChaos":      "iochaos",
}


# ─── K8s API fault injectors ──────────────────────────────────────────────────

class K8sApiFaultInjector:
    """Handles fault types that use K8s API directly instead of Chaos Mesh."""

    def __init__(self, namespace: str):
        _load_k8s()
        self.namespace = namespace
        self.v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()
        self._cleanup_stack = []  # (fn, args) to call on cleanup

    def inject(self, fault_type: str, service: str, spec: dict):
        action = spec.get("action")
        if action == "patch_resource_quota":
            self._patch_resource_quota(spec)
        elif action == "apply_network_policy":
            self._apply_network_policy(service, spec)
        elif action == "delete_secret":
            self._inject_missing_secret(service)
        elif action == "delete_configmap":
            self._inject_missing_configmap(service)
        elif action == "patch_hpa_max_replicas":
            self._patch_hpa(service, spec)
        elif action == "patch_env_var":
            self._patch_env_var(service, spec)
        elif action == "cordon_node":
            self._cordon_node()
        else:
            logger.warning(f"Unknown k8s_api action: {action}", extra={"incident_id": None})

    def cleanup(self):
        for fn, args in reversed(self._cleanup_stack):
            try:
                fn(*args)
            except Exception as e:
                logger.error(f"Cleanup error: {e}", extra={"incident_id": None})
        self._cleanup_stack.clear()

    def _patch_resource_quota(self, spec: dict):
        quota_name = f"safekube-dataset-quota"
        quota = {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": quota_name, "namespace": self.namespace},
            "spec": {"hard": {
                "requests.cpu": spec.get("cpu_limit", "10m"),
                "requests.memory": spec.get("memory_limit", "1Mi"),
            }},
        }
        try:
            self.v1.create_namespaced_resource_quota(self.namespace, quota)
        except client.exceptions.ApiException as e:
            if e.status == 409:
                self.v1.replace_namespaced_resource_quota(quota_name, self.namespace, quota)
        self._cleanup_stack.append((
            self.v1.delete_namespaced_resource_quota,
            [quota_name, self.namespace]
        ))

    def _apply_network_policy(self, service: str, spec: dict):
        policy_name = f"safekube-dataset-deny-{service}"
        policy = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": policy_name, "namespace": self.namespace},
            "spec": {
                "podSelector": {"matchLabels": {"app": service}},
                "policyTypes": ["Ingress"],
                "ingress": [],
            },
        }
        networking = client.NetworkingV1Api()
        try:
            networking.create_namespaced_network_policy(self.namespace, policy)
        except client.exceptions.ApiException as e:
            if e.status == 409:
                networking.replace_namespaced_network_policy(policy_name, self.namespace, policy)
        self._cleanup_stack.append((
            networking.delete_namespaced_network_policy,
            [policy_name, self.namespace]
        ))

    def _inject_missing_secret(self, service: str):
        secret_name = f"safekube-dataset-secret-{service}"
        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=self.namespace),
            string_data={"key": "value"},
        )
        try:
            self.v1.create_namespaced_secret(self.namespace, secret)
        except client.exceptions.ApiException:
            pass
        # Patch deployment to reference this secret, then delete the secret
        self.v1.delete_namespaced_secret(secret_name, self.namespace)

    def _inject_missing_configmap(self, service: str):
        cm_name = f"safekube-dataset-cm-{service}"
        cm = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name, namespace=self.namespace),
            data={"key": "value"},
        )
        try:
            self.v1.create_namespaced_config_map(self.namespace, cm)
        except client.exceptions.ApiException:
            pass
        self.v1.delete_namespaced_config_map(cm_name, self.namespace)

    def _patch_hpa(self, service: str, spec: dict):
        try:
            hpa_api = client.AutoscalingV1Api()
            hpa = hpa_api.read_namespaced_horizontal_pod_autoscaler(service, self.namespace)
            original_max = hpa.spec.max_replicas
            hpa.spec.max_replicas = spec.get("max_replicas", 1)
            hpa_api.replace_namespaced_horizontal_pod_autoscaler(service, self.namespace, hpa)
            self._cleanup_stack.append((
                lambda orig: hpa_api.patch_namespaced_horizontal_pod_autoscaler(
                    service, self.namespace, {"spec": {"maxReplicas": orig}}
                ),
                [original_max]
            ))
        except client.exceptions.ApiException:
            pass

    def _patch_env_var(self, service: str, spec: dict):
        try:
            dep = self.apps_v1.read_namespaced_deployment(service, self.namespace)
            container = dep.spec.template.spec.containers[0]
            var_name = spec.get("var_name")
            bad_value = spec.get("bad_value")
            original_value = None
            for env in (container.env or []):
                if env.name == var_name:
                    original_value = env.value
                    env.value = bad_value
                    break
            self.apps_v1.replace_namespaced_deployment(service, self.namespace, dep)
            if original_value is not None:
                self._cleanup_stack.append((
                    lambda orig: self.apps_v1.patch_namespaced_deployment(
                        service, self.namespace,
                        {"spec": {"template": {"spec": {"containers": [
                            {"name": container.name, "env": [{"name": var_name, "value": orig}]}
                        ]}}}}
                    ),
                    [original_value]
                ))
        except client.exceptions.ApiException:
            pass

    def _cordon_node(self):
        core = client.CoreV1Api()
        nodes = core.list_node().items
        if not nodes:
            return
        node = nodes[-1]  # cordon last node (least disruptive)
        node_name = node.metadata.name
        core.patch_node(node_name, {"spec": {"unschedulable": True}})
        self._cleanup_stack.append((
            lambda n: core.patch_node(n, {"spec": {"unschedulable": False}}),
            [node_name]
        ))


# ─── Duration parser ─────────────────────────────────────────────────────────

def _parse_duration_seconds(duration_str: str) -> int:
    s = duration_str.strip()
    if s.endswith("s"):
        return int(s[:-1])
    elif s.endswith("m"):
        return int(s[:-1]) * 60
    elif s.endswith("h"):
        return int(s[:-1]) * 3600
    return int(s)


# ─── Main runner ─────────────────────────────────────────────────────────────

class ScenarioRunner:

    def __init__(self, scenario_path: Path, namespace: str = "staging"):
        self.scenario_path = scenario_path
        self.namespace = namespace
        _load_k8s()
        self.custom_api = client.CustomObjectsApi()

    def _load_scenario(self) -> dict:
        with open(self.scenario_path) as f:
            return yaml.safe_load(f)

    async def _inject_chaos(self, experiment_name: str, chaos_kind: str, manifest: dict):
        loop = asyncio.get_event_loop()
        plural = KIND_TO_PLURAL[chaos_kind]

        def _create():
            try:
                self.custom_api.create_namespaced_custom_object(
                    group=CHAOS_MESH_GROUP,
                    version=CHAOS_MESH_VERSION,
                    namespace=self.namespace,
                    plural=plural,
                    body=manifest,
                )
            except client.exceptions.ApiException as e:
                if e.status == 409:  # already exists
                    self.custom_api.replace_namespaced_custom_object(
                        group=CHAOS_MESH_GROUP,
                        version=CHAOS_MESH_VERSION,
                        namespace=self.namespace,
                        plural=plural,
                        name=experiment_name,
                        body=manifest,
                    )
                else:
                    raise

        await loop.run_in_executor(None, _create)

    async def _delete_chaos(self, experiment_name: str, chaos_kind: str):
        loop = asyncio.get_event_loop()
        plural = KIND_TO_PLURAL[chaos_kind]

        def _delete():
            try:
                self.custom_api.delete_namespaced_custom_object(
                    group=CHAOS_MESH_GROUP,
                    version=CHAOS_MESH_VERSION,
                    namespace=self.namespace,
                    plural=plural,
                    name=experiment_name,
                )
            except client.exceptions.ApiException:
                pass

        await loop.run_in_executor(None, _delete)

    async def _wait_cluster_healthy(self, namespace: str, timeout: int = 120):
        """
        Block until all deployments in namespace have desired == ready replicas.
        Prevents residual effects from one fault contaminating the next window.
        """
        loop = asyncio.get_event_loop()
        deadline = asyncio.get_event_loop().time() + timeout
        apps_v1 = client.AppsV1Api()

        while asyncio.get_event_loop().time() < deadline:
            def _check():
                deps = apps_v1.list_namespaced_deployment(namespace).items
                return all(
                    (d.status.ready_replicas or 0) >= (d.spec.replicas or 1)
                    for d in deps
                )
            healthy = await loop.run_in_executor(None, _check)
            if healthy:
                return True
            await asyncio.sleep(10)

        logger.warning("Cluster did not fully recover within timeout — proceeding anyway",
                       extra={"incident_id": None})
        return False

    async def _collect_pre_fault_baseline(self, service: str) -> dict:
        """Snapshot metrics/events before fault injection for delta computation."""
        metrics, events = await asyncio.gather(
            collect_metrics_window(self.namespace, service, window_minutes=5),
            collect_events(self.namespace, service, window_seconds=300),
        )
        return {"metrics": metrics, "events": events, "captured_at": datetime.now(timezone.utc).isoformat()}

    async def run_single(
        self,
        scenario: dict,
        service: str,
        run_index: int,
        collect_window_minutes: int,
        split: str = "train",
    ) -> bool:
        fault_type = scenario["fault_type"]
        chaos_kind = scenario["chaos_kind"]
        spec = scenario.get("spec", {})
        duration_str = scenario.get("duration", "60s")
        duration_seconds = _parse_duration_seconds(duration_str)
        run_id = f"{fault_type}__{service}__{run_index}__{uuid.uuid4().hex[:6]}"
        experiment_name = f"sk-{fault_type.replace('_', '-')}-{uuid.uuid4().hex[:6]}"

        logger.info(
            f"[{run_id}] Injecting {fault_type} on {service} for {duration_str} (split={split})",
            extra={"incident_id": None},
        )

        injection_time = datetime.now(timezone.utc)
        k8s_injector = None

        try:
            # ── Pre-fault baseline ────────────────────────────────────────────
            pre_fault = await self._collect_pre_fault_baseline(service)

            # ── Inject fault ──────────────────────────────────────────────────
            if chaos_kind == "k8s_api":
                k8s_injector = K8sApiFaultInjector(self.namespace)
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, lambda: k8s_injector.inject(fault_type, service, spec)
                )
            else:
                builder = CHAOS_BUILDERS.get(chaos_kind)
                if builder is None:
                    logger.warning(f"No builder for {chaos_kind}", extra={"incident_id": None})
                    return False
                manifest = builder(experiment_name, self.namespace, service, spec, duration_str)
                await self._inject_chaos(experiment_name, chaos_kind, manifest)

            # ── Wait for full fault duration + 30s buffer before collecting ───
            await asyncio.sleep(duration_seconds + 30)

            # ── Collect telemetry ─────────────────────────────────────────────
            # Window covers 2 min before injection → 2 min after fault ends
            fault_end = injection_time + timedelta(seconds=duration_seconds)
            collection_window_start = injection_time - timedelta(minutes=2)
            window_minutes = int((fault_end - collection_window_start).total_seconds() / 60) + 2

            metrics, logs, events, deps = await asyncio.gather(
                collect_metrics_window(self.namespace, service, collection_window_start, window_minutes),
                extract_logs(self.namespace, service, since_seconds=duration_seconds + 120),
                collect_events(self.namespace, service, window_seconds=duration_seconds + 120),
                collect_dependency_impact(self.namespace, service),
            )

            # Attach pre-fault baseline to metrics for LLM delta reasoning
            metrics["pre_fault_baseline"] = pre_fault

            # ── Label ─────────────────────────────────────────────────────────
            label = create_label(
                fault_type=fault_type,
                target_service=service,
                target_namespace=self.namespace,
                injection_time=injection_time,
                duration_seconds=duration_seconds,
                experiment_name=experiment_name,
            )

            # ── Write bundle ──────────────────────────────────────────────────
            bundle_path, csv_path = await write_training_row(
                label, metrics, logs, events, deps, run_id, split=split
            )
            logger.info(
                f"[{run_id}] Bundle written: {bundle_path.name}",
                extra={"incident_id": None},
            )
            return True

        except Exception as e:
            logger.error(f"[{run_id}] Failed: {e}", extra={"incident_id": None})
            return False

        finally:
            # ── Cleanup ───────────────────────────────────────────────────────
            if k8s_injector:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, k8s_injector.cleanup)
            elif chaos_kind != "k8s_api":
                await self._delete_chaos(experiment_name, chaos_kind)

    async def run_concurrent(
        self,
        scenarios: list[dict],
        service: str,
        run_index: int,
        collect_window_minutes: int,
        split: str = "train",
    ) -> bool:
        """
        Inject two faults simultaneously on the same service.
        Used for conflict detection training — e.g. oomkilled + dns_failure at once.
        Combined fault_type is stored as "fault_a__fault_b".
        """
        if len(scenarios) != 2:
            raise ValueError("run_concurrent requires exactly 2 scenarios")

        s1, s2 = scenarios
        combined_fault_type = f"{s1['fault_type']}__concurrent__{s2['fault_type']}"
        run_id = f"{combined_fault_type}__{service}__{run_index}__{uuid.uuid4().hex[:6]}"
        exp1 = f"sk-{s1['fault_type'].replace('_', '-')[:12]}-{uuid.uuid4().hex[:4]}"
        exp2 = f"sk-{s2['fault_type'].replace('_', '-')[:12]}-{uuid.uuid4().hex[:4]}"

        logger.info(f"[{run_id}] Concurrent injection: {s1['fault_type']} + {s2['fault_type']} on {service}",
                    extra={"incident_id": None})

        injection_time = datetime.now(timezone.utc)
        duration_seconds = max(
            _parse_duration_seconds(s1.get("duration", "60s")),
            _parse_duration_seconds(s2.get("duration", "60s")),
        )

        try:
            pre_fault = await self._collect_pre_fault_baseline(service)

            # Inject both simultaneously
            inject_tasks = []
            for scenario, exp_name in [(s1, exp1), (s2, exp2)]:
                chaos_kind = scenario["chaos_kind"]
                if chaos_kind != "k8s_api":
                    builder = CHAOS_BUILDERS.get(chaos_kind)
                    if builder:
                        manifest = builder(exp_name, self.namespace, service,
                                           scenario.get("spec", {}), scenario.get("duration", "60s"))
                        inject_tasks.append(self._inject_chaos(exp_name, chaos_kind, manifest))

            await asyncio.gather(*inject_tasks)
            await asyncio.sleep(min(duration_seconds // 2, 30))

            metrics, logs, events, deps = await asyncio.gather(
                collect_metrics_window(self.namespace, service, injection_time, collect_window_minutes),
                extract_logs(self.namespace, service, since_seconds=duration_seconds + 60),
                collect_events(self.namespace, service, window_seconds=duration_seconds + 60),
                collect_dependency_impact(self.namespace, service),
            )
            metrics["pre_fault_baseline"] = pre_fault
            metrics["concurrent_faults"] = [s1["fault_type"], s2["fault_type"]]

            label = create_label(
                fault_type=combined_fault_type,
                target_service=service,
                target_namespace=self.namespace,
                injection_time=injection_time,
                duration_seconds=duration_seconds,
                experiment_name=f"{exp1}__{exp2}",
            )

            bundle_path, _ = await write_training_row(
                label, metrics, logs, events, deps, run_id, split=split
            )
            logger.info(f"[{run_id}] Concurrent bundle written: {bundle_path.name}",
                        extra={"incident_id": None})
            return True

        except Exception as e:
            logger.error(f"[{run_id}] Concurrent run failed: {e}", extra={"incident_id": None})
            return False

        finally:
            for exp_name, scenario in [(exp1, s1), (exp2, s2)]:
                if scenario["chaos_kind"] != "k8s_api":
                    await self._delete_chaos(exp_name, scenario["chaos_kind"])

    async def run_all(
        self,
        fault_filter: str = None,
        service_filter: str = None,
        runs_override: int = None,
        concurrent_pairs: list[tuple[str, str]] = None,
    ):
        """
        Main loop. For each scenario × service × run:
          1. Wait for cluster to be healthy (no residual effects)
          2. Capture pre-fault baseline
          3. Inject fault
          4. Collect labeled telemetry
          5. Write bundle + CSV row with train/test split
          6. Cleanup + health check before next run

        Split strategy:
          - Last run_index of each (fault_type, service) pair → "test"
          - All others → "train"
          This ensures test data comes from different time windows than train,
          preventing data leakage while keeping same fault/service coverage.

        concurrent_pairs: list of (fault_type_a, fault_type_b) to inject simultaneously.
        """
        data = self._load_scenario()
        meta = data.get("metadata", {})
        collect_window = meta.get("collect_window_minutes", 15)
        cooldown = meta.get("cooldown_seconds", 60)
        default_runs = runs_override or meta.get("runs_per_fault", 3)
        target_services = (
            [service_filter] if service_filter
            else meta.get("target_services", ["cartservice"])
        )
        has_fuse = _check_fuse()

        scenarios = data.get("scenarios", [])
        if fault_filter:
            scenarios = [s for s in scenarios if s["fault_type"] == fault_filter]

        total = len(scenarios) * len(target_services) * default_runs
        completed = 0
        failed = 0

        # Resume: load already-completed (fault_type, service) pairs from CSV
        completed_runs = _load_completed_runs(CSV_PATH)
        if completed_runs:
            logger.info(
                f"Resuming — {len(completed_runs)} (fault, service) combos already done, skipping them.",
                extra={"incident_id": None},
            )

        logger.info(
            f"Starting dataset generation: {len(scenarios)} faults × "
            f"{len(target_services)} services × {default_runs} runs = {total} incidents",
            extra={"incident_id": None},
        )

        # Build scenario lookup for concurrent pairs
        scenario_by_fault = {s["fault_type"]: s for s in scenarios}

        for scenario in scenarios:
            if scenario.get("requires_fuse") and not has_fuse:
                logger.info(
                    f"Skipping {scenario['fault_type']} — no /dev/fuse on host",
                    extra={"incident_id": None},
                )
                continue

            for service in target_services:
                if (scenario["fault_type"], service) in completed_runs:
                    logger.info(
                        f"Skipping [{scenario['fault_type']} / {service}] — already completed.",
                        extra={"incident_id": None},
                    )
                    completed += default_runs
                    continue

                for run_idx in range(default_runs):
                    # Last run per (fault, service) pair goes to test split
                    split = "test" if run_idx == default_runs - 1 else "train"

                    # Wait for cluster to fully recover before injecting
                    logger.info(
                        f"Checking cluster health before run {run_idx + 1}/{default_runs} "
                        f"[{scenario['fault_type']} / {service}]",
                        extra={"incident_id": None},
                    )
                    await self._wait_cluster_healthy(self.namespace)

                    success = await self.run_single(
                        scenario, service, run_idx, collect_window, split=split
                    )
                    if success:
                        completed += 1
                    else:
                        failed += 1

                    if cooldown > 0:
                        logger.info(
                            f"Cooldown {cooldown}s "
                            f"({completed}/{total} done, {failed} failed)",
                            extra={"incident_id": None},
                        )
                        await asyncio.sleep(cooldown)

        # ── Concurrent fault pairs ─────────────────────────────────────────────
        if concurrent_pairs:
            logger.info(
                f"Running {len(concurrent_pairs)} concurrent fault pairs",
                extra={"incident_id": None},
            )
            for fault_a, fault_b in concurrent_pairs:
                s_a = scenario_by_fault.get(fault_a)
                s_b = scenario_by_fault.get(fault_b)
                if not s_a or not s_b:
                    logger.warning(
                        f"Concurrent pair skipped — unknown fault: {fault_a} or {fault_b}",
                        extra={"incident_id": None},
                    )
                    continue
                for service in target_services:
                    await self._wait_cluster_healthy(self.namespace)
                    success = await self.run_concurrent(
                        [s_a, s_b], service, 0, collect_window, split="train"
                    )
                    completed += int(success)
                    failed += int(not success)
                    await asyncio.sleep(cooldown)

        logger.info(
            f"Dataset generation complete. "
            f"Completed: {completed}, Failed: {failed}, Total attempted: {completed + failed}",
            extra={"incident_id": None},
        )
        return {"completed": completed, "failed": failed, "total": completed + failed}


# ─── CLI entry point ──────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="SafeKube Dataset Engine — Scenario Runner")
    parser.add_argument("--scenario", default=str(DEFAULT_SCENARIO), help="Path to scenario YAML")
    parser.add_argument("--fault", help="Run only this fault type")
    parser.add_argument("--service", help="Run only on this service")
    parser.add_argument("--runs", type=int, help="Override runs per fault")
    parser.add_argument("--namespace", default="staging")
    args = parser.parse_args()

    runner = ScenarioRunner(Path(args.scenario), namespace=args.namespace)
    result = await runner.run_all(
        fault_filter=args.fault,
        service_filter=args.service,
        runs_override=args.runs,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
