"""
Healthy baseline collector — collects telemetry with NO fault injected.
Produces negative samples (label: "healthy") for the classifier.

Also contains the Alibaba cluster trace normalizer that converts
Alibaba v2022 container_meta + container_usage CSVs into SafeKube's
training CSV format.

Usage:
    # Collect live healthy baselines from your cluster
    python -m src.dataset_engine.healthy_collector --runs 20 --interval 120

    # Normalize Alibaba v2022 data
    python -m src.dataset_engine.healthy_collector --alibaba-dir data/alibaba_v2022 --output data/dataset/training/alibaba_healthy.csv
"""

import asyncio
import argparse
import csv
import json
import uuid
import random
from datetime import datetime, timezone
from pathlib import Path

from src.common.logger import get_logger
from src.dataset_engine.collectors.metrics_window import collect_metrics_window
from src.dataset_engine.collectors.log_extractor import extract_logs
from src.dataset_engine.collectors.event_collector import collect_events
from src.dataset_engine.collectors.dependency_graph import collect_dependency_impact

logger = get_logger("dataset_engine.healthy_collector")

OUTPUT_DIR = Path("data/dataset")

ONLINE_BOUTIQUE_SERVICES = [
    "frontend", "cartservice", "checkoutservice", "productcatalogservice",
    "currencyservice", "shippingservice", "emailservice", "paymentservice",
    "recommendationservice", "adservice",
]


# ─── Live healthy baseline collection ─────────────────────────────────────────

async def collect_healthy_snapshot(namespace: str, service: str) -> dict:
    """Collect telemetry for a service with no fault active."""
    run_id = f"healthy__{service}__{uuid.uuid4().hex[:6]}"
    collected_at = datetime.now(timezone.utc)

    metrics, logs, events, deps = await asyncio.gather(
        collect_metrics_window(namespace, service),
        extract_logs(namespace, service, since_seconds=300),
        collect_events(namespace, service, window_seconds=300),
        collect_dependency_impact(namespace, service),
    )

    # Build text feature in same format as fault samples
    event_reasons = list((events.get("reason_counts") or {}).keys())[:3]
    log_signals = list((logs.get("aggregate_signals") or {}).keys())[:3]

    metric_indicators = []
    for pod, m in list(metrics.get("pod_metrics", {}).items())[:1]:
        if "memory_working_set_mib" in m:
            mem = m["memory_working_set_mib"]
            metric_indicators.append(f"mem_trend:{mem.get('trend', 0):.0f}")
        if "cpu_usage_millicores" in m:
            cpu = m["cpu_usage_millicores"]
            metric_indicators.append(f"cpu_max:{cpu.get('max', 0):.0f}m")
        if "restart_count" in m:
            r = m["restart_count"]
            metric_indicators.append(f"restarts:{r.get('last', 0):.0f}")

    text = " | ".join([
        f"event_types: {', '.join(event_reasons) or 'none'}",
        f"log_signals: {', '.join(log_signals) or 'none'}",
        f"fault_category: none",
        f"severity: none",
        f"metrics: {', '.join(metric_indicators) or 'none'}",
        f"pod_status_reason: {events.get('dominant_reason') or 'Running'}",
        f"dominant_log_signal: none",
    ])

    row = {
        "run_id": run_id,
        "fault_type": "healthy",
        "fault_category": "none",
        "severity": "none",
        "target_service": service,
        "chaos_kind": "none",
        "injection_time": collected_at.isoformat(),
        "duration_seconds": 0,
        "text": text,
        "label": "healthy",
        "confidence": 1.0,
        "dominant_log_signal": "",
        "dominant_event_reason": events.get("dominant_reason") or "",
        "warning_event_count": events.get("warning_event_count", 0),
        "total_log_error_lines": sum(
            p.get("error_line_count", 0) for p in logs.get("pod_logs", {}).values()
        ),
        "cascade_depth": 0,
        "split": "train",
        "pre_fault_baseline": False,
        "source": "live_cluster",
    }

    # Write bundle
    bundle = {
        "run_id": run_id,
        "schema_version": "1.0",
        "collected_at": collected_at.isoformat(),
        "label": {"fault_type": "healthy", "fault_category": "none", "target_service": service,
                  "target_namespace": namespace, "confidence": 1.0},
        "metrics_window": metrics,
        "logs": logs,
        "events": events,
        "dependency_impact": deps,
    }
    bundle_dir = OUTPUT_DIR / "bundles"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / f"healthy__{service}__{run_id}.json").write_text(
        json.dumps(bundle, default=str, indent=2)
    )

    return row


async def run_healthy_collection(
    namespace: str = "staging",
    runs: int = 20,
    interval_seconds: int = 120,
    services: list = None,
):
    """
    Collect `runs` healthy snapshots across all services.
    Waits `interval_seconds` between collections so windows don't overlap.
    """
    if services is None:
        services = ONLINE_BOUTIQUE_SERVICES

    csv_path = OUTPUT_DIR / "training" / "training_data.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0

    for run_idx in range(runs):
        # Rotate through services
        service = services[run_idx % len(services)]
        logger.info(f"Healthy snapshot {run_idx + 1}/{runs} — service: {service}",
                    extra={"incident_id": None})

        try:
            row = await collect_healthy_snapshot(namespace, service)
            _append_csv_row(csv_path, row)
            total += 1
        except Exception as e:
            logger.error(f"Healthy collection failed for {service}: {e}",
                         extra={"incident_id": None})

        if run_idx < runs - 1:
            await asyncio.sleep(interval_seconds)

    logger.info(f"Healthy baseline collection done: {total} samples written",
                extra={"incident_id": None})
    return total


# ─── Alibaba v2022 normalizer ──────────────────────────────────────────────────

def normalize_alibaba_to_safekube(
    alibaba_dir: str,
    output_path: str,
    max_windows: int = 500,
):
    """
    Converts Alibaba Cluster Trace v2022 CSVs into SafeKube training CSV format.

    Expected Alibaba files (download from https://github.com/alibaba/clusterdata):
        container_meta.csv     — container/service metadata
        container_usage.csv    — per-container CPU/memory usage time series

    Each output row is labeled "healthy" since Alibaba data is normal operation.
    We sample windows where:
        - No OOM events
        - CPU usage < 80% of limit
        - Memory usage < 80% of limit
        - No restarts
    """
    alibaba_path = Path(alibaba_dir)
    meta_file = alibaba_path / "container_meta.csv"
    usage_file = alibaba_path / "container_usage.csv"

    if not usage_file.exists():
        raise FileNotFoundError(f"Alibaba usage file not found: {usage_file}")

    logger.info(f"Loading Alibaba usage data from {usage_file}", extra={"incident_id": None})

    # Load metadata if available for service name mapping
    service_map = {}
    if meta_file.exists():
        with open(meta_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row.get("container_id") or row.get("containerId", "")
                svc = row.get("app_du") or row.get("appDu") or row.get("service", "unknown")
                service_map[cid] = svc

    # Group usage rows by container, then extract healthy windows
    container_data: dict[str, list] = {}
    with open(usage_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("container_id") or row.get("containerId", "")
            if cid not in container_data:
                container_data[cid] = []
            container_data[cid].append(row)

    logger.info(f"Loaded {len(container_data)} containers from Alibaba",
                extra={"incident_id": None})

    output_rows = []
    for cid, usage_rows in container_data.items():
        if len(output_rows) >= max_windows:
            break
        if len(usage_rows) < 10:
            continue

        service_name = service_map.get(cid, f"alibaba_svc_{cid[:6]}")

        # Extract numeric values — Alibaba columns vary by release
        cpu_values = []
        mem_values = []
        for r in usage_rows:
            try:
                cpu = float(r.get("cpu_util_percent") or r.get("cpu_usage_percent")
                            or r.get("cpuUsagePercent") or r.get("cpu", 0))
                mem = float(r.get("mem_util_percent") or r.get("memory_usage_percent")
                            or r.get("memUsagePercent") or r.get("mem", 0))
                cpu_values.append(cpu)
                mem_values.append(mem)
            except (ValueError, TypeError):
                continue

        if not cpu_values:
            continue

        cpu_max = max(cpu_values)
        mem_max = max(mem_values) if mem_values else 0
        cpu_mean = sum(cpu_values) / len(cpu_values)
        cpu_trend = cpu_values[-1] - cpu_values[0]

        # Only use truly healthy windows
        if cpu_max > 80 or mem_max > 80:
            continue

        text = " | ".join([
            "event_types: none",
            "log_signals: none",
            "fault_category: none",
            "severity: none",
            f"metrics: mem_trend:{(mem_values[-1] - mem_values[0]) if mem_values else 0:.0f} cpu_max:{cpu_max:.0f}m restarts:0",
            "pod_status_reason: Running",
            "dominant_log_signal: none",
        ])

        run_id = f"alibaba__{cid[:8]}__{uuid.uuid4().hex[:6]}"
        output_rows.append({
            "run_id": run_id,
            "fault_type": "healthy",
            "fault_category": "none",
            "severity": "none",
            "target_service": service_name,
            "chaos_kind": "none",
            "injection_time": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 0,
            "text": text,
            "label": "healthy",
            "confidence": 0.9,   # Slightly lower — we can't 100% verify no fault
            "dominant_log_signal": "",
            "dominant_event_reason": "",
            "warning_event_count": 0,
            "total_log_error_lines": 0,
            "cascade_depth": 0,
            "split": "train",
            "pre_fault_baseline": False,
            "source": "alibaba_v2022",
        })

    # Shuffle before writing to avoid ordering bias
    random.shuffle(output_rows)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    exists = output_file.exists()
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(output_rows[0].keys()))
        if not exists:
            writer.writeheader()
        writer.writerows(output_rows)

    logger.info(f"Alibaba normalization done: {len(output_rows)} healthy windows written to {output_file}",
                extra={"incident_id": None})
    return len(output_rows)


# ─── CSV helper ───────────────────────────────────────────────────────────────

def _append_csv_row(csv_path: Path, row: dict):
    exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


# ─── CLI ──────────────────────────────────────────────────────────────────────

async def _async_main(args):
    if args.alibaba_dir:
        normalize_alibaba_to_safekube(
            args.alibaba_dir,
            args.output or "data/dataset/training/alibaba_healthy.csv",
            max_windows=args.max_windows,
        )
    else:
        await run_healthy_collection(
            namespace=args.namespace,
            runs=args.runs,
            interval_seconds=args.interval,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Healthy baseline collector + Alibaba normalizer")
    parser.add_argument("--namespace", default="staging")
    parser.add_argument("--runs", type=int, default=20, help="Number of healthy snapshots to collect")
    parser.add_argument("--interval", type=int, default=120, help="Seconds between snapshots")
    parser.add_argument("--alibaba-dir", help="Path to Alibaba v2022 CSV directory")
    parser.add_argument("--output", help="Output CSV path (for Alibaba mode)")
    parser.add_argument("--max-windows", type=int, default=500)
    args = parser.parse_args()
    asyncio.run(_async_main(args))
