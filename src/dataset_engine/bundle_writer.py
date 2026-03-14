"""
Writes structured incident bundles to disk.
Output formats: JSON (full fidelity) + CSV (SetFit training-ready flat format).
"""

import json
import csv
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import asdict

from src.dataset_engine.labeler import FaultLabel, to_setfit_text

OUTPUT_DIR = Path("data/dataset")
CSV_PATH = OUTPUT_DIR / "training" / "training_data.csv"


def _ensure_output_dirs():
    (OUTPUT_DIR / "bundles").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "training").mkdir(parents=True, exist_ok=True)


def _serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


async def write_bundle(
    label: FaultLabel,
    metrics: dict,
    logs: dict,
    events: dict,
    dependency_impact: dict,
    run_id: str,
) -> Path:
    """Write a full incident bundle as JSON."""
    _ensure_output_dirs()

    bundle = {
        "run_id": run_id,
        "schema_version": "1.0",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "label": asdict(label),
        "metrics_window": metrics,
        "logs": logs,
        "events": events,
        "dependency_impact": dependency_impact,
    }

    # Fix datetime serialization in label
    bundle["label"]["injection_time"] = label.injection_time.isoformat()

    filename = (
        OUTPUT_DIR / "bundles" /
        f"{label.fault_type}__{label.target_service}__{run_id}.json"
    )

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        lambda: filename.write_text(json.dumps(bundle, default=_serialize, indent=2))
    )

    return filename


async def append_training_row(
    label: FaultLabel,
    metrics: dict,
    logs: dict,
    events: dict,
    run_id: str,
    split: str = "train",
    dependency_impact: dict = None,
):
    """Append a single row to the flat CSV training file."""
    _ensure_output_dirs()
    csv_path = OUTPUT_DIR / "training" / "training_data.csv"
    exists = csv_path.exists()

    text = to_setfit_text(label, events, logs, metrics)

    row = {
        "run_id": run_id,
        "fault_type": label.fault_type,
        "fault_category": label.fault_category,
        "severity": label.severity,
        "target_service": label.target_service,
        "chaos_kind": label.chaos_kind,
        "injection_time": label.injection_time.isoformat(),
        "duration_seconds": label.duration_seconds,
        "text": text,
        "label": label.fault_type,
        "confidence": label.confidence,
        "split": split,
        "pre_fault_baseline": "pre_fault_baseline" in metrics,
        "source": "chaos_mesh",
        # Key signals
        "dominant_log_signal": logs.get("dominant_signal") or "",
        "dominant_event_reason": events.get("dominant_reason") or "",
        "warning_event_count": events.get("warning_event_count", 0),
        "total_log_error_lines": sum(
            p.get("error_line_count", 0) for p in logs.get("pod_logs", {}).values()
        ),
        "cascade_depth": dependency_impact_cascade(metrics, dependency_impact),
    }

    loop = asyncio.get_event_loop()

    def _write():
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    await loop.run_in_executor(None, _write)
    return csv_path


def dependency_impact_cascade(metrics: dict, dependency_impact: dict = None) -> int:
    """Cascade depth: number of impacted downstream services, fallback to restarting pods."""
    if dependency_impact:
        impacted = dependency_impact.get("impacted_services", [])
        if impacted:
            return len(impacted)
    count = 0
    for pod_data in metrics.get("pod_metrics", {}).values():
        restart_info = pod_data.get("restart_rate_10m", {})
        if restart_info.get("last", 0) > 0:
            count += 1
    return count


async def write_training_row(
    label: FaultLabel,
    metrics: dict,
    logs: dict,
    events: dict,
    dependency_impact: dict,
    run_id: str,
    split: str = "train",
):
    """Convenience: write both bundle and training row."""
    bundle_path = await write_bundle(label, metrics, logs, events, dependency_impact, run_id)
    csv_path = await append_training_row(label, metrics, logs, events, run_id, split=split, dependency_impact=dependency_impact)
    return bundle_path, csv_path
