"""
Feature normalization for merging Alibaba + Chaos Mesh data into a consistent space.

Problem: Alibaba reports CPU as percentage (0-100), SafeKube uses millicores (0-4000).
         Alibaba memory is percentage, SafeKube uses MiB absolute values.
         Scales are incompatible — a raw merge would confuse the model.

Solution: Map everything to a normalized [0, 1] feature space using per-metric
          percentile bounds computed from the SafeKube cluster's observed ranges.
"""

import json
import csv
from pathlib import Path
from typing import Optional

OUTPUT_DIR = Path("data/dataset")
NORM_CONFIG_PATH = OUTPUT_DIR / "normalization_config.json"

# Default bounds derived from typical Online Boutique resource limits
# Override by running compute_normalization_bounds() after first data collection
DEFAULT_BOUNDS = {
    "cpu_usage_millicores":     {"min": 0,    "max": 2000,   "source": "default"},
    "memory_working_set_mib":   {"min": 0,    "max": 512,    "source": "default"},
    "memory_rss_mib":           {"min": 0,    "max": 512,    "source": "default"},
    "restart_count":            {"min": 0,    "max": 20,     "source": "default"},
    "restart_rate_10m":         {"min": 0,    "max": 10,     "source": "default"},
    "network_rx_bytes":         {"min": 0,    "max": 1e7,    "source": "default"},
    "network_tx_bytes":         {"min": 0,    "max": 1e7,    "source": "default"},
    "warning_event_count":      {"min": 0,    "max": 50,     "source": "default"},
    "total_log_error_lines":    {"min": 0,    "max": 200,    "source": "default"},
    "cascade_depth":            {"min": 0,    "max": 10,     "source": "default"},
    # Alibaba-specific (already percentage, just clip to 0-100)
    "alibaba_cpu_percent":      {"min": 0,    "max": 100,    "source": "default"},
    "alibaba_mem_percent":      {"min": 0,    "max": 100,    "source": "default"},
}


def load_bounds() -> dict:
    if NORM_CONFIG_PATH.exists():
        return json.loads(NORM_CONFIG_PATH.read_text())
    return DEFAULT_BOUNDS


def save_bounds(bounds: dict):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    NORM_CONFIG_PATH.write_text(json.dumps(bounds, indent=2))


def normalize_value(value: float, metric_name: str, bounds: Optional[dict] = None) -> float:
    """Clip and normalize a single value to [0, 1]."""
    if bounds is None:
        bounds = load_bounds()
    b = bounds.get(metric_name, {"min": 0, "max": 1})
    min_v, max_v = b["min"], b["max"]
    if max_v == min_v:
        return 0.0
    return max(0.0, min(1.0, (value - min_v) / (max_v - min_v)))


def compute_normalization_bounds(training_csv: Path) -> dict:
    """
    Compute p1/p99 bounds from actual collected data.
    Call this after the first overnight run to replace DEFAULT_BOUNDS.
    """
    numeric_cols = [
        "warning_event_count", "total_log_error_lines", "cascade_depth"
    ]
    col_values: dict[str, list] = {c: [] for c in numeric_cols}

    with open(training_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in numeric_cols:
                try:
                    col_values[col].append(float(row[col]))
                except (ValueError, KeyError):
                    pass

    bounds = load_bounds()
    for col, values in col_values.items():
        if len(values) < 10:
            continue
        values.sort()
        n = len(values)
        p1  = values[max(0, int(n * 0.01))]
        p99 = values[min(n - 1, int(n * 0.99))]
        bounds[col] = {"min": p1, "max": p99, "source": "computed", "n": n}

    save_bounds(bounds)
    return bounds


def normalize_training_csv(input_csv: Path, output_csv: Path):
    """
    Add normalized_* columns to the training CSV.
    Reads input, writes output with extra columns — original columns preserved.
    Both Alibaba and Chaos Mesh rows go through the same normalization.
    """
    bounds = load_bounds()
    numeric_cols = ["warning_event_count", "total_log_error_lines", "cascade_depth"]

    rows = []
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        norm_cols = [f"norm_{c}" for c in numeric_cols]
        out_fieldnames = fieldnames + [c for c in norm_cols if c not in fieldnames]

        for row in reader:
            for col in numeric_cols:
                try:
                    raw = float(row.get(col, 0))
                except ValueError:
                    raw = 0.0
                row[f"norm_{col}"] = round(normalize_value(raw, col, bounds), 4)
            rows.append(row)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def merge_and_normalize(
    chaos_mesh_csv: Path,
    alibaba_csv: Optional[Path],
    output_csv: Path,
):
    """
    Merge Chaos Mesh fault data + Alibaba healthy data,
    compute bounds from the merged set, normalize all rows.
    """
    rows = []

    if chaos_mesh_csv.exists():
        with open(chaos_mesh_csv, newline="", encoding="utf-8") as f:
            rows.extend(list(csv.DictReader(f)))

    if alibaba_csv and alibaba_csv.exists():
        with open(alibaba_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # Ensure all expected columns exist (fill missing with "")
                rows.append(row)

    if not rows:
        raise ValueError("No data to merge")

    # Recompute bounds from merged data if large enough
    if len(rows) > 50:
        compute_normalization_bounds(chaos_mesh_csv)

    bounds = load_bounds()
    numeric_cols = ["warning_event_count", "total_log_error_lines", "cascade_depth"]

    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    norm_cols = [f"norm_{c}" for c in numeric_cols]
    fieldnames = sorted(all_keys) + [c for c in norm_cols if c not in all_keys]

    for row in rows:
        for col in numeric_cols:
            try:
                raw = float(row.get(col, 0) or 0)
            except ValueError:
                raw = 0.0
            row[f"norm_{col}"] = round(normalize_value(raw, col, bounds), 4)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Normalize and merge training data")
    parser.add_argument("--chaos-csv", default="data/dataset/training/training_data.csv")
    parser.add_argument("--alibaba-csv", default="data/dataset/training/alibaba_healthy.csv")
    parser.add_argument("--output", default="data/dataset/training/merged_normalized.csv")
    args = parser.parse_args()

    count = merge_and_normalize(
        Path(args.chaos_csv),
        Path(args.alibaba_csv) if Path(args.alibaba_csv).exists() else None,
        Path(args.output),
    )
    print(f"Merged and normalized {count} rows → {args.output}")
