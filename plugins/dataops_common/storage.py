"""Partition-based file store. A rerun overwrites its date's partition instead of
appending, so reruns and backfills stay idempotent."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

DATA_ROOT = Path(os.environ.get("DATAOPS_DATA_DIR", "/opt/airflow/data"))


def partition_dir(layer: str, table: str, logical_date: datetime) -> Path:
    date_key = logical_date.strftime("%Y-%m-%d")
    return DATA_ROOT / layer / table / f"dt={date_key}"


def _partition_file(layer: str, table: str, logical_date: datetime) -> Path:
    return partition_dir(layer, table, logical_date) / "data.json"


def write_partition(layer: str, table: str, logical_date: datetime, records: list[dict]) -> Path:
    """Atomically overwrite the partition for logical_date (temp file + rename)."""
    target = _partition_file(layer, table, logical_date)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(records, handle, indent=2, sort_keys=True)
        temp_path = Path(handle.name)
    temp_path.replace(target)
    return target


def read_partition(layer: str, table: str, logical_date: datetime) -> list[dict]:
    target = _partition_file(layer, table, logical_date)
    if not target.exists():
        return []
    with target.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, list) else []


def reconcile_counts(source_count: int, target_count: int, label: str) -> None:
    if source_count != target_count:
        raise ValueError(
            f"Reconciliation failed for {label}: source={source_count}, target={target_count}"
        )
