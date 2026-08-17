"""Reusable structural validation primitives; no table-specific business rules."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame


def assert_no_rows(frame: DataFrame, check_name: str, condition) -> None:
    if frame.where(condition).limit(1).count():
        raise RuntimeError(f"Blocking DQ failed: {check_name}")


def assert_unique(frame: DataFrame, check_name: str, keys: Sequence[str]) -> None:
    if frame.groupBy(*keys).count().where("count > 1").limit(1).count():
        raise RuntimeError(f"Blocking DQ failed: {check_name}.duplicate_key")
