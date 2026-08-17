from __future__ import annotations

import csv
from pathlib import Path

import pytest
from ingestion.sftp_bronze import RAW_COLUMNS, validate_csv_header


def write_csv(path: Path, columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerow(["value"] * len(columns))


def test_additive_sftp_column_is_recorded_not_rejected(tmp_path: Path) -> None:
    path = tmp_path / "partner.csv"
    write_csv(path, [*RAW_COLUMNS, "new_nullable_field"])
    assert validate_csv_header(path) == ("new_nullable_field",)


def test_missing_required_sftp_column_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "partner.csv"
    write_csv(path, list(RAW_COLUMNS[1:]))
    with pytest.raises(RuntimeError, match="Missing required CSV columns"):
        validate_csv_header(path)
