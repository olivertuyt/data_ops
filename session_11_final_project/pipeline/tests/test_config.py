from __future__ import annotations

import pytest
from common.config import required_env, validate_date_range, validate_run_id


def test_validate_run_id_accepts_airflow_safe_value() -> None:
    assert validate_run_id("scheduled__2026-06-01T02_00_00_07_00")


@pytest.mark.parametrize("value", ["", "bad value", "$(command)", "../escape", "a/b"])
def test_validate_run_id_rejects_unsafe_value(value: str) -> None:
    with pytest.raises(ValueError):
        validate_run_id(value)


def test_validate_date_range_rejects_reverse_window() -> None:
    with pytest.raises(ValueError):
        validate_date_range("2026-06-30", "2026-06-01")


def test_required_env_fails_closed_for_missing_and_placeholder(monkeypatch) -> None:
    monkeypatch.delenv("SHOPVN_TEST_REQUIRED", raising=False)
    with pytest.raises(RuntimeError, match="SHOPVN_TEST_REQUIRED"):
        required_env("SHOPVN_TEST_REQUIRED")
    monkeypatch.setenv("SHOPVN_TEST_REQUIRED", "<set-secret>")
    with pytest.raises(RuntimeError, match="placeholder"):
        required_env("SHOPVN_TEST_REQUIRED")
