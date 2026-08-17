"""Validated runtime configuration boundary for all pipeline jobs."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    if value.strip().startswith("<") and value.strip().endswith(">"):
        raise RuntimeError(f"Environment variable {name} still contains a placeholder")
    return value.strip()


def optional_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else value.strip()


def env_int(name: str, *, minimum: int | None = None) -> int:
    raw = required_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"Environment variable {name} must be >= {minimum}")
    return value


def env_csv(name: str) -> tuple[str, ...]:
    values = tuple(item.strip().lower() for item in required_env(name).split(",") if item.strip())
    if not values or len(values) != len(set(values)):
        raise RuntimeError(f"Environment variable {name} must contain unique CSV values")
    return values


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO date: {value}") from exc


def validate_date_range(start_date: str, end_date: str) -> tuple[date, date]:
    start = parse_iso_date(start_date)
    end = parse_iso_date(end_date)
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    return start, end


def validate_run_id(value: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(value):
        raise ValueError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, numbers, dot, underscore, or hyphen"
        )
    return value


@dataclass(frozen=True)
class PostgresSourceConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresSourceConfig:
        return cls(
            host=required_env("SHOPVN_DB_HOST"),
            port=env_int("SHOPVN_DB_PORT", minimum=1),
            database=required_env("SHOPVN_DB_NAME"),
            user=required_env("SHOPVN_DB_USER"),
            password=required_env("SHOPVN_DB_PASSWORD"),
        )


@dataclass(frozen=True)
class ApiSourceConfig:
    base_url: str
    api_key: str
    connect_timeout_seconds: int
    read_timeout_seconds: int
    requests_per_minute: int

    @classmethod
    def from_env(cls) -> ApiSourceConfig:
        base_url = required_env("LOGISTICS_API_URL")
        if not base_url.startswith(("http://", "https://")):
            raise RuntimeError("LOGISTICS_API_URL must use http or https")
        return cls(
            base_url=base_url.rstrip("/"),
            api_key=required_env("LOGISTICS_API_KEY"),
            connect_timeout_seconds=env_int("LOGISTICS_API_CONNECT_TIMEOUT_SECONDS", minimum=1),
            read_timeout_seconds=env_int("LOGISTICS_API_READ_TIMEOUT_SECONDS", minimum=1),
            requests_per_minute=env_int("LOGISTICS_API_REQUESTS_PER_MINUTE", minimum=1),
        )


@dataclass(frozen=True)
class SftpSourceConfig:
    host: str
    port: int
    user: str
    password: str
    remote_base_dir: str
    partners: tuple[str, ...]

    @classmethod
    def from_env(cls) -> SftpSourceConfig:
        remote_base_dir = required_env("SFTP_REMOTE_BASE_DIR")
        if not remote_base_dir.startswith("/"):
            raise RuntimeError("SFTP_REMOTE_BASE_DIR must be an absolute remote path")
        return cls(
            host=required_env("SFTP_HOST"),
            port=env_int("SFTP_PORT", minimum=1),
            user=required_env("SFTP_USER"),
            password=required_env("SFTP_PASSWORD"),
            remote_base_dir=remote_base_dir.rstrip("/"),
            partners=env_csv("SFTP_PARTNERS"),
        )


def audit_versions() -> tuple[str, str, str]:
    return (
        required_env("SHOPVN_CODE_VERSION"),
        required_env("SHOPVN_CONFIG_VERSION"),
        required_env("SHOPVN_SCHEMA_VERSION"),
    )
