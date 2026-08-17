"""Structured Iceberg audit writes shared by all pipeline stages."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from common.config import audit_versions
from common.iceberg import merge_upsert

AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("object_name", T.StringType(), False),
        T.StructField("start_date", T.StringType(), False),
        T.StructField("end_date", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("source_count", T.LongType(), True),
        T.StructField("target_count", T.LongType(), True),
        T.StructField("amount_sum", T.DecimalType(24, 2), True),
        T.StructField("duration_ms", T.LongType(), True),
        T.StructField("error_class", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("code_version", T.StringType(), False),
        T.StructField("config_version", T.StringType(), False),
        T.StructField("schema_version", T.StringType(), False),
        T.StructField("recorded_at", T.TimestampType(), False),
    ]
)


def write_run_audit(
    spark: SparkSession,
    *,
    run_id: str,
    stage: str,
    object_name: str,
    start_date: str,
    end_date: str,
    status: str,
    source_count: int | None = None,
    target_count: int | None = None,
    amount_sum=None,
    duration_ms: int | None = None,
    error: Exception | None = None,
) -> None:
    code_version, config_version, schema_version = audit_versions()
    row = [
        (
            run_id,
            stage,
            object_name,
            start_date,
            end_date,
            status,
            source_count,
            target_count,
            amount_sum,
            duration_ms,
            type(error).__name__ if error else None,
            str(error)[:4000] if error else None,
            code_version,
            config_version,
            schema_version,
            datetime.now(UTC).replace(tzinfo=None),
        )
    ]
    frame = spark.createDataFrame(row, AUDIT_SCHEMA)
    merge_upsert(
        spark,
        frame,
        "polaris.audit.pipeline_runs",
        ["run_id", "stage", "object_name", "start_date", "end_date"],
    )
