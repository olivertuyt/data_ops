"""Audit-table writes shared by ingestion and publication jobs."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession, types as T

from common.iceberg import merge_upsert


AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("stage", T.StringType(), False),
        T.StructField("object_name", T.StringType(), False),
        T.StructField("business_date", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("source_count", T.LongType(), True),
        T.StructField("target_count", T.LongType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("recorded_at", T.TimestampType(), False),
    ]
)


def write_run_audit(
    spark: SparkSession,
    *,
    run_id: str,
    stage: str,
    object_name: str,
    business_date: str,
    status: str,
    source_count: int | None = None,
    target_count: int | None = None,
    error_message: str | None = None,
) -> None:
    row = [
        (
            run_id,
            stage,
            object_name,
            business_date,
            status,
            source_count,
            target_count,
            error_message,
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
    ]
    frame = spark.createDataFrame(row, AUDIT_SCHEMA)
    merge_upsert(
        spark,
        frame,
        "polaris.audit.pipeline_runs",
        ["run_id", "stage", "object_name", "business_date"],
    )
