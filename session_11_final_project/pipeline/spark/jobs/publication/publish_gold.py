"""Publish validated Gold history and physical serving tables, then mark the domain PASS."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from common.audit import write_run_audit
from common.config import validate_date_range, validate_run_id
from common.iceberg import ensure_namespace, merge_upsert, quote_identifier
from common.spark_session import create_spark_session
from pyspark.sql import functions as F
from pyspark.sql import types as T
from transformation.gold.registry import DOMAIN_MODELS

MARKER_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("domain", T.StringType(), False),
        T.StructField("start_date", T.StringType(), False),
        T.StructField("end_date", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("published_at", T.TimestampType(), False),
        T.StructField("error_message", T.StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--domain", choices=sorted(DOMAIN_MODELS), required=True)
    return parser.parse_args()


def write_marker(spark, args, status: str, error: Exception | None = None) -> None:
    frame = spark.createDataFrame(
        [
            (
                args.run_id,
                args.domain,
                args.start_date,
                args.end_date,
                status,
                datetime.now(UTC).replace(tzinfo=None),
                str(error)[:4000] if error else None,
            )
        ],
        MARKER_SCHEMA,
    )
    merge_upsert(
        spark,
        frame,
        "polaris.audit.published_domains",
        ["run_id", "domain", "start_date", "end_date"],
    )


def assert_domain_dq_passed(spark, args) -> None:
    rows = (
        spark.table("polaris.audit.data_quality_results")
        .where(
            (F.col("run_id") == args.run_id)
            & (F.col("domain") == args.domain)
            & (F.col("severity") == "BLOCKING")
        )
        .select("passed")
        .collect()
    )
    if not rows or any(not row["passed"] for row in rows):
        raise RuntimeError(f"Domain {args.domain} has no complete blocking-DQ PASS")


def publish_serving_table(spark, model, candidate) -> None:
    """Publish a Trino-readable Iceberg table while retaining version history."""
    ensure_namespace(spark, "polaris.serving")
    target = f"polaris.serving.{model.TARGET}"
    view_exists = (
        spark.sql("SHOW VIEWS IN polaris.serving")
        .where(F.col("viewName") == model.TARGET)
        .limit(1)
        .count()
    )
    if view_exists:
        spark.sql(f"DROP VIEW {quote_identifier(target)}")
    merge_upsert(spark, candidate, target, model.KEYS, model.PARTITION)


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session(f"shopvn-publish-{args.domain}")
    write_marker(spark, args, "PENDING")
    try:
        assert_domain_dq_passed(spark, args)
        for model in DOMAIN_MODELS[args.domain]:
            candidate = (
                spark.table(f"polaris.work.{model.TARGET}_candidate")
                .where(F.col("run_id") == args.run_id)
                .withColumn("publication_domain", F.lit(args.domain))
                .withColumn("published_at", F.current_timestamp())
            )
            merge_upsert(
                spark,
                candidate,
                f"polaris.gold_versions.{model.TARGET}",
                ["run_id", *model.KEYS],
                model.PARTITION,
            )
            publish_serving_table(spark, model, candidate)
            write_run_audit(
                spark,
                run_id=args.run_id,
                stage="gold_publish",
                object_name=model.TARGET,
                start_date=args.start_date,
                end_date=args.end_date,
                status="PASS",
                source_count=candidate.count(),
                target_count=candidate.count(),
            )
        write_marker(spark, args, "PASS")
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="gold_publish",
            object_name=args.domain,
            start_date=args.start_date,
            end_date=args.end_date,
            status="PASS",
        )
    except Exception as exc:
        write_marker(spark, args, "FAIL", exc)
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="gold_publish",
            object_name=args.domain,
            start_date=args.start_date,
            end_date=args.end_date,
            status="FAIL",
            error=exc,
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
