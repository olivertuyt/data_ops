"""Publish only DQ-approved candidate rows into serving Gold tables."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, functions as F

from common.audit import write_run_audit
from common.config import validate_date_range, validate_run_id
from common.iceberg import ensure_compatible_table, merge_upsert
from common.spark_session import create_spark_session


GOLD_KEYS: dict[str, list[str]] = {
    "fact_daily_revenue": ["metric_date", "sales_channel", "revenue_basis"],
    "fact_customer_daily": ["metric_date", "customer_id"],
    "fact_delivery_daily": [
        "metric_date",
        "carrier",
        "recipient_province",
        "failure_reason",
    ],
    "fact_voucher_daily": ["metric_date", "voucher_code"],
    "fact_return_daily": ["metric_date", "category", "sales_channel"],
    "fact_product_rating_daily": ["metric_date", "category", "sales_channel"],
    "fact_inventory_eod": ["snapshot_date", "product_id", "warehouse_id"],
    "fact_product_channel_daily": ["metric_date", "product_id", "sales_channel"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser.parse_args()


def assert_dq_passed(spark, run_id: str) -> None:
    rows = (
        spark.table("polaris.audit.data_quality_results")
        .where((F.col("run_id") == run_id) & (F.col("severity") == "BLOCKING"))
        .select("passed")
        .collect()
    )
    if not rows or any(not row["passed"] for row in rows):
        raise RuntimeError(f"Run {run_id} has no complete blocking-DQ PASS record")


def publish_customer_scd2(spark, run_id: str, effective_date: str) -> None:
    snapshot = (
        spark.table("polaris.silver.customers")
        .select("customer_id", "loyalty_tier")
        .withColumn(
            "attribute_hash",
            F.sha2(F.concat_ws("|", F.col("customer_id"), F.col("loyalty_tier")), 256),
        )
    )
    target = "polaris.gold.dim_customer_scd2"
    if not spark.catalog.tableExists(target):
        seed = (
            snapshot.withColumn("effective_from", F.lit(effective_date).cast("date"))
            .withColumn("effective_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn("run_id", F.lit(run_id))
            .withColumn(
                "customer_version_key",
                F.sha2(
                    F.concat_ws(
                        "|", F.col("customer_id"), F.col("attribute_hash"), F.lit(effective_date)
                    ),
                    256,
                ),
            )
            .withColumn("published_at", F.current_timestamp())
        )
        ensure_compatible_table(spark, target, seed)

    current = (
        spark.table(target)
        .where(F.col("is_current"))
        .select(
            "customer_id",
            F.col("attribute_hash").alias("current_attribute_hash"),
        )
    )
    changed = (
        snapshot.join(current, "customer_id", "left")
        .where(
            F.col("current_attribute_hash").isNull()
            | (F.col("attribute_hash") != F.col("current_attribute_hash"))
        )
        .drop("current_attribute_hash")
    )
    changed.createOrReplaceTempView("_changed_customers")
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING _changed_customers s
        ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN MATCHED AND t.attribute_hash <> s.attribute_hash THEN UPDATE SET
          t.effective_to = date_sub(DATE '{effective_date}', 1),
          t.is_current = false,
          t.published_at = current_timestamp()
        """
    )
    new_versions = (
        changed.withColumn("effective_from", F.lit(effective_date).cast("date"))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "customer_version_key",
            F.sha2(
                F.concat_ws(
                    "|", F.col("customer_id"), F.col("attribute_hash"), F.lit(effective_date)
                ),
                256,
            ),
        )
        .withColumn("published_at", F.current_timestamp())
    )
    if new_versions.limit(1).count():
        merge_upsert(spark, new_versions, target, ["customer_version_key"])


def candidate_for_run(spark, table_name: str, run_id: str) -> DataFrame:
    return spark.table(f"polaris.work.{table_name}_candidate").where(
        F.col("run_id") == run_id
    )


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session("shopvn-publish-gold")
    try:
        assert_dq_passed(spark, args.run_id)
        publish_customer_scd2(spark, args.run_id, args.end_date)
        for table_name, keys in GOLD_KEYS.items():
            candidate = candidate_for_run(spark, table_name, args.run_id).withColumn(
                "published_at", F.current_timestamp()
            )
            target = f"polaris.gold.{table_name}"
            partition = (
                "days(snapshot_date)"
                if "snapshot_date" in candidate.columns
                else "days(metric_date)"
            )
            merge_upsert(spark, candidate, target, keys, partition)
            write_run_audit(
                spark,
                run_id=args.run_id,
                stage="gold_publish",
                object_name=table_name,
                business_date=f"{args.start_date}:{args.end_date}",
                status="PASS",
                source_count=candidate.count(),
                target_count=candidate.count(),
            )
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="gold_publish",
            object_name="all_gold_tables",
            business_date=f"{args.start_date}:{args.end_date}",
            status="PASS",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
