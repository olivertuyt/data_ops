"""Ingest all nine PostgreSQL source tables into raw Iceberg Bronze tables."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, functions as F

from common.audit import write_run_audit
from common.config import required_env, validate_date_range, validate_run_id
from common.iceberg import assert_unique_non_null, merge_upsert
from common.spark_session import create_spark_session


TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("item_id",),
    "vouchers": ("voucher_code",),
    "inventory": ("product_id",),
    "inventory_transactions": ("txn_id",),
    "returns": ("return_id",),
    "product_reviews": ("review_id",),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser.parse_args()


def jdbc_options(table_name: str) -> dict[str, str]:
    host = required_env("SHOPVN_DB_HOST")
    port = required_env("SHOPVN_DB_PORT")
    database = required_env("SHOPVN_DB_NAME")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "dbtable": f'public."{table_name}"',
        "user": required_env("SHOPVN_DB_USER"),
        "password": required_env("SHOPVN_DB_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }


def add_metadata(frame: DataFrame, table_name: str, run_id: str) -> DataFrame:
    return (
        frame.withColumn("_source_system", F.lit("postgresql"))
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    business_window = f"{args.start_date}:{args.end_date}"
    spark = create_spark_session("shopvn-postgres-bronze")
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table_name, keys in TABLE_PRIMARY_KEYS.items():
            try:
                raw = spark.read.format("jdbc").options(**jdbc_options(table_name)).load()
                source_count = raw.count()
                staged = add_metadata(raw, table_name, run_id)
                target = f"polaris.bronze.{table_name}"
                merge_upsert(spark, staged, target, keys)
                assert_unique_non_null(spark, target, keys)
                target_count = spark.table(target).count()
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="bronze_postgres",
                    object_name=table_name,
                    business_date=business_window,
                    status="PASS",
                    source_count=source_count,
                    target_count=target_count,
                )
            except Exception as exc:
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="bronze_postgres",
                    object_name=table_name,
                    business_date=business_window,
                    status="FAIL",
                    error_message=str(exc)[:4000],
                )
                raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
