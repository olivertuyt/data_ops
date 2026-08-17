"""Ingest all nine PostgreSQL source tables into raw Iceberg Bronze tables."""

from __future__ import annotations

import argparse
import time

from common.audit import write_run_audit
from common.config import PostgresSourceConfig, validate_date_range, validate_run_id
from common.iceberg import assert_unique_non_null, merge_upsert
from common.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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


def jdbc_options(table_name: str, config: PostgresSourceConfig) -> dict[str, str]:
    return {
        "url": f"jdbc:postgresql://{config.host}:{config.port}/{config.database}",
        "dbtable": f'public."{table_name}"',
        "user": config.user,
        "password": config.password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }


def add_metadata(frame: DataFrame, table_name: str, run_id: str) -> DataFrame:
    source_columns = frame.columns
    return (
        frame.withColumn(
            "_source_row_hash",
            F.sha2(
                F.concat_ws(
                    "\u001f",
                    *[F.coalesce(F.col(name).cast("string"), F.lit("")) for name in source_columns],
                ),
                256,
            ),
        )
        .withColumn("_source_system", F.lit("postgresql"))
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    config = PostgresSourceConfig.from_env()
    spark = create_spark_session("shopvn-postgres-bronze")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for table_name, keys in TABLE_PRIMARY_KEYS.items():
            started = time.monotonic()
            try:
                raw = spark.read.format("jdbc").options(**jdbc_options(table_name, config)).load()
                source_count = raw.count()
                staged = add_metadata(raw, table_name, run_id)
                assert_unique_non_null(staged, f"postgres.{table_name}", keys)
                target = f"polaris.bronze.{table_name}"
                merge_upsert(spark, staged, target, keys)
                target_count = spark.table(target).count()
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="bronze_postgres",
                    object_name=table_name,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    status="PASS",
                    source_count=source_count,
                    target_count=target_count,
                    duration_ms=int((time.monotonic() - started) * 1000),
                )
            except Exception as exc:
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="bronze_postgres",
                    object_name=table_name,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    status="FAIL",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error=exc,
                )
                raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
