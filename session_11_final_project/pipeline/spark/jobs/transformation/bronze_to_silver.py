"""Thin runner for model-scoped Bronze-to-Silver transformations."""

from __future__ import annotations

import argparse
import time

from common.audit import write_run_audit
from common.config import required_env, validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session

from transformation.silver import (
    api_shipments,
    customers,
    direct_orders,
    inventory,
    inventory_transactions,
    marketplace_sales,
    order_items,
    product_reviews,
    products,
    returns,
    vouchers,
)

MODEL_GROUPS = {
    "postgres": [
        customers,
        products,
        returns,
        direct_orders,
        order_items,
        vouchers,
        inventory,
        inventory_transactions,
        product_reviews,
    ],
    "api": [api_shipments],
    "sftp": [marketplace_sales],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--model-group", choices=sorted(MODEL_GROUPS), required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session(f"shopvn-silver-{args.model_group}")
    pii_salt = required_env("PII_HASH_SALT")
    try:
        for model in MODEL_GROUPS[args.model_group]:
            started = time.monotonic()
            try:
                frame = model.build(spark, pii_salt=pii_salt)
                target = f"polaris.silver.{model.TARGET}"
                merge_upsert(spark, frame, target, model.KEYS)
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="silver",
                    object_name=model.TARGET,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    status="PASS",
                    source_count=frame.count(),
                    target_count=spark.table(target).count(),
                    duration_ms=int((time.monotonic() - started) * 1000),
                )
            except Exception as exc:
                write_run_audit(
                    spark,
                    run_id=run_id,
                    stage="silver",
                    object_name=model.TARGET,
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
