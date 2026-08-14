"""Run blocking DQ and reconciliation before any Gold publication."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import functions as F

from common.audit import write_run_audit
from common.config import validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session


@dataclass
class CheckResult:
    run_id: str
    check_name: str
    severity: str
    passed: bool
    actual_value: str
    expected_value: str
    details: str
    checked_at: datetime


CANDIDATE_KEYS: dict[str, list[str]] = {
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


def scalar(spark, query: str):
    return spark.sql(query).first()[0]


def result(
    run_id: str,
    name: str,
    passed: bool,
    actual,
    expected,
    details: str,
    severity: str = "BLOCKING",
) -> CheckResult:
    return CheckResult(
        run_id=run_id,
        check_name=name,
        severity=severity,
        passed=bool(passed),
        actual_value=str(actual),
        expected_value=str(expected),
        details=details,
        checked_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def approximately_equal(left, right, tolerance: Decimal = Decimal("0.01")) -> bool:
    return abs(Decimal(str(left or 0)) - Decimal(str(right or 0))) <= tolerance


def structural_checks(spark, args) -> list[CheckResult]:
    checks: list[CheckResult] = []
    for table_name, keys in CANDIDATE_KEYS.items():
        table = f"polaris.work.{table_name}_candidate"
        frame = spark.table(table).where(F.col("run_id") == args.run_id)
        null_condition = None
        for key in keys:
            null_condition = (
                F.col(key).isNull()
                if null_condition is None
                else null_condition | F.col(key).isNull()
            )
        null_count = frame.where(null_condition).count()
        duplicate_count = frame.groupBy(*keys).count().where(F.col("count") > 1).count()
        checks.append(
            result(
                args.run_id,
                f"{table_name}.required_keys",
                null_count == 0,
                null_count,
                0,
                "Candidate business keys must be non-null",
            )
        )
        checks.append(
            result(
                args.run_id,
                f"{table_name}.duplicate_keys",
                duplicate_count == 0,
                duplicate_count,
                0,
                "Candidate grain must be unique within run_id",
            )
        )
    return checks


def reconciliation_checks(spark, args) -> list[CheckResult]:
    checks: list[CheckResult] = []
    direct_source = scalar(
        spark,
        f"""
        SELECT coalesce(sum(eligible_net_revenue), 0)
        FROM polaris.silver.direct_orders
        WHERE order_date BETWEEN DATE '{args.start_date}' AND DATE '{args.end_date}'
        """,
    )
    direct_candidate = scalar(
        spark,
        f"""
        SELECT coalesce(sum(net_revenue), 0)
        FROM polaris.work.fact_daily_revenue_candidate
        WHERE run_id = '{args.run_id}' AND sales_channel LIKE 'direct:%'
          AND revenue_basis = 'earned'
        """,
    )
    checks.append(
        result(
            args.run_id,
            "reconcile.direct_net_revenue",
            approximately_equal(direct_source, direct_candidate),
            direct_candidate,
            direct_source,
            "Silver eligible direct revenue must equal the Finance candidate",
        )
    )

    marketplace_source = scalar(
        spark,
        f"""
        SELECT coalesce(sum(CASE WHEN is_revenue_eligible THEN net_revenue ELSE 0 END), 0)
        FROM polaris.silver.marketplace_sales
        WHERE order_date BETWEEN DATE '{args.start_date}' AND DATE '{args.end_date}'
        """,
    )
    marketplace_candidate = scalar(
        spark,
        f"""
        SELECT coalesce(sum(net_revenue), 0)
        FROM polaris.work.fact_daily_revenue_candidate
        WHERE run_id = '{args.run_id}' AND sales_channel LIKE 'marketplace:%'
          AND revenue_basis = 'earned'
        """,
    )
    checks.append(
        result(
            args.run_id,
            "reconcile.marketplace_net_revenue",
            approximately_equal(marketplace_source, marketplace_candidate),
            marketplace_candidate,
            marketplace_source,
            "Silver eligible marketplace revenue must equal the Finance candidate",
        )
    )

    direct_orders = scalar(
        spark,
        f"""
        SELECT count(*) FROM polaris.silver.direct_orders
        WHERE order_date BETWEEN DATE '{args.start_date}' AND DATE '{args.end_date}'
        """,
    )
    delivery_orders = scalar(
        spark,
        f"""
        SELECT coalesce(sum(order_count), 0)
        FROM polaris.work.fact_delivery_daily_candidate
        WHERE run_id = '{args.run_id}'
        """,
    )
    checks.append(
        result(
            args.run_id,
            "reconcile.delivery_order_count",
            direct_orders == delivery_orders,
            delivery_orders,
            direct_orders,
            "LEFT JOIN shipment model must preserve every direct order",
        )
    )

    customer_spend = scalar(
        spark,
        f"""
        SELECT coalesce(sum(net_spend), 0)
        FROM polaris.work.fact_customer_daily_candidate
        WHERE run_id = '{args.run_id}'
        """,
    )
    checks.append(
        result(
            args.run_id,
            "reconcile.customer_spend",
            approximately_equal(customer_spend, direct_source),
            customer_spend,
            direct_source,
            "Customer spend must reconcile to eligible direct revenue",
        )
    )

    negative_finance = scalar(
        spark,
        f"""
        SELECT count(*) FROM polaris.work.fact_daily_revenue_candidate
        WHERE run_id = '{args.run_id}' AND (net_revenue < 0 OR order_count < 0)
        """,
    )
    checks.append(
        result(
            args.run_id,
            "finance.non_negative",
            negative_finance == 0,
            negative_finance,
            0,
            "Published revenue and counts cannot be negative",
        )
    )

    expected_inventory_rows = scalar(
        spark,
        f"""
        SELECT count(*) * (datediff(DATE '{args.end_date}', DATE '{args.start_date}') + 1)
        FROM polaris.silver.inventory
        """,
    )
    actual_inventory_rows = scalar(
        spark,
        f"""
        SELECT count(*) FROM polaris.work.fact_inventory_eod_candidate
        WHERE run_id = '{args.run_id}'
        """,
    )
    checks.append(
        result(
            args.run_id,
            "inventory.snapshot_coverage",
            actual_inventory_rows == expected_inventory_rows,
            actual_inventory_rows,
            expected_inventory_rows,
            "Every inventory key must have one row for every requested snapshot date",
        )
    )
    return checks


def sftp_completeness_checks(spark, args) -> list[CheckResult]:
    expected = (
        spark.sql(
            f"SELECT explode(sequence(DATE '{args.start_date}', DATE '{args.end_date}', "
            "interval 1 day)) AS business_date"
        )
        .crossJoin(spark.createDataFrame([("lazada",), ("shopee",), ("tiktok",)], ["partner"]))
    )
    manifests = (
        spark.table("polaris.audit.source_manifests")
        .where(F.col("run_id") == args.run_id)
        .select(F.to_date("business_date").alias("business_date"), "partner", "status")
    )
    incomplete = (
        expected.join(manifests, ["business_date", "partner"], "left")
        .where(F.coalesce(F.col("status"), F.lit("MISSING")) != "VALID")
        .count()
    )
    return [
        result(
            args.run_id,
            "sftp.expected_partner_files",
            incomplete == 0,
            incomplete,
            0,
            "All three partner files must be checksum-valid before Finance/Product Gold publish",
        )
    ]


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session("shopvn-validate-gold-candidates")
    try:
        checks = (
            structural_checks(spark, args)
            + reconciliation_checks(spark, args)
            + sftp_completeness_checks(spark, args)
        )
        frame = spark.createDataFrame([asdict(check) for check in checks])
        merge_upsert(
            spark,
            frame,
            "polaris.audit.data_quality_results",
            ["run_id", "check_name"],
        )
        failures = [check for check in checks if check.severity == "BLOCKING" and not check.passed]
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="candidate_dq",
            object_name="all_gold_candidates",
            business_date=f"{args.start_date}:{args.end_date}",
            status="FAIL" if failures else "PASS",
            source_count=len(checks),
            target_count=len(checks) - len(failures),
            error_message=(
                ",".join(check.check_name for check in failures) if failures else None
            ),
        )
        if failures:
            raise RuntimeError(
                "Gold publication blocked by: "
                + ", ".join(check.check_name for check in failures)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
