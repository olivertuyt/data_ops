"""Persist domain-scoped DQ/reconciliation and block failed publication."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal

from common.audit import write_run_audit
from common.config import env_csv, validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session
from pyspark.sql import functions as F
from transformation.gold.registry import DOMAIN_MODELS


@dataclass
class CheckResult:
    run_id: str
    domain: str
    dataset: str
    check_name: str
    severity: str
    passed: bool
    actual_value: str
    expected_value: str
    details: str
    start_date: str
    end_date: str
    checked_at: datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--domain", choices=sorted(DOMAIN_MODELS), required=True)
    return parser.parse_args()


def check(args, dataset, name, passed, actual, expected, details, severity="BLOCKING"):
    return CheckResult(
        run_id=args.run_id,
        domain=args.domain,
        dataset=dataset,
        check_name=name,
        severity=severity,
        passed=bool(passed),
        actual_value=str(actual),
        expected_value=str(expected),
        details=details,
        start_date=args.start_date,
        end_date=args.end_date,
        checked_at=datetime.now(UTC).replace(tzinfo=None),
    )


def approximately_equal(left, right, tolerance=Decimal("0.01")) -> bool:
    return abs(Decimal(str(left or 0)) - Decimal(str(right or 0))) <= tolerance


def frame_sum(frame, column: str):
    return frame.agg(F.coalesce(F.sum(column), F.lit(0)).alias("value")).first()["value"]


def structural_checks(spark, args):
    checks = []
    for model in DOMAIN_MODELS[args.domain]:
        frame = spark.table(f"polaris.work.{model.TARGET}_candidate").where(
            F.col("run_id") == args.run_id
        )
        null_condition = None
        for key in model.KEYS:
            current = F.col(key).isNull()
            null_condition = current if null_condition is None else null_condition | current
        null_count = frame.where(null_condition).count()
        duplicate_count = frame.groupBy(*model.KEYS).count().where("count > 1").count()
        row_count = frame.count()
        allow_empty = getattr(model, "ALLOW_EMPTY", False)
        empty_details = (
            "Sparse-event contract permits a valid empty logical window"
            if allow_empty
            else "A supported test window with source data must not publish an empty model"
        )
        checks.extend(
            [
                check(
                    args,
                    model.TARGET,
                    "required_keys",
                    null_count == 0,
                    null_count,
                    0,
                    "Candidate business keys must be non-null",
                ),
                check(
                    args,
                    model.TARGET,
                    "duplicate_keys",
                    duplicate_count == 0,
                    duplicate_count,
                    0,
                    "Candidate grain must be unique within run_id",
                ),
                check(
                    args,
                    model.TARGET,
                    "non_empty",
                    row_count > 0 or allow_empty,
                    row_count,
                    ">=0" if allow_empty else ">0",
                    empty_details,
                ),
            ]
        )
    return checks


def sftp_completeness_check(spark, args):
    partners = [(item,) for item in env_csv("SFTP_PARTNERS")]
    expected = spark.sql(
        f"SELECT explode(sequence(DATE '{args.start_date}', DATE '{args.end_date}', "
        "interval 1 day)) AS business_date"
    ).crossJoin(spark.createDataFrame(partners, ["partner"]))
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
    return check(
        args,
        "marketplace_files",
        "expected_partner_files",
        incomplete == 0,
        incomplete,
        0,
        "All configured partner/date files must be checksum-valid for this domain",
    )


def finance_checks(spark, args):
    candidate = spark.table("polaris.work.fact_daily_revenue_candidate").where(
        F.col("run_id") == args.run_id
    )
    direct_source = frame_sum(
        spark.table("polaris.silver.direct_orders").where(
            F.col("order_date").between(args.start_date, args.end_date)
        ),
        "eligible_net_revenue",
    )
    direct_candidate = frame_sum(
        candidate.where(
            F.col("sales_channel").startswith("direct:") & (F.col("revenue_basis") == "earned")
        ),
        "net_revenue",
    )
    marketplace_source = frame_sum(
        spark.table("polaris.silver.marketplace_sales").where(
            F.col("order_date").between(args.start_date, args.end_date)
            & F.col("is_revenue_eligible")
        ),
        "net_revenue",
    )
    marketplace_candidate = frame_sum(
        candidate.where(
            F.col("sales_channel").startswith("marketplace:") & (F.col("revenue_basis") == "earned")
        ),
        "net_revenue",
    )
    negative = candidate.where((F.col("net_revenue") < 0) | (F.col("order_count") < 0)).count()
    anomalies = (
        spark.table("polaris.silver.direct_orders")
        .where(
            F.col("order_date").between(args.start_date, args.end_date)
            & F.col("is_discount_anomaly")
        )
        .count()
    )
    return [
        check(
            args,
            "fact_daily_revenue",
            "direct_net_revenue",
            approximately_equal(direct_source, direct_candidate),
            direct_candidate,
            direct_source,
            "Eligible direct revenue must reconcile from Silver to candidate",
        ),
        check(
            args,
            "fact_daily_revenue",
            "marketplace_net_revenue",
            approximately_equal(marketplace_source, marketplace_candidate),
            marketplace_candidate,
            marketplace_source,
            "Eligible marketplace earned revenue must reconcile",
        ),
        check(
            args,
            "fact_daily_revenue",
            "non_negative",
            negative == 0,
            negative,
            0,
            "Finance measures cannot be negative",
        ),
        check(
            args,
            "direct_orders",
            "discount_anomaly_observed",
            anomalies == 0,
            anomalies,
            0,
            "Known anomalies are retained but excluded from eligible revenue",
            severity="WARNING",
        ),
        sftp_completeness_check(spark, args),
    ]


def customer_checks(spark, args):
    daily = spark.table("polaris.work.fact_customer_daily_candidate").where(
        F.col("run_id") == args.run_id
    )
    source_spend = frame_sum(
        spark.table("polaris.silver.direct_orders").where(
            F.col("order_date").between(args.start_date, args.end_date)
        ),
        "eligible_net_revenue",
    )
    candidate_spend = frame_sum(daily, "daily_net_spend")
    source_voucher_orders = (
        spark.table("polaris.silver.direct_orders")
        .where(
            F.col("order_date").between(args.start_date, args.end_date)
            & F.col("voucher_code").isNotNull()
            & F.col("is_revenue_eligible")
        )
        .select("order_id")
        .distinct()
        .count()
    )
    candidate_voucher_orders = frame_sum(
        spark.table("polaris.work.fact_voucher_daily_candidate").where(
            F.col("run_id") == args.run_id
        ),
        "usage_count",
    )
    prohibited = {"full_name", "phone", "email", "ward", "comment"}
    inspected = [spark.table("polaris.silver.customers")]
    inspected.extend(
        spark.table(f"polaris.work.{model.TARGET}_candidate").where(F.col("run_id") == args.run_id)
        for model in DOMAIN_MODELS[args.domain]
    )
    found = sorted({column for frame in inspected for column in frame.columns} & prohibited)
    phone_missing = spark.table("polaris.silver.customers").where(F.col("is_phone_missing")).count()
    return [
        check(
            args,
            "fact_customer_daily",
            "customer_spend",
            approximately_equal(source_spend, candidate_spend),
            candidate_spend,
            source_spend,
            "Daily customer spend must reconcile to eligible direct revenue",
        ),
        check(
            args,
            "fact_voucher_daily",
            "voucher_usage",
            int(candidate_voucher_orders or 0) == source_voucher_orders,
            candidate_voucher_orders,
            source_voucher_orders,
            "Voucher usage count must include only revenue-eligible orders",
        ),
        check(
            args,
            "customer_domain",
            "no_plaintext_pii_columns",
            not found,
            found,
            [],
            "Silver/Gold candidates must not expose direct PII or free-text review comments",
        ),
        check(
            args,
            "customers",
            "nullable_phone_observed",
            phone_missing == 0,
            phone_missing,
            0,
            "Nullable phone is valid and remains represented as a null hash",
            severity="WARNING",
        ),
    ]


def operations_checks(spark, args):
    source_orders = (
        spark.table("polaris.silver.direct_orders")
        .where(F.col("order_date").between(args.start_date, args.end_date))
        .count()
    )
    candidate_orders = frame_sum(
        spark.table("polaris.work.fact_delivery_daily_candidate").where(
            F.col("run_id") == args.run_id
        ),
        "order_count",
    )
    technical_failures = 0
    if spark.catalog.tableExists("polaris.audit.api_events"):
        technical_failures = (
            spark.table("polaris.audit.api_events")
            .where((F.col("run_id") == args.run_id) & F.col("blocks_downstream"))
            .count()
        )
    return [
        check(
            args,
            "fact_delivery_daily",
            "left_join_preserves_orders",
            int(candidate_orders or 0) == source_orders,
            candidate_orders,
            source_orders,
            "Orders without shipments must remain as carrier=unassigned",
        ),
        check(
            args,
            "api_shipments",
            "no_exhausted_technical_failures",
            technical_failures == 0,
            technical_failures,
            0,
            "Expected 404 is allowed; exhausted timeout/429/5xx blocks Operations",
        ),
        sftp_completeness_check(spark, args),
    ]


def product_checks(spark, args):
    inventory_keys = spark.table("polaris.silver.inventory").count()
    days = (
        validate_date_range(args.start_date, args.end_date)[1]
        - validate_date_range(args.start_date, args.end_date)[0]
    ).days + 1
    actual_inventory = (
        spark.table("polaris.work.fact_inventory_eod_candidate")
        .where(F.col("run_id") == args.run_id)
        .count()
    )
    zero_quantity = (
        spark.table("polaris.silver.order_items").where(F.col("is_zero_quantity")).count()
    )
    loss_products = spark.table("polaris.silver.products").where(F.col("is_loss_making")).count()
    return [
        check(
            args,
            "fact_inventory_eod",
            "snapshot_coverage",
            actual_inventory == inventory_keys * days,
            actual_inventory,
            inventory_keys * days,
            "Every inventory key requires one row per requested snapshot date",
        ),
        check(
            args,
            "order_items",
            "zero_quantity_observed",
            zero_quantity == 0,
            zero_quantity,
            0,
            "Zero quantity rows remain traceable but contribute zero eligible units",
            severity="WARNING",
        ),
        check(
            args,
            "products",
            "loss_making_products_observed",
            loss_products == 0,
            loss_products,
            0,
            "Loss-making products are business insight, not pipeline rejection",
            severity="WARNING",
        ),
        sftp_completeness_check(spark, args),
    ]


DOMAIN_CHECKS = {
    "finance": finance_checks,
    "customer": customer_checks,
    "operations": operations_checks,
    "product": product_checks,
}


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session(f"shopvn-dq-{args.domain}")
    try:
        checks = structural_checks(spark, args) + DOMAIN_CHECKS[args.domain](spark, args)
        frame = spark.createDataFrame([asdict(item) for item in checks])
        merge_upsert(
            spark,
            frame,
            "polaris.audit.data_quality_results",
            ["run_id", "domain", "dataset", "check_name"],
        )
        failures = [item for item in checks if item.severity == "BLOCKING" and not item.passed]
        write_run_audit(
            spark,
            run_id=args.run_id,
            stage="candidate_dq",
            object_name=args.domain,
            start_date=args.start_date,
            end_date=args.end_date,
            status="FAIL" if failures else "PASS",
            source_count=len(checks),
            target_count=len(checks) - len(failures),
            error=(
                RuntimeError(",".join(item.check_name for item in failures)) if failures else None
            ),
        )
        if failures:
            raise RuntimeError(
                "Publication blocked by: " + ", ".join(item.check_name for item in failures)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
