"""Create validated, typed, and PII-safe Silver tables from raw Bronze data."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, Window, functions as F

from common.audit import write_run_audit
from common.config import required_env, validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser.parse_args()


def hash_pii(column_name: str, salt: str):
    return F.sha2(F.concat(F.lit(salt), F.coalesce(F.col(column_name), F.lit(""))), 256)


def assert_no_rows(frame: DataFrame, check_name: str, condition) -> None:
    failures = frame.where(condition).limit(1).count()
    if failures:
        raise RuntimeError(f"Blocking Silver DQ failed: {check_name}")


def assert_unique(frame: DataFrame, check_name: str, keys: Sequence[str]) -> None:
    duplicate = frame.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise RuntimeError(f"Blocking Silver DQ failed: {check_name}.duplicate_key")


def transform_customers(spark, salt: str) -> DataFrame:
    source = spark.table("polaris.bronze.customers")
    result = source.select(
        "customer_id",
        hash_pii("full_name", salt).alias("full_name_hash"),
        F.when(F.col("phone").isNull(), F.lit(None)).otherwise(hash_pii("phone", salt)).alias(
            "phone_hash"
        ),
        hash_pii("email", salt).alias("email_hash"),
        "city",
        "district",
        "ward",
        F.lower("gender").alias("gender"),
        F.to_date("date_of_birth").alias("date_of_birth"),
        F.lower("loyalty_tier").alias("loyalty_tier"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("phone").isNull().alias("is_phone_missing"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(result, "customers.required", F.col("customer_id").isNull())
    assert_no_rows(
        result,
        "customers.loyalty_tier",
        ~F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
    )
    assert_unique(result, "customers", ["customer_id"])
    return result


def transform_products(spark) -> DataFrame:
    source = spark.table("polaris.bronze.products")
    result = source.select(
        "product_id",
        "name",
        "category",
        "subcategory",
        "brand",
        "sku",
        F.col("base_price").cast("decimal(18,2)").alias("base_price"),
        F.col("cost_price").cast("decimal(18,2)").alias("cost_price"),
        F.col("weight_gram").cast("int").alias("weight_gram"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.to_timestamp("created_at").alias("created_at"),
        (F.col("cost_price") > F.col("base_price")).alias("is_loss_making"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "products.required",
        F.col("product_id").isNull()
        | F.col("base_price").isNull()
        | F.col("cost_price").isNull(),
    )
    assert_unique(result, "products", ["product_id"])
    return result


def transform_returns(spark) -> DataFrame:
    source = spark.table("polaris.bronze.returns")
    result = source.select(
        "return_id",
        "order_id",
        "reason",
        F.lower("status").alias("return_status"),
        F.col("refund_amount").cast("decimal(18,2)").alias("refund_amount"),
        F.to_timestamp("created_at").alias("created_at"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "returns.required",
        F.col("return_id").isNull()
        | F.col("order_id").isNull()
        | F.col("refund_amount").isNull()
        | (F.col("refund_amount") < 0),
    )
    assert_no_rows(
        result,
        "returns.status",
        ~F.col("return_status").isin("pending", "approved", "rejected", "refunded"),
    )
    assert_unique(result, "returns", ["return_id"])
    return result


def transform_orders(spark, returns: DataFrame) -> DataFrame:
    refunds = (
        returns.where(F.col("return_status") == "refunded")
        .groupBy("order_id")
        .agg(F.sum("refund_amount").alias("refunded_amount"))
    )
    source = spark.table("polaris.bronze.orders")
    typed = source.select(
        "order_id",
        "customer_id",
        F.upper("voucher_code").alias("voucher_code"),
        F.lower("channel").alias("channel"),
        F.to_date("order_date").alias("order_date"),
        F.lower("status").alias("order_status"),
        F.col("subtotal").cast("decimal(18,2)").alias("subtotal"),
        F.col("shipping_fee").cast("decimal(18,2)").alias("shipping_fee"),
        F.col("discount_amount").cast("decimal(18,2)").alias("discount_amount"),
        F.col("total_amount").cast("decimal(18,2)").alias("total_amount"),
        F.lower("payment_method").alias("payment_method"),
        F.lower("payment_status").alias("payment_status"),
        F.to_timestamp("created_at").alias("created_at"),
    )
    result = (
        typed.join(refunds, "order_id", "left")
        .fillna({"refunded_amount": 0})
        .withColumn("is_zero_shipping_anomaly", F.col("shipping_fee") == 0)
        .withColumn("is_discount_anomaly", F.col("discount_amount") > F.col("subtotal"))
        .withColumn(
            "is_revenue_eligible",
            (~F.col("is_discount_anomaly"))
            & (F.col("order_status") != "cancelled")
            & F.col("payment_status").isin("paid", "refunded"),
        )
        .withColumn(
            "eligible_net_revenue",
            F.when(
                F.col("is_revenue_eligible"),
                F.greatest(
                    F.col("total_amount") - F.col("refunded_amount"),
                    F.lit(0).cast("decimal(18,2)"),
                ),
            ).otherwise(F.lit(0).cast("decimal(18,2)")),
        )
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    assert_no_rows(
        result,
        "orders.required",
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_date").isNull()
        | F.col("subtotal").isNull()
        | F.col("total_amount").isNull(),
    )
    assert_unique(result, "orders", ["order_id"])
    return result


def transform_order_items(spark) -> DataFrame:
    source = spark.table("polaris.bronze.order_items")
    result = (
        source.select(
            "item_id",
            "order_id",
            "product_id",
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("discount_per_item")
            .cast("decimal(18,2)")
            .alias("discount_per_item"),
            F.col("total_price").cast("decimal(18,2)").alias("total_price"),
        )
        .withColumn("is_zero_quantity", F.col("quantity") <= 0)
        .withColumn(
            "eligible_quantity",
            F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(F.lit(0)),
        )
        .withColumn(
            "eligible_item_revenue",
            F.when(F.col("quantity") > 0, F.col("total_price")).otherwise(
                F.lit(0).cast("decimal(18,2)")
            ),
        )
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    assert_no_rows(
        result,
        "order_items.required",
        F.col("item_id").isNull()
        | F.col("order_id").isNull()
        | F.col("product_id").isNull()
        | F.col("quantity").isNull(),
    )
    assert_unique(result, "order_items", ["item_id"])
    return result


def transform_simple_tables(spark) -> dict[str, tuple[DataFrame, list[str]]]:
    vouchers = spark.table("polaris.bronze.vouchers").select(
        F.upper("voucher_code").alias("voucher_code"),
        F.lower("type").alias("voucher_type"),
        F.col("value").cast("decimal(18,2)").alias("value"),
        F.col("min_order_value").cast("decimal(18,2)").alias("min_order_value"),
        F.col("max_discount").cast("decimal(18,2)").alias("max_discount"),
        "campaign_id",
        F.to_date("valid_from").alias("valid_from"),
        F.to_date("valid_to").alias("valid_to"),
        F.col("usage_count").cast("long").alias("usage_count"),
        F.col("max_usage").cast("long").alias("max_usage"),
        (F.col("usage_count") > F.col("max_usage")).alias("is_usage_over_limit"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    inventory = spark.table("polaris.bronze.inventory").select(
        "product_id",
        "warehouse_id",
        F.col("stock_qty").cast("long").alias("stock_qty"),
        F.to_timestamp("last_updated").alias("last_updated"),
        (F.col("stock_qty") < 0).alias("is_negative_stock"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    transactions = spark.table("polaris.bronze.inventory_transactions").select(
        "txn_id",
        "product_id",
        "order_id",
        F.lower("type").alias("transaction_type"),
        F.col("qty_change").cast("long").alias("qty_change"),
        F.to_timestamp("created_at").alias("transaction_at"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    reviews = spark.table("polaris.bronze.product_reviews").select(
        "review_id",
        "order_id",
        "product_id",
        "customer_id",
        F.col("rating").cast("int").alias("rating"),
        "comment",
        F.to_timestamp("created_at").alias("created_at"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        vouchers,
        "vouchers.required",
        F.col("voucher_code").isNull()
        | ~F.col("voucher_type").isin("value", "percent"),
    )
    assert_no_rows(
        inventory,
        "inventory.required",
        F.col("product_id").isNull()
        | F.col("warehouse_id").isNull()
        | F.col("stock_qty").isNull(),
    )
    assert_no_rows(
        transactions,
        "inventory_transactions.required",
        F.col("txn_id").isNull()
        | F.col("product_id").isNull()
        | F.col("qty_change").isNull()
        | F.col("transaction_at").isNull(),
    )
    assert_no_rows(
        reviews,
        "product_reviews.required",
        F.col("review_id").isNull()
        | F.col("order_id").isNull()
        | F.col("product_id").isNull()
        | ~F.col("rating").between(1, 5),
    )
    result = {
        "vouchers": (vouchers, ["voucher_code"]),
        "inventory": (inventory, ["product_id", "warehouse_id"]),
        "inventory_transactions": (transactions, ["txn_id"]),
        "product_reviews": (reviews, ["review_id"]),
    }
    for name, (frame, keys) in result.items():
        assert_unique(frame, name, keys)
    return result


def transform_api_shipments(spark) -> DataFrame:
    source = spark.table("polaris.bronze.api_shipments")
    result = source.select(
        "order_id",
        F.lower("carrier").alias("carrier"),
        "tracking_code",
        F.lower("status").alias("shipment_status"),
        F.col("actual_shipping_fee").cast("decimal(18,2)").alias("actual_shipping_fee"),
        F.to_timestamp("shipped_at").alias("shipped_at"),
        F.to_timestamp("actual_delivery_date").alias("actual_delivery_at"),
        F.to_date("estimated_delivery_date").alias("estimated_delivery_date"),
        "recipient_province",
        "recipient_district",
        F.col("delivery_attempts").cast("int").alias("delivery_attempts"),
        F.lower("failure_reason").alias("failure_reason"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(result, "api_shipments.required", F.col("order_id").isNull())
    assert_no_rows(
        result,
        "api_shipments.time_order",
        F.col("actual_delivery_at").isNotNull()
        & F.col("shipped_at").isNotNull()
        & (F.col("actual_delivery_at") < F.col("shipped_at")),
    )
    assert_unique(result, "api_shipments", ["order_id"])
    return result


def transform_marketplace(spark) -> DataFrame:
    source = spark.table("polaris.bronze.marketplace_sales")
    window = Window.partitionBy(
        "partner", "external_order_id", "shopvn_product_id", "seller_sku"
    ).orderBy(F.col("_ingested_at").desc(), F.col("_record_hash").desc())
    latest = source.withColumn("_rank", F.row_number().over(window)).where(F.col("_rank") == 1)
    result = (
        latest.select(
            "external_order_id",
            F.col("shopvn_product_id").cast("int").alias("shopvn_product_id"),
            "seller_sku",
            F.lower("partner").alias("partner"),
            F.to_date("order_date").alias("order_date"),
            F.col("quantity_sold").cast("long").alias("quantity_sold"),
            F.col("sale_price").cast("decimal(18,2)").alias("sale_price"),
            F.col("platform_discount")
            .cast("decimal(18,2)")
            .alias("platform_discount"),
            F.col("commission_rate").cast("decimal(9,6)").alias("commission_rate"),
            F.col("net_revenue").cast("decimal(18,2)").alias("net_revenue"),
            F.to_date("settlement_date").alias("settlement_date"),
            F.lower("status").alias("marketplace_status"),
            "_source_file",
            "_record_hash",
        )
        .withColumn(
            "calculated_net_revenue",
            F.floor(
                F.col("sale_price")
                * F.col("quantity_sold")
                * (F.lit(1) - F.col("commission_rate"))
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "is_net_revenue_mismatch",
            F.col("net_revenue") != F.col("calculated_net_revenue"),
        )
        .withColumn(
            "is_revenue_eligible", F.col("marketplace_status") == "completed"
        )
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    assert_no_rows(
        result,
        "marketplace.required_or_cast",
        F.col("external_order_id").isNull()
        | F.col("shopvn_product_id").isNull()
        | F.col("partner").isNull()
        | F.col("order_date").isNull()
        | F.col("quantity_sold").isNull()
        | F.col("net_revenue").isNull(),
    )
    assert_no_rows(
        result,
        "marketplace.partner",
        ~F.col("partner").isin("lazada", "shopee", "tiktok"),
    )
    assert_no_rows(
        result,
        "marketplace.revenue_formula",
        F.col("is_net_revenue_mismatch"),
    )
    assert_unique(
        result,
        "marketplace",
        ["partner", "external_order_id", "shopvn_product_id", "seller_sku"],
    )
    return result


def main() -> None:
    args = parse_args()
    run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    salt = required_env("PII_HASH_SALT")
    spark = create_spark_session("shopvn-bronze-to-silver")
    try:
        returns = transform_returns(spark)
        outputs: dict[str, tuple[DataFrame, list[str]]] = {
            "customers": (transform_customers(spark, salt), ["customer_id"]),
            "products": (transform_products(spark), ["product_id"]),
            "returns": (returns, ["return_id"]),
            "direct_orders": (transform_orders(spark, returns), ["order_id"]),
            "order_items": (transform_order_items(spark), ["item_id"]),
            "api_shipments": (transform_api_shipments(spark), ["order_id"]),
            "marketplace_sales": (
                transform_marketplace(spark),
                ["partner", "external_order_id", "shopvn_product_id", "seller_sku"],
            ),
            **transform_simple_tables(spark),
        }

        for name, (frame, keys) in outputs.items():
            target = f"polaris.silver.{name}"
            merge_upsert(spark, frame, target, keys)
            write_run_audit(
                spark,
                run_id=run_id,
                stage="silver",
                object_name=name,
                business_date=f"{args.start_date}:{args.end_date}",
                status="PASS",
                source_count=frame.count(),
                target_count=spark.table(target).count(),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
