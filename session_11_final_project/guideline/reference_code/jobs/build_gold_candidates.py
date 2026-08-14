"""Build all analytics candidates in the non-serving work namespace."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, functions as F

from common.audit import write_run_audit
from common.config import validate_date_range, validate_run_id
from common.iceberg import merge_upsert
from common.spark_session import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser.parse_args()


def in_window(column: str, start_date: str, end_date: str):
    return (F.col(column) >= F.lit(start_date).cast("date")) & (
        F.col(column) <= F.lit(end_date).cast("date")
    )


def add_run_id(frame: DataFrame, run_id: str) -> DataFrame:
    return frame.withColumn("run_id", F.lit(run_id)).withColumn(
        "candidate_created_at", F.current_timestamp()
    )


def daily_revenue(spark, args) -> DataFrame:
    direct = (
        spark.table("polaris.silver.direct_orders")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy("order_date", "channel")
        .agg(
            F.sum(
                F.when(
                    F.col("is_revenue_eligible"),
                    F.col("subtotal") + F.col("shipping_fee"),
                ).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("gross_revenue"),
            F.sum("eligible_net_revenue").cast("decimal(20,2)").alias("net_revenue"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("discount_amount")).otherwise(0))
            .cast("decimal(20,2)")
            .alias("discount_cost"),
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("refunded_amount")).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("refund_amount"),
            F.countDistinct(
                F.when(F.col("is_revenue_eligible"), F.col("order_id"))
            ).alias("order_count"),
            F.sum(F.col("is_discount_anomaly").cast("long")).alias("anomaly_count"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
            F.lit("earned").alias("revenue_basis"),
            "gross_revenue",
            "net_revenue",
            "discount_cost",
            "refund_amount",
            "order_count",
            "anomaly_count",
        )
    )
    marketplace_source = spark.table("polaris.silver.marketplace_sales").where(
        in_window("order_date", args.start_date, args.end_date)
        | in_window("settlement_date", args.start_date, args.end_date)
    )

    def aggregate_marketplace(date_column: str, revenue_basis: str) -> DataFrame:
        return (
            marketplace_source.where(
                in_window(date_column, args.start_date, args.end_date)
            )
            .groupBy(date_column, "partner")
            .agg(
            F.sum(
                F.when(
                    F.col("is_revenue_eligible"),
                    F.col("sale_price") * F.col("quantity_sold"),
                ).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("gross_revenue"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("net_revenue")).otherwise(0))
            .cast("decimal(20,2)")
            .alias("net_revenue"),
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("platform_discount")).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("discount_cost"),
            F.lit(0).cast("decimal(20,2)").alias("refund_amount"),
            F.countDistinct("external_order_id").alias("order_count"),
            F.sum(F.col("is_net_revenue_mismatch").cast("long")).alias("anomaly_count"),
            )
            .select(
            F.col(date_column).alias("metric_date"),
            F.concat(F.lit("marketplace:"), F.col("partner")).alias("sales_channel"),
            F.lit(revenue_basis).alias("revenue_basis"),
            "gross_revenue",
            "net_revenue",
            "discount_cost",
            "refund_amount",
            "order_count",
            "anomaly_count",
            )
        )

    marketplace_earned = aggregate_marketplace("order_date", "earned")
    marketplace_cash = aggregate_marketplace("settlement_date", "cash_received")
    return add_run_id(
        direct.unionByName(marketplace_earned).unionByName(marketplace_cash), args.run_id
    )


def customer_daily(spark, args) -> DataFrame:
    customer_window = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
    history_window = customer_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    orders = (
        spark.table("polaris.silver.direct_orders")
        .where(F.col("is_revenue_eligible"))
        .withColumn("customer_first_order_date", F.min("order_date").over(history_window))
        .withColumn("previous_order_date", F.lag("order_date").over(customer_window))
        .withColumn(
            "is_repeat_within_30_days",
            F.col("previous_order_date").isNotNull()
            & (F.datediff("order_date", "previous_order_date") <= 30),
        )
        .where(in_window("order_date", args.start_date, args.end_date))
    )
    result = orders.groupBy("order_date", "customer_id").agg(
        F.sum("eligible_net_revenue").cast("decimal(20,2)").alias("net_spend"),
        F.countDistinct("order_id").alias("eligible_order_count"),
        F.min("customer_first_order_date").alias("customer_first_order_date"),
        F.max(F.col("is_repeat_within_30_days").cast("int")).cast("boolean").alias(
            "is_repeat_within_30_days"
        ),
    )
    return add_run_id(result.withColumnRenamed("order_date", "metric_date"), args.run_id)


def delivery_daily(spark, args) -> DataFrame:
    orders = spark.table("polaris.silver.direct_orders").where(
        in_window("order_date", args.start_date, args.end_date)
    )
    shipments = spark.table("polaris.silver.api_shipments")
    joined = orders.join(shipments, "order_id", "left")
    result = joined.groupBy(
        F.col("order_date").alias("metric_date"),
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier"),
        F.coalesce("recipient_province", F.lit("unknown")).alias("recipient_province"),
        F.coalesce("failure_reason", F.lit("none")).alias("failure_reason"),
    ).agg(
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct(F.when(F.col("tracking_code").isNotNull(), F.col("order_id"))).alias(
            "shipment_count"
        ),
        F.countDistinct(
            F.when(F.col("shipment_status") == "delivered", F.col("order_id"))
        ).alias("delivered_count"),
        F.countDistinct(
            F.when(F.col("shipment_status") == "failed", F.col("order_id"))
        ).alias("failed_count"),
        F.countDistinct(
            F.when(F.col("delivery_attempts") > 1, F.col("order_id"))
        ).alias("redelivery_count"),
        F.avg(
            F.when(
                F.col("shipment_status") == "delivered",
                (F.unix_timestamp("actual_delivery_at") - F.unix_timestamp("shipped_at"))
                / F.lit(3600.0),
            )
        ).alias("avg_delivery_hours"),
    )
    return add_run_id(result, args.run_id)


def voucher_daily(spark, args) -> DataFrame:
    orders = spark.table("polaris.silver.direct_orders").where(
        in_window("order_date", args.start_date, args.end_date)
        & F.col("voucher_code").isNotNull()
    )
    result = orders.groupBy(
        F.col("order_date").alias("metric_date"), "voucher_code"
    ).agg(
        F.countDistinct("order_id").alias("usage_count"),
        F.sum("eligible_net_revenue").cast("decimal(20,2)").alias("net_revenue"),
        F.sum("discount_amount").cast("decimal(20,2)").alias("discount_cost"),
    )
    return add_run_id(result, args.run_id)


def return_daily(spark, args) -> DataFrame:
    direct_orders = spark.table("polaris.silver.direct_orders").select(
        "order_id", "order_date", "order_status"
    )
    direct_items = spark.table("polaris.silver.order_items")
    products = spark.table("polaris.silver.products").select("product_id", "category")
    direct = (
        direct_items.join(direct_orders, "order_id")
        .join(products, "product_id")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy(F.col("order_date").alias("metric_date"), "category")
        .agg(
            F.sum("eligible_quantity").alias("sold_units"),
            F.sum(
                F.when(F.col("order_status") == "returned", F.col("eligible_quantity")).otherwise(0)
            ).alias("returned_units"),
        )
        .withColumn("sales_channel", F.lit("direct"))
    )
    marketplace = (
        spark.table("polaris.silver.marketplace_sales")
        .join(products, F.col("shopvn_product_id") == products.product_id)
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy(F.col("order_date").alias("metric_date"), "category", "partner")
        .agg(
            F.sum("quantity_sold").alias("sold_units"),
            F.sum(
                F.when(F.col("marketplace_status") == "returned", F.col("quantity_sold")).otherwise(0)
            ).alias("returned_units"),
        )
        .withColumn("sales_channel", F.concat(F.lit("marketplace:"), F.col("partner")))
        .drop("partner")
    )
    return add_run_id(direct.unionByName(marketplace), args.run_id)


def product_rating_daily(spark, args) -> DataFrame:
    reviews = spark.table("polaris.silver.product_reviews")
    products = spark.table("polaris.silver.products").select("product_id", "category")
    orders = spark.table("polaris.silver.direct_orders").select("order_id", "channel")
    result = (
        reviews.join(products, "product_id")
        .join(orders, "order_id")
        .where(
            (F.to_date("created_at") >= F.lit(args.start_date).cast("date"))
            & (F.to_date("created_at") <= F.lit(args.end_date).cast("date"))
        )
        .groupBy(
            F.to_date("created_at").alias("metric_date"),
            "category",
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
        )
        .agg(F.avg("rating").alias("average_rating"), F.count("review_id").alias("review_count"))
    )
    return add_run_id(result, args.run_id)


def inventory_eod(spark, args) -> DataFrame:
    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{args.start_date}'), "
        f"to_date('{args.end_date}'), interval 1 day)) AS snapshot_date"
    )
    inventory = spark.table("polaris.silver.inventory")
    transactions = spark.table("polaris.silver.inventory_transactions")
    future_movements = (
        inventory.crossJoin(dates)
        .join(transactions, "product_id", "left")
        .groupBy("snapshot_date", "product_id", "warehouse_id", "stock_qty")
        .agg(
            F.sum(
                F.when(
                    F.to_date("transaction_at") > F.col("snapshot_date"),
                    F.col("qty_change"),
                ).otherwise(0)
            ).alias("movement_after_snapshot")
        )
        .withColumn(
            "eod_stock_qty", F.col("stock_qty") - F.col("movement_after_snapshot")
        )
        .select("snapshot_date", "product_id", "warehouse_id", "eod_stock_qty")
    )
    return add_run_id(future_movements, args.run_id)


def product_channel_daily(spark, args) -> DataFrame:
    products = spark.table("polaris.silver.products").select(
        "product_id", "category", "is_loss_making"
    )
    orders = spark.table("polaris.silver.direct_orders").select(
        "order_id", "order_date", "channel", "is_revenue_eligible"
    )
    direct = (
        spark.table("polaris.silver.order_items")
        .join(orders, "order_id")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy("order_date", "product_id", "channel")
        .agg(
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("eligible_quantity")).otherwise(0)).alias(
                "units_sold"
            ),
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("eligible_item_revenue")).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("net_revenue"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            "product_id",
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
            "units_sold",
            "net_revenue",
        )
    )
    marketplace = (
        spark.table("polaris.silver.marketplace_sales")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy("order_date", "shopvn_product_id", "partner")
        .agg(
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("quantity_sold")).otherwise(0)
            ).alias("units_sold"),
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("net_revenue")).otherwise(0)
            )
            .cast("decimal(20,2)")
            .alias("net_revenue"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            F.col("shopvn_product_id").alias("product_id"),
            F.concat(F.lit("marketplace:"), F.col("partner")).alias("sales_channel"),
            "units_sold",
            "net_revenue",
        )
    )
    combined = direct.unionByName(marketplace).join(products, "product_id", "left")
    stock = spark.table("polaris.silver.inventory").groupBy("product_id").agg(
        F.sum("stock_qty").alias("current_stock_qty")
    )
    rolling = (
        Window.partitionBy("product_id", "sales_channel")
        .orderBy(F.col("metric_date").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    result = (
        combined.join(stock, "product_id", "left")
        .withColumn("avg_daily_units_sold_7d", F.sum("units_sold").over(rolling) / F.lit(7.0))
        .withColumn(
            "days_to_stockout",
            F.when(
                F.col("avg_daily_units_sold_7d") > 0,
                F.col("current_stock_qty") / F.col("avg_daily_units_sold_7d"),
            ),
        )
    )
    return add_run_id(result, args.run_id)


def main() -> None:
    args = parse_args()
    args.run_id = validate_run_id(args.run_id)
    validate_date_range(args.start_date, args.end_date)
    spark = create_spark_session("shopvn-build-gold-candidates")
    try:
        candidates: dict[str, tuple[DataFrame, list[str], str | None]] = {
            "fact_daily_revenue": (
                daily_revenue(spark, args),
                ["run_id", "metric_date", "sales_channel", "revenue_basis"],
                "days(metric_date)",
            ),
            "fact_customer_daily": (
                customer_daily(spark, args),
                ["run_id", "metric_date", "customer_id"],
                "days(metric_date)",
            ),
            "fact_delivery_daily": (
                delivery_daily(spark, args),
                [
                    "run_id",
                    "metric_date",
                    "carrier",
                    "recipient_province",
                    "failure_reason",
                ],
                "days(metric_date)",
            ),
            "fact_voucher_daily": (
                voucher_daily(spark, args),
                ["run_id", "metric_date", "voucher_code"],
                "days(metric_date)",
            ),
            "fact_return_daily": (
                return_daily(spark, args),
                ["run_id", "metric_date", "category", "sales_channel"],
                "days(metric_date)",
            ),
            "fact_product_rating_daily": (
                product_rating_daily(spark, args),
                ["run_id", "metric_date", "category", "sales_channel"],
                "days(metric_date)",
            ),
            "fact_inventory_eod": (
                inventory_eod(spark, args),
                ["run_id", "snapshot_date", "product_id", "warehouse_id"],
                "days(snapshot_date)",
            ),
            "fact_product_channel_daily": (
                product_channel_daily(spark, args),
                ["run_id", "metric_date", "product_id", "sales_channel"],
                "days(metric_date)",
            ),
        }
        for name, (frame, keys, partition) in candidates.items():
            target = f"polaris.work.{name}_candidate"
            merge_upsert(spark, frame, target, keys, partition)
            write_run_audit(
                spark,
                run_id=args.run_id,
                stage="candidate",
                object_name=name,
                business_date=f"{args.start_date}:{args.end_date}",
                status="BUILT",
                source_count=frame.count(),
                target_count=frame.count(),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
