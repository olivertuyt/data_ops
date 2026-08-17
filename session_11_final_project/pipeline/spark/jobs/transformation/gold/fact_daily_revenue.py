"""Finance revenue by earned/cash basis and direct/marketplace channel."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, in_window

TARGET = "fact_daily_revenue"
DOMAIN = "finance"
KEYS = ["metric_date", "sales_channel", "revenue_basis"]
PARTITION = "days(metric_date)"


def build(spark, args):
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
            .cast("decimal(24,2)")
            .alias("gross_revenue"),
            F.sum("eligible_net_revenue").cast("decimal(24,2)").alias("net_revenue"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("discount_amount")).otherwise(0))
            .cast("decimal(24,2)")
            .alias("discount_cost"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("refunded_amount")).otherwise(0))
            .cast("decimal(24,2)")
            .alias("refund_amount"),
            F.lit(0).cast("decimal(24,2)").alias("commission_cost"),
            F.countDistinct(F.when(F.col("is_revenue_eligible"), F.col("order_id"))).alias(
                "order_count"
            ),
            F.sum(F.col("is_discount_anomaly").cast("long")).alias("anomaly_count"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            F.lit("direct").alias("channel_group"),
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
            F.lit("earned").alias("revenue_basis"),
            "gross_revenue",
            "net_revenue",
            "discount_cost",
            "refund_amount",
            "commission_cost",
            "order_count",
            "anomaly_count",
        )
    )
    marketplace = spark.table("polaris.silver.marketplace_sales")

    def aggregate_marketplace(date_column: str, revenue_basis: str):
        return (
            marketplace.where(in_window(date_column, args.start_date, args.end_date))
            .groupBy(date_column, "partner")
            .agg(
                F.sum(
                    F.when(
                        F.col("is_revenue_eligible"),
                        F.col("sale_price") * F.col("quantity_sold"),
                    ).otherwise(0)
                )
                .cast("decimal(24,2)")
                .alias("gross_revenue"),
                F.sum(F.when(F.col("is_revenue_eligible"), F.col("net_revenue")).otherwise(0))
                .cast("decimal(24,2)")
                .alias("net_revenue"),
                F.sum(F.when(F.col("is_revenue_eligible"), F.col("platform_discount")).otherwise(0))
                .cast("decimal(24,2)")
                .alias("discount_cost"),
                F.lit(0).cast("decimal(24,2)").alias("refund_amount"),
                F.sum(
                    F.when(
                        F.col("is_revenue_eligible"),
                        (F.col("sale_price") * F.col("quantity_sold")) - F.col("net_revenue"),
                    ).otherwise(0)
                )
                .cast("decimal(24,2)")
                .alias("commission_cost"),
                F.countDistinct(
                    F.when(F.col("is_revenue_eligible"), F.col("external_order_id"))
                ).alias("order_count"),
                F.sum(F.col("is_net_revenue_mismatch").cast("long")).alias("anomaly_count"),
            )
            .select(
                F.col(date_column).alias("metric_date"),
                F.lit("marketplace").alias("channel_group"),
                F.concat(F.lit("marketplace:"), F.col("partner")).alias("sales_channel"),
                F.lit(revenue_basis).alias("revenue_basis"),
                "gross_revenue",
                "net_revenue",
                "discount_cost",
                "refund_amount",
                "commission_cost",
                "order_count",
                "anomaly_count",
            )
        )

    result = direct.unionByName(aggregate_marketplace("order_date", "earned")).unionByName(
        aggregate_marketplace("settlement_date", "cash_received")
    )
    return add_candidate_metadata(result, args)
