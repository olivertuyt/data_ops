"""Voucher and campaign effectiveness at daily grain."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, in_window

TARGET = "fact_voucher_daily"
DOMAIN = "customer"
KEYS = ["metric_date", "voucher_code"]
PARTITION = "days(metric_date)"


def build(spark, args):
    vouchers = spark.table("polaris.silver.vouchers").select("voucher_code", "campaign_id")
    orders = spark.table("polaris.silver.direct_orders").where(
        in_window("order_date", args.start_date, args.end_date) & F.col("voucher_code").isNotNull()
    )
    result = (
        orders.join(vouchers, "voucher_code", "left")
        .groupBy(F.col("order_date").alias("metric_date"), "voucher_code", "campaign_id")
        .agg(
            F.countDistinct(F.when(F.col("is_revenue_eligible"), F.col("order_id"))).alias(
                "usage_count"
            ),
            F.sum("eligible_net_revenue").cast("decimal(24,2)").alias("net_revenue"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("discount_amount")).otherwise(0))
            .cast("decimal(24,2)")
            .alias("discount_cost"),
            F.sum(F.col("is_discount_anomaly").cast("long")).alias("anomaly_count"),
        )
    )
    return add_candidate_metadata(result, args)
