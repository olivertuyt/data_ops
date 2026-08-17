"""Silver direct orders and the authoritative direct-revenue eligibility rule."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "direct_orders"
KEYS = ["order_id"]


def build(spark, **_):
    refunds = (
        spark.table("polaris.silver.returns")
        .where(F.col("return_status") == "refunded")
        .groupBy("order_id")
        .agg(F.sum("refund_amount").alias("refunded_amount"))
    )
    typed = spark.table("polaris.bronze.orders").select(
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
        F.col("_run_id").alias("_source_run_id"),
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
        "direct_orders.required",
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_date").isNull()
        | F.col("subtotal").isNull()
        | F.col("total_amount").isNull(),
    )
    assert_no_rows(
        result,
        "direct_orders.channel",
        ~F.col("channel").isin("web", "app"),
    )
    assert_unique(result, TARGET, KEYS)
    return result
