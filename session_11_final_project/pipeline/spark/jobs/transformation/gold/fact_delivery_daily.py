"""Operations delivery performance while preserving orders without shipments."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, in_window

TARGET = "fact_delivery_daily"
DOMAIN = "operations"
KEYS = ["metric_date", "carrier", "recipient_province", "failure_reason"]
PARTITION = "days(metric_date)"


def build(spark, args):
    orders = spark.table("polaris.silver.direct_orders").where(
        in_window("order_date", args.start_date, args.end_date)
    )
    shipments = spark.table("polaris.silver.api_shipments")
    result = (
        orders.join(shipments, "order_id", "left")
        .groupBy(
            F.col("order_date").alias("metric_date"),
            F.coalesce("carrier", F.lit("unassigned")).alias("carrier"),
            F.coalesce("recipient_province", F.lit("unknown")).alias("recipient_province"),
            F.coalesce("failure_reason", F.lit("none")).alias("failure_reason"),
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct(F.when(F.col("tracking_code").isNotNull(), F.col("order_id"))).alias(
                "shipment_count"
            ),
            F.countDistinct(
                F.when(F.col("shipment_status") == "delivered", F.col("order_id"))
            ).alias("delivered_count"),
            F.countDistinct(F.when(F.col("shipment_status") == "failed", F.col("order_id"))).alias(
                "failed_count"
            ),
            F.countDistinct(F.when(F.col("delivery_attempts") > 1, F.col("order_id"))).alias(
                "redelivery_count"
            ),
            F.avg(
                F.when(
                    F.col("shipment_status") == "delivered",
                    (F.unix_timestamp("actual_delivery_at") - F.unix_timestamp("shipped_at"))
                    / F.lit(3600.0),
                )
            ).alias("avg_delivery_hours"),
        )
    )
    return add_candidate_metadata(result, args)
