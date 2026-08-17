"""Silver logistics shipments with typed timestamps and contract checks."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "api_shipments"
KEYS = ["order_id"]


def build(spark, **_):
    result = spark.table("polaris.bronze.api_shipments").select(
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
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(result, "api_shipments.required", F.col("order_id").isNull())
    assert_no_rows(
        result,
        "api_shipments.carrier",
        ~F.col("carrier").isin("ghn", "ghtk"),
    )
    assert_no_rows(
        result,
        "api_shipments.status",
        ~F.col("shipment_status").isin(
            "picked_up", "in_transit", "delivered", "failed", "returned"
        ),
    )
    assert_no_rows(
        result,
        "api_shipments.time_order",
        F.col("actual_delivery_at").isNotNull()
        & F.col("shipped_at").isNotNull()
        & (F.col("actual_delivery_at") < F.col("shipped_at")),
    )
    assert_unique(result, TARGET, KEYS)
    return result
