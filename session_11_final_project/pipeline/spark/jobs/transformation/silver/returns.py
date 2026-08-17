"""Silver returns; only refunded status is eligible for revenue deduction."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "returns"
KEYS = ["return_id"]


def build(spark, **_):
    result = spark.table("polaris.bronze.returns").select(
        "return_id",
        "order_id",
        "reason",
        F.lower("status").alias("return_status"),
        F.col("refund_amount").cast("decimal(18,2)").alias("refund_amount"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("_run_id").alias("_source_run_id"),
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
    assert_unique(result, TARGET, KEYS)
    return result
