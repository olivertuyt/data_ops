"""Silver voucher contract and usage anomaly flag."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "vouchers"
KEYS = ["voucher_code"]


def build(spark, **_):
    result = spark.table("polaris.bronze.vouchers").select(
        F.upper("voucher_code").alias("voucher_code"),
        F.lower("type").alias("voucher_type"),
        F.col("value").cast("decimal(18,2)").alias("value"),
        F.col("min_order_value").cast("decimal(18,2)").alias("min_order_value"),
        F.col("max_discount").cast("decimal(18,2)").alias("max_discount"),
        F.upper("campaign_id").alias("campaign_id"),
        F.to_date("valid_from").alias("valid_from"),
        F.to_date("valid_to").alias("valid_to"),
        F.col("usage_count").cast("long").alias("usage_count"),
        F.col("max_usage").cast("long").alias("max_usage"),
        (F.col("usage_count") > F.col("max_usage")).alias("is_usage_over_limit"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "vouchers.required",
        F.col("voucher_code").isNull() | ~F.col("voucher_type").isin("value", "percent"),
    )
    assert_unique(result, TARGET, KEYS)
    return result
