"""Silver current inventory state."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "inventory"
KEYS = ["product_id", "warehouse_id"]


def build(spark, **_):
    result = spark.table("polaris.bronze.inventory").select(
        "product_id",
        "warehouse_id",
        F.col("stock_qty").cast("long").alias("stock_qty"),
        F.to_timestamp("last_updated").alias("last_updated"),
        (F.col("stock_qty") < 0).alias("is_negative_stock"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "inventory.required",
        F.col("product_id").isNull() | F.col("warehouse_id").isNull() | F.col("stock_qty").isNull(),
    )
    assert_unique(result, TARGET, KEYS)
    return result
