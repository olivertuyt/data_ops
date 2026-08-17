"""Silver order items retaining zero-quantity rows without metric contamination."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "order_items"
KEYS = ["item_id"]


def build(spark, **_):
    result = (
        spark.table("polaris.bronze.order_items")
        .select(
            "item_id",
            "order_id",
            "product_id",
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("discount_per_item").cast("decimal(18,2)").alias("discount_per_item"),
            F.col("total_price").cast("decimal(18,2)").alias("total_price"),
            F.col("_run_id").alias("_source_run_id"),
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
    assert_unique(result, TARGET, KEYS)
    return result
