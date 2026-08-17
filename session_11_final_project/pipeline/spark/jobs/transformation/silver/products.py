"""Silver products with explicit price typing and loss-making flag."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "products"
KEYS = ["product_id"]


def build(spark, **_):
    result = spark.table("polaris.bronze.products").select(
        "product_id",
        "name",
        "category",
        "subcategory",
        "brand",
        "sku",
        F.col("base_price").cast("decimal(18,2)").alias("base_price"),
        F.col("cost_price").cast("decimal(18,2)").alias("cost_price"),
        F.col("weight_gram").cast("int").alias("weight_gram"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.to_timestamp("created_at").alias("created_at"),
        (F.col("cost_price") > F.col("base_price")).alias("is_loss_making"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "products.required",
        F.col("product_id").isNull()
        | F.col("sku").isNull()
        | F.col("base_price").isNull()
        | F.col("cost_price").isNull(),
    )
    assert_unique(result, TARGET, KEYS)
    return result
