"""Silver product reviews without free-text comments that may contain PII."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "product_reviews"
KEYS = ["review_id"]


def build(spark, **_):
    source = spark.table("polaris.bronze.product_reviews")
    result = source.select(
        "review_id",
        "order_id",
        "product_id",
        "customer_id",
        F.col("rating").cast("int").alias("rating"),
        F.col("comment").isNotNull().alias("has_comment"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "product_reviews.required",
        F.col("review_id").isNull()
        | F.col("order_id").isNull()
        | F.col("product_id").isNull()
        | ~F.col("rating").between(1, 5),
    )
    assert_unique(result, TARGET, KEYS)
    return result
