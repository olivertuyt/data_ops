"""Silver inventory movement ledger."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "inventory_transactions"
KEYS = ["txn_id"]


def build(spark, **_):
    result = spark.table("polaris.bronze.inventory_transactions").select(
        "txn_id",
        "product_id",
        "order_id",
        F.lower("type").alias("transaction_type"),
        F.col("qty_change").cast("long").alias("qty_change"),
        F.to_timestamp("created_at").alias("transaction_at"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "inventory_transactions.required",
        F.col("txn_id").isNull()
        | F.col("product_id").isNull()
        | F.col("qty_change").isNull()
        | F.col("transaction_at").isNull(),
    )
    assert_no_rows(
        result,
        "inventory_transactions.type",
        ~F.col("transaction_type").isin("reserve", "release", "sold", "restock"),
    )
    assert_unique(result, TARGET, KEYS)
    return result
