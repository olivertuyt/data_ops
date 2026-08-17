"""Historical end-of-day inventory reconstructed from current stock and movements."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, date_spine

TARGET = "fact_inventory_eod"
DOMAIN = "product"
KEYS = ["snapshot_date", "product_id", "warehouse_id"]
PARTITION = "days(snapshot_date)"


def build(spark, args):
    dates = date_spine(spark, args.start_date, args.end_date, "snapshot_date")
    inventory = spark.table("polaris.silver.inventory")
    transactions = spark.table("polaris.silver.inventory_transactions")
    result = (
        inventory.crossJoin(dates)
        .join(transactions, "product_id", "left")
        .groupBy("snapshot_date", "product_id", "warehouse_id", "stock_qty")
        .agg(
            F.sum(
                F.when(
                    F.to_date("transaction_at") > F.col("snapshot_date"),
                    F.col("qty_change"),
                ).otherwise(0)
            ).alias("movement_after_snapshot")
        )
        .withColumn(
            "eod_stock_qty",
            F.col("stock_qty") - F.coalesce(F.col("movement_after_snapshot"), F.lit(0)),
        )
        .select("snapshot_date", "product_id", "warehouse_id", "eod_stock_qty")
    )
    return add_candidate_metadata(result, args)
