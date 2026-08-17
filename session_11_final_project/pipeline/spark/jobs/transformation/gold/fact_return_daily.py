"""Return-rate metrics by category and direct/marketplace sales channel."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, in_window

TARGET = "fact_return_daily"
DOMAIN = "operations"
KEYS = ["metric_date", "category", "sales_channel"]
PARTITION = "days(metric_date)"


def build(spark, args):
    products = spark.table("polaris.silver.products").select("product_id", "category")
    orders = spark.table("polaris.silver.direct_orders").select(
        "order_id", "order_date", "order_status", "channel", "is_revenue_eligible"
    )
    direct = (
        spark.table("polaris.silver.order_items")
        .join(orders, "order_id")
        .join(products, "product_id")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy(F.col("order_date").alias("metric_date"), "category", "channel")
        .agg(
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("eligible_quantity")).otherwise(0)
            ).alias("sold_units"),
            F.sum(
                F.when(
                    (F.col("order_status") == "returned") & F.col("is_revenue_eligible"),
                    F.col("eligible_quantity"),
                ).otherwise(0)
            ).alias("returned_units"),
        )
        .withColumn("sales_channel", F.concat(F.lit("direct:"), F.col("channel")))
        .drop("channel")
    )
    marketplace = (
        spark.table("polaris.silver.marketplace_sales")
        .join(products, F.col("shopvn_product_id") == products.product_id)
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy(F.col("order_date").alias("metric_date"), "category", "partner")
        .agg(
            F.sum("quantity_sold").alias("sold_units"),
            F.sum(
                F.when(F.col("marketplace_status") == "returned", F.col("quantity_sold")).otherwise(
                    0
                )
            ).alias("returned_units"),
        )
        .withColumn("sales_channel", F.concat(F.lit("marketplace:"), F.col("partner")))
        .drop("partner")
    )
    result = direct.unionByName(marketplace).withColumn(
        "return_rate",
        F.when(F.col("sold_units") > 0, F.col("returned_units") / F.col("sold_units")),
    )
    return add_candidate_metadata(result, args)
