"""Product sales, loss status, sell-through, and stockout forecast by channel."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, in_window

TARGET = "fact_product_channel_daily"
DOMAIN = "product"
KEYS = ["metric_date", "product_id", "sales_channel"]
PARTITION = "days(metric_date)"


def build(spark, args):
    products = spark.table("polaris.silver.products").select(
        "product_id", "category", "cost_price", "is_loss_making"
    )
    orders = spark.table("polaris.silver.direct_orders").select(
        "order_id", "order_date", "channel", "is_revenue_eligible"
    )
    direct = (
        spark.table("polaris.silver.order_items")
        .join(orders, "order_id")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy("order_date", "product_id", "channel")
        .agg(
            F.sum(
                F.when(F.col("is_revenue_eligible"), F.col("eligible_quantity")).otherwise(0)
            ).alias("units_sold"),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("eligible_item_revenue")).otherwise(0))
            .cast("decimal(24,2)")
            .alias("net_revenue"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            "product_id",
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
            "units_sold",
            "net_revenue",
        )
    )
    marketplace = (
        spark.table("polaris.silver.marketplace_sales")
        .where(in_window("order_date", args.start_date, args.end_date))
        .groupBy("order_date", "shopvn_product_id", "partner")
        .agg(
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("quantity_sold")).otherwise(0)).alias(
                "units_sold"
            ),
            F.sum(F.when(F.col("is_revenue_eligible"), F.col("net_revenue")).otherwise(0))
            .cast("decimal(24,2)")
            .alias("net_revenue"),
        )
        .select(
            F.col("order_date").alias("metric_date"),
            F.col("shopvn_product_id").alias("product_id"),
            F.concat(F.lit("marketplace:"), F.col("partner")).alias("sales_channel"),
            "units_sold",
            "net_revenue",
        )
    )
    stock = (
        spark.table("polaris.silver.inventory")
        .groupBy("product_id")
        .agg(F.sum("stock_qty").alias("current_stock_qty"))
    )
    rolling = (
        Window.partitionBy("product_id", "sales_channel")
        .orderBy(F.col("metric_date").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    result = (
        direct.unionByName(marketplace)
        .join(products, "product_id", "left")
        .join(stock, "product_id", "left")
        .withColumn(
            "average_net_unit_price",
            F.when(F.col("units_sold") > 0, F.col("net_revenue") / F.col("units_sold")),
        )
        .withColumn(
            "is_selling_at_loss",
            F.col("average_net_unit_price").isNotNull()
            & (F.col("average_net_unit_price") < F.col("cost_price")),
        )
        .withColumnRenamed("is_loss_making", "is_loss_making_catalog")
        .withColumn("avg_daily_units_sold_7d", F.sum("units_sold").over(rolling) / F.lit(7.0))
        .withColumn(
            "days_to_stockout",
            F.when(
                F.col("avg_daily_units_sold_7d") > 0,
                F.col("current_stock_qty") / F.col("avg_daily_units_sold_7d"),
            ),
        )
    )
    return add_candidate_metadata(result, args)
