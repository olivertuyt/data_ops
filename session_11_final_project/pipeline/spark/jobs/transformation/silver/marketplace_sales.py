"""Silver marketplace sales with deterministic natural-key deduplication."""

from common.config import env_csv
from common.validation import assert_no_rows, assert_unique
from pyspark.sql import Window
from pyspark.sql import functions as F

TARGET = "marketplace_sales"
KEYS = ["partner", "external_order_id", "shopvn_product_id", "order_date"]


def build(spark, **_):
    partners = env_csv("SFTP_PARTNERS")
    window = Window.partitionBy(*KEYS).orderBy(
        F.col("_ingested_at").desc(), F.col("_source_row_hash").desc()
    )
    latest = (
        spark.table("polaris.bronze.marketplace_sales")
        .withColumn("_rank", F.row_number().over(window))
        .where(F.col("_rank") == 1)
    )
    result = (
        latest.select(
            "external_order_id",
            F.col("shopvn_product_id").cast("int").alias("shopvn_product_id"),
            "seller_sku",
            F.lower("partner").alias("partner"),
            F.to_date("order_date").alias("order_date"),
            F.col("quantity_sold").cast("long").alias("quantity_sold"),
            F.col("sale_price").cast("decimal(18,2)").alias("sale_price"),
            F.col("platform_discount").cast("decimal(18,2)").alias("platform_discount"),
            F.col("commission_rate").cast("decimal(9,6)").alias("commission_rate"),
            F.col("net_revenue").cast("decimal(18,2)").alias("net_revenue"),
            F.to_date("settlement_date").alias("settlement_date"),
            F.lower("status").alias("marketplace_status"),
            "_source_file",
            "_source_row_hash",
            F.col("_run_id").alias("_source_run_id"),
        )
        .withColumn(
            "calculated_net_revenue",
            F.floor(
                F.col("sale_price").cast("double")
                * F.col("quantity_sold")
                * (F.lit(1.0) - F.col("commission_rate").cast("double"))
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "is_net_revenue_mismatch",
            F.col("net_revenue") != F.col("calculated_net_revenue"),
        )
        .withColumn("is_revenue_eligible", F.col("marketplace_status") == "completed")
        .withColumn("_silver_updated_at", F.current_timestamp())
    )
    assert_no_rows(
        result,
        "marketplace.required_or_cast",
        F.col("external_order_id").isNull()
        | F.col("shopvn_product_id").isNull()
        | F.col("partner").isNull()
        | F.col("order_date").isNull()
        | F.col("quantity_sold").isNull()
        | F.col("net_revenue").isNull(),
    )
    assert_no_rows(result, "marketplace.partner", ~F.col("partner").isin(*partners))
    assert_no_rows(
        result,
        "marketplace.status",
        ~F.col("marketplace_status").isin("completed", "returned", "disputed"),
    )
    assert_no_rows(result, "marketplace.revenue_formula", F.col("is_net_revenue_mismatch"))
    assert_unique(result, TARGET, KEYS)
    return result
