from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark("lab1", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://silver/orders")
order_items = spark.read.format("delta").load("s3a://silver/order_items")

# Report: item revenue for orders approved on July 7, 2026 (daily scheduled job).
result = (
    orders.filter(
        (F.col("order_approved_at") >= "2026-07-07") & (F.col("order_approved_at") < "2026-07-08")
    )
    .join(order_items, "order_id", "inner")
    .groupBy("product_category")
    .agg(
        F.sum("price").alias("revenue"),
        F.count("*").alias("items_sold"),
        F.countDistinct("coupon_code").alias("coupons_used"),
    )
)

result.explain("formatted")
log.info("lab1 done: %d category revenue rows", result.count())

spark.stop()
