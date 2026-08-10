from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark(
    "hw1",
    {
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        "spark.sql.shuffle.partitions": "5",
        "spark.sql.adaptive.enabled": "false",
    },
)

order_items = spark.read.format("delta").load("s3a://silver/order_items")

agg = (
    order_items.groupBy("seller_id", "product_category", "product_id")
    .agg(
        F.sum("price").alias("sum_price"),
        F.count("*").alias("n"),
        F.avg("price").alias("avg_price"),
        F.max("shipping_charges").alias("max_ship"),
        F.min("shipping_charges").alias("min_ship"),
    )
)

agg.write.format("delta").mode("overwrite").save("s3a://gold/hw1_seller_product_stats")
log.info("hw1 done: wrote %s rows -> s3a://gold/hw1_seller_product_stats", f"{agg.count():,}")

spark.stop()
