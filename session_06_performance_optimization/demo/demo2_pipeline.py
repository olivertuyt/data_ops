import os

from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
AQE = os.environ.get("AQE", "false")

# shuffle.partitions copied from a much larger production cluster.
spark = build_spark(
    f"demo2-aqe-{AQE}",
    {
        "spark.sql.adaptive.enabled": AQE,
        "spark.sql.adaptive.coalescePartitions.enabled": AQE,
        "spark.sql.shuffle.partitions": "2000",
    },
)

orders = spark.read.format("delta").load("s3a://silver/orders")
order_items = spark.read.format("delta").load("s3a://silver/order_items")

recent = orders.filter(F.col("order_purchase_timestamp") >= "2026-07-19")
result = (
    recent.join(order_items, "order_id")
    .groupBy("order_status", "product_category")
    .agg(F.sum("price").alias("gmv"))
)

result.write.format("delta").mode("overwrite").save("s3a://gold/demo2_status_category_gmv")
log.info(
    "demo2 (AQE=%s) done: %d rows -> s3a://gold/demo2_status_category_gmv", AQE, result.count()
)
log.info("Re-run with `-e AQE=true` and compare the same stage at http://localhost:18080")

spark.stop()
