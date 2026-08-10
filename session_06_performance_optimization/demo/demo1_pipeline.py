from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark(
    "demo1",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "200",
    },
)

orders = spark.read.format("delta").load("s3a://silver/orders")
order_items = spark.read.format("delta").load("s3a://silver/order_items")
customers = spark.read.format("delta").load("s3a://silver/customers")

enriched = orders.join(order_items, "order_id").join(customers, "customer_id")
result = enriched.groupBy("customer_state").agg(F.sum("price").alias("gmv"))

result.write.format("delta").mode("overwrite").save("s3a://gold/demo1_state_gmv")
log.info("demo1 done: %d state rows -> s3a://gold/demo1_state_gmv", result.count())
log.info("Inspect the run at http://localhost:18080 -> Stages -> Summary Metrics")

spark.stop()
