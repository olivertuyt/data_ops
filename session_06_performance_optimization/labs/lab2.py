from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark("lab2", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://silver/lab2_orders")

# Daily operations report: orders and unique customers per day and status.
result = orders.groupBy("order_date", "order_status").agg(
    F.count("*").alias("orders"),
    F.countDistinct("customer_id").alias("customers"),
)

log.info("lab2 done: %d report rows", result.count())

spark.stop()