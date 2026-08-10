from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark("hw2", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://silver/orders")
order_items = spark.read.format("delta").load("s3a://silver/order_items")
payments = spark.read.format("delta").load("s3a://silver/payments")


@udf(returnType=DoubleType())
def risk_score(payment_type, installments, value):
    base = {"credit_card": 30.0, "debit_card": 10.0, "voucher": 5.0, "boleto": 15.0}.get(
        payment_type, 0.0
    )
    return float(base + installments * 2.0 + (value or 0.0) * 0.01)


enriched = order_items.join(orders, "order_id").join(payments, "order_id")

result = (
    enriched
    .withColumn("score", risk_score(F.col("payment_type"), F.col("payment_installments"), F.col("payment_value")))
    .groupBy("order_status")
    .agg(F.sum("score").alias("total_risk_score"))
)

result.show()
log.info("hw2 done: %d rows", result.count())

spark.stop()
