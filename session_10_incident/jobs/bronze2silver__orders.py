from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, sha2

log = get_logger("bronze2silver__orders")

VALID_STATUSES = {"completed", "refunded", "pending"}

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
ds = parser.parse_args().ds

spark = build_spark("bronze2silver__orders")

df = (
    spark.read.format("delta")
    .load("s3a://bronze/marketplace_orders")
    .filter(col("order_date") == lit(ds))
)

bronze_count = df.count()
if bronze_count == 0:
    raise ValueError(f"bronze.marketplace_orders is empty for {ds}")

# Deduplicate on order_id, keep earliest
window = Window.partitionBy("order_id").orderBy(col("order_ts").asc())
df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

df = df.filter(col("status").isin(*VALID_STATUSES))

# Mask PII — store hash of email, not raw value
if "customer_email" in df.columns:
    df = df.withColumn("customer_email_masked", sha2(col("customer_email"), 256)).drop(
        "customer_email"
    )

df = df.select(
    col("order_id"),
    col("partner"),
    col("amount").cast("double"),
    col("order_date"),
    col("status"),
    col("customer_email_masked"),
)

(
    df.repartition("order_date")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date = '{ds}'")
    .save("s3a://silver/orders")
)
log.info("silver.orders loaded for %s: %d rows (from %d bronze)", ds, df.count(), bronze_count)

spark.stop()
