from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql.functions import col, to_date

log = get_logger("raw2bronze__marketplace_orders")

EXPECTED_COLUMNS = {"order_id", "partner", "customer_email", "amount", "order_ts", "status"}

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
ds = parser.parse_args().ds

spark = build_spark("raw2bronze__marketplace_orders")

raw_path = f"s3a://bronze/orders/{ds}.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

raw_count = df.count()
if raw_count == 0:
    raise ValueError(f"No data found at {raw_path}")

actual_columns = set(df.columns)
missing = EXPECTED_COLUMNS - actual_columns
if missing:
    log.warning("Source CSV is missing expected columns: %s — they will be null in bronze", missing)

df = df.withColumn("order_date", to_date(col("order_ts")))

(
    df.repartition("order_date")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date = '{ds}'")
    .option("mergeSchema", "true")
    .save("s3a://bronze/marketplace_orders")
)
log.info("bronze.marketplace_orders loaded for %s: %d rows", ds, raw_count)

spark.stop()
