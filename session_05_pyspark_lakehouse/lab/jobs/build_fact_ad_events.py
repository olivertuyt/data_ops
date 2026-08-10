from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, to_date

log = get_logger("build_fact_ad_events")

ID_COLUMNS = ["event_id", "ad_id", "campaign_id", "user_id"]
KNOWN_COLUMNS = ID_COLUMNS + ["event_type", "event_ts", "device_type", "cost_micros"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

spark = build_spark("ad-build-fact")

raw_path = f"s3a://bronze/ad_events/{ds}.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
if df.count() == 0:
    raise ValueError(f"No data found at {raw_path}")

# 1) normalize id columns -> string (giữ type ổn định khi Day 3 đổi campaign_id sang string)
for c in ID_COLUMNS:
    df = df.withColumn(c, col(c).cast("string"))

# 2) chỉ giữ impression/click, thêm event_date
df = df.filter(col("event_type").isin("impression", "click"))
df = df.withColumn("event_date", to_date(col("event_ts")))

# 3) dedup theo event_id, giữ event_ts sớm nhất (retry gửi trùng -> đếm 2 lần là sai)
w = Window.partitionBy("event_id").orderBy(col("event_ts").asc())
df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

# 4) chỉ ghi các cột đã biết (cột lạ ngoài ý muốn bị drop)
df = df.select([c for c in KNOWN_COLUMNS if c in df.columns] + ["event_date"])

# 5) idempotent load + mergeSchema để nhận cột mới (Day 2/3)
(
    df.repartition("event_date")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{ds}'")
    .option("mergeSchema", "true")
    .save("s3a://silver/fact_ad_events")
)
log.info("silver.fact_ad_events loaded for %s: %d rows", ds, df.count())

spark.stop()
