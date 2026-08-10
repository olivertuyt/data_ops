from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql.functions import col, count, countDistinct, lit, when
from pyspark.sql.functions import round as sround

log = get_logger("aggregate_ad_daily")

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

spark = build_spark("ad-aggregate-daily")

fact = spark.table("silver.fact_ad_events").filter(col("event_date") == lit(ds))

# countDistinct is exact — right at this volume; swap in approx_count_distinct at
# billions of rows to trade a few % accuracy for speed.
agg = (
    fact.groupBy("event_date", "ad_id", "campaign_id")
    .agg(
        count(when(col("event_type") == "impression", True)).alias("impressions"),
        count(when(col("event_type") == "click", True)).alias("clicks"),
        countDistinct("user_id").alias("unique_users"),
    )
    .withColumn(
        "ctr",
        when(col("impressions") > 0, sround(col("clicks") / col("impressions"), 4)).otherwise(
            lit(0.0)
        ),
    )
)

(
    agg.repartition("event_date")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{ds}'")
    .save("s3a://gold/agg_ad_daily")
)
log.info("gold.agg_ad_daily loaded for %s: %d ads", ds, agg.count())

spark.stop()
