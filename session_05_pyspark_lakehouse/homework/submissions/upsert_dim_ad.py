from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql import functions as F

log = get_logger("upsert_dim_ad")

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
ds = parser.parse_args().ds

spark = build_spark("upsert-dim-ad")

TARGET = "s3a://gold/dim_ad"

spark.sql("CREATE SCHEMA IF NOT EXISTS gold LOCATION 's3a://gold/'")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.dim_ad (
        ad_id           STRING,
        campaign_id     STRING,
        first_seen_date DATE,
        last_active_date DATE,
        latest_ctr      DOUBLE
    )
    USING delta
    LOCATION '{TARGET}'
""")

source = (
    spark.read.format("delta")
    .load("s3a://gold/agg_ad_daily")
    .filter(F.col("event_date") == F.lit(ds).cast("date"))
    .select("ad_id", "campaign_id", F.col("ctr"), F.lit(ds).cast("date").alias("ds"))
)

try:
    spark.read.format("delta").load(TARGET).limit(1).count()
    table_exists = True
except Exception:
    table_exists = False

if not table_exists:
    (
        source.select(
            F.col("ad_id"),
            F.col("campaign_id"),
            F.col("ds").alias("first_seen_date"),
            F.col("ds").alias("last_active_date"),
            F.col("ctr").alias("latest_ctr"),
        )
        .write.format("delta")
        .save(TARGET)
    )
else:
    source.createOrReplaceTempView("_dim_ad_source")
    spark.sql(f"""
        MERGE INTO delta.`{TARGET}` AS t
        USING _dim_ad_source AS s
        ON t.ad_id = s.ad_id
        WHEN MATCHED THEN UPDATE SET
            t.campaign_id      = s.campaign_id,
            t.first_seen_date  = LEAST(t.first_seen_date, s.ds),
            t.last_active_date = GREATEST(t.last_active_date, s.ds),
            t.latest_ctr       = s.ctr
        WHEN NOT MATCHED THEN INSERT (ad_id, campaign_id, first_seen_date, last_active_date, latest_ctr)
            VALUES (s.ad_id, s.campaign_id, s.ds, s.ds, s.ctr)
    """)

count = spark.read.format("delta").load(TARGET).count()
log.info("gold.dim_ad upserted for %s: %d rows total", ds, count)
spark.stop()
