from __future__ import annotations

from lakehouse_common import build_spark, get_logger

log = get_logger("init_schema")

# Optimized writes + auto-compaction (few, large files), data-skipping stats, and the
# retention that time travel / VACUUM read from.
TBLPROPERTIES = """
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.dataSkippingNumIndexedCols = 8,
    delta.logRetentionDuration = 'interval 30 days',
    delta.deletedFileRetentionDuration = 'interval 7 days'
"""

spark = build_spark("lakehouse-init")

spark.sql("CREATE SCHEMA IF NOT EXISTS silver LOCATION 's3a://silver/'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold LOCATION 's3a://gold/'")

# device_type (Day 2) and cost_micros (Day 3) are added later by the loader (mergeSchema).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.fact_ad_events (
        event_id    STRING,
        ad_id       STRING,
        campaign_id STRING,
        user_id     STRING,
        event_type  STRING,
        event_ts    TIMESTAMP,
        event_date  DATE
    )
    USING delta
    PARTITIONED BY (event_date)
    LOCATION 's3a://silver/fact_ad_events'
    TBLPROPERTIES ({TBLPROPERTIES})
""")
log.info("Created silver.fact_ad_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.agg_ad_daily (
        event_date   DATE,
        ad_id        STRING,
        campaign_id  STRING,
        impressions  BIGINT,
        clicks       BIGINT,
        unique_users BIGINT,
        ctr          DOUBLE
    )
    USING delta
    PARTITIONED BY (event_date)
    LOCATION 's3a://gold/agg_ad_daily'
    TBLPROPERTIES ({TBLPROPERTIES})
""")
log.info("Created gold.agg_ad_daily")

spark.stop()
