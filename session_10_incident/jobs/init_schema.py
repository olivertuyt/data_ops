from __future__ import annotations

from lakehouse_common import build_spark, get_logger

log = get_logger("init_schema")

TBLPROPERTIES = """
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.dataSkippingNumIndexedCols = 8,
    delta.logRetentionDuration = 'interval 30 days',
    delta.deletedFileRetentionDuration = 'interval 7 days'
"""

spark = build_spark("orders-init-schema")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze LOCATION 's3a://bronze/'")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver LOCATION 's3a://silver/'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold LOCATION 's3a://gold/'")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.marketplace_orders (
        order_id       STRING,
        partner        STRING,
        customer_email STRING,
        amount         DOUBLE,
        order_ts       TIMESTAMP,
        status         STRING,
        order_date     DATE
    )
    USING delta
    PARTITIONED BY (order_date)
    LOCATION 's3a://bronze/marketplace_orders'
    TBLPROPERTIES ({TBLPROPERTIES})
""")
log.info("Registered bronze.marketplace_orders")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.orders (
        order_id              STRING,
        partner               STRING,
        amount                DOUBLE,
        status                STRING,
        customer_email_masked STRING,
        order_date            DATE
    )
    USING delta
    PARTITIONED BY (order_date)
    LOCATION 's3a://silver/orders'
    TBLPROPERTIES ({TBLPROPERTIES})
""")
log.info("Registered silver.orders")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.fact_daily_revenue (
        partner       STRING,
        order_count   BIGINT,
        total_revenue DOUBLE,
        order_date    DATE
    )
    USING delta
    PARTITIONED BY (order_date)
    LOCATION 's3a://gold/fact_daily_revenue'
    TBLPROPERTIES ({TBLPROPERTIES})
""")
log.info("Registered gold.fact_daily_revenue")

spark.stop()
