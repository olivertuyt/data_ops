import argparse

from pyspark.sql.functions import col, lit, when
from security_utils import get_logger, get_spark_session, get_vault_secret, log_pii_access

logger = get_logger("transform_gold")

PII_COLUMNS = ["phone", "email", "cccd"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

minio_secret = get_vault_secret("dataops/minio")

spark = get_spark_session("transform-gold-customer-analytics", minio_secret)
spark.sparkContext.setLogLevel("WARN")

log_pii_access(
    job_name="transform_gold",
    table="silver.customers",
    columns=PII_COLUMNS,
    record_count=0,
    ds=ds,
)

df_silver = spark.table("silver.customers")

if df_silver.count() == 0:
    raise ValueError("silver.customers is empty, cannot build gold.customer_analytics")

df_gold = (
    df_silver.withColumn(
        "balance_tier",
        when(col("account_balance") < 50_000_000, "low")
        .when(col("account_balance") < 200_000_000, "medium")
        .otherwise("high"),
    )
    .withColumn(
        "credit_tier",
        when(col("credit_score") < 580, "poor")
        .when(col("credit_score") < 670, "fair")
        .when(col("credit_score") < 740, "good")
        .when(col("credit_score") < 800, "very_good")
        .otherwise("excellent"),
    )
    .withColumn("updated_at", lit(ds).cast("date"))
    .select(
        "customer_id",
        "segment",
        "region",
        "registered_at",
        "phone",
        "email",
        "cccd",
        "balance_tier",
        "credit_tier",
        "updated_at",
    )
)

# same SCD-1 MERGE as silver.
df_gold.createOrReplaceTempView("gold_customer_updates")
try:
    spark.sql("""
        MERGE INTO gold.customer_analytics AS target
        USING gold_customer_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
except Exception as e:
    logger.error(f"gold.customer_analytics merge failed for ds={ds}: {type(e).__name__}: {e}")
    raise

row_count = df_gold.count()
logger.info(f"gold.customer_analytics upserted for {ds}: {row_count} rows in batch")

log_pii_access(
    job_name="transform_gold",
    table="gold.customer_analytics",
    columns=PII_COLUMNS,
    record_count=row_count,
    ds=ds,
)

spark.stop()
