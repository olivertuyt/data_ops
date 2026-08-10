import argparse

from pyspark.sql.functions import col, lit, lower, row_number, trim
from pyspark.sql.window import Window
from security_utils import get_logger, get_spark_session, get_vault_secret, log_pii_access

logger = get_logger("transform_silver")

PII_COLUMNS = [
    "full_name",
    "phone",
    "email",
    "cccd",
    "date_of_birth",
    "address",
    "account_balance",
    "credit_score",
]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

minio_secret = get_vault_secret("dataops/minio")

spark = get_spark_session("transform-silver-customers", minio_secret)
spark.sparkContext.setLogLevel("WARN")

log_pii_access(
    job_name="transform_silver",
    table="bronze.customers",
    columns=PII_COLUMNS,
    record_count=0,
    ds=ds,
)

df_bronze = spark.table("bronze.customers").filter(col("ingested_at") == ds)

if df_bronze.count() == 0:
    raise ValueError(f"No bronze.customers rows found for ingested_at={ds}")

window = Window.partitionBy("customer_id").orderBy(col("registered_at").desc())
df_silver = (
    df_bronze.withColumn("full_name", trim(col("full_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("phone", trim(col("phone")))
    .withColumn("address", trim(col("address")))
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn", "ingested_at")
    .withColumn("updated_at", lit(ds).cast("date"))
)

# SCD-1 MERGE: one row per customer; overwrite would duplicate or drop rows from partial batches.
df_silver.createOrReplaceTempView("silver_customer_updates")
try:
    spark.sql("""
        MERGE INTO silver.customers AS target
        USING silver_customer_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
except Exception as e:
    # Only ever log the non-PII identifier set, never row-level PII values.
    logger.error(f"silver.customers merge failed for ds={ds}: {type(e).__name__}: {e}")
    raise

row_count = df_silver.count()
logger.info(f"silver.customers upserted for {ds}: {row_count} rows in batch")

log_pii_access(
    job_name="transform_silver",
    table="silver.customers",
    columns=PII_COLUMNS,
    record_count=row_count,
    ds=ds,
)

spark.stop()
