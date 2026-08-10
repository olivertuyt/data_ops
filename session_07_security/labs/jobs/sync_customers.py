import argparse

from pyspark.sql.functions import col, lit
from security_utils import get_logger, get_spark_session, get_vault_secret

# TODO (Finding 1): logging.basicConfig(level=logging.DEBUG) was removed.
# Spark at DEBUG level prints raw partition data including PII to stdout.
# security_utils.get_logger() already configures the correct log level.
logger = get_logger("customer_sync")

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

pg_secret = get_vault_secret("dataops/postgres_source")
minio_secret = get_vault_secret("dataops/minio")

spark = get_spark_session("customer-sync", minio_secret)
spark.sparkContext.setLogLevel("DEBUG")  # Finding 1: exposes PII in Spark executor logs

jdbc_url = f"jdbc:postgresql://{pg_secret['host']}:{pg_secret['port']}/{pg_secret['database']}"
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "raw.customers")
    .option("user", pg_secret["username"])
    .option("password", pg_secret["password"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

# TODO (Finding 2): df.show() prints raw customer PII to stdout.
# Remove this line entirely.
df.show(20)

flagged = df.filter(col("credit_score") < 450)
# TODO (Finding 3): logger.warning logs full_name, cccd, phone, balance — raw PII.
# Replace with a count-only log: logger.warning("Low credit score records: %d", flagged.count())
for row in flagged.collect():
    logger.warning(
        f"Low credit score record: {row['full_name']} ({row['cccd']}), "
        f"phone={row['phone']}, balance={row['account_balance']}, score={row['credit_score']}"
    )

df_synced = df.withColumn("synced_at", lit(ds).cast("date"))

(
    df_synced.write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://bronze/customers_sync")
    .saveAsTable("bronze.customers_sync")
)

logger.info("synced %d customers for %s", df_synced.count(), ds)

spark.stop()
