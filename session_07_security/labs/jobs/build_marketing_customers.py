import argparse

from pyspark.sql import functions as F
from security_utils import get_logger, get_spark_session, get_vault_secret

logger = get_logger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

minio_secret = get_vault_secret("dataops/minio")
spark = get_spark_session("build-marketing-customers", minio_secret)
spark.sparkContext.setLogLevel("WARN")

df = spark.table("silver.customers")

df_out = df.select(
    "customer_id",
    "full_name",  # TODO F1: not in DPO-approved requirements
    "phone",  # TODO F2: mask at query layer, not here
    "email",  # TODO F2: mask at query layer, not here
    "cccd",  # TODO F1: not in DPO-approved requirements
    "date_of_birth",  # TODO F1: not in DPO-approved requirements
    "address",  # TODO F1: not in DPO-approved requirements
    "segment",
    "region",
    "registered_at",
    "account_balance",  # TODO F1: replace with balance tier
    "credit_score",  # TODO F1: not in DPO-approved requirements
).withColumn("updated_at", F.lit(ds).cast("date"))

# TODO F3: update labs/trino_rules_patch.json so user "marketing" can only
# query gold.marketing_customers and is denied all other tables.

(
    df_out.write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://gold/marketing_customers")
    .saveAsTable("gold.marketing_customers")
)

logger.info("marketing_customers built for %s: %d rows", ds, df_out.count())
spark.stop()
