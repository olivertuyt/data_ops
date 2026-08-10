from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from security_utils import get_logger, get_spark_session, get_vault_secret

logger = get_logger("init_schema")
minio_secret = get_vault_secret("dataops/minio")

spark = get_spark_session("security-demo-init", minio_secret)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://bronze/'")
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://silver/'")
spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://gold/'")

bronze_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("account_balance", DecimalType(18, 2), True),
        StructField("credit_score", IntegerType(), True),
        StructField("ingested_at", DateType(), False),
    ]
)

silver_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("account_balance", DecimalType(18, 2), True),
        StructField("credit_score", IntegerType(), True),
        StructField("updated_at", DateType(), False),
    ]
)

gold_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("balance_tier", StringType(), True),
        StructField("credit_tier", StringType(), True),
        StructField("updated_at", DateType(), False),
    ]
)

(
    spark.createDataFrame([], bronze_schema)
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("ingested_at")
    .option("path", "s3a://bronze/customers")
    .saveAsTable("bronze.customers")
)
logger.info("Created bronze.customers")

(
    spark.createDataFrame([], silver_schema)
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://silver/customers")
    .saveAsTable("silver.customers")
)
logger.info("Created silver.customers")

(
    spark.createDataFrame([], gold_schema)
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("updated_at")
    .option("path", "s3a://gold/customer_analytics")
    .saveAsTable("gold.customer_analytics")
)
logger.info("Created gold.customer_analytics")

spark.stop()
