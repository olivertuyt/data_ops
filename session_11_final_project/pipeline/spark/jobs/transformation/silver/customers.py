"""Silver customers: deterministic PII hashing and coarse location only."""

from common.validation import assert_no_rows, assert_unique
from pyspark.sql import functions as F

TARGET = "customers"
KEYS = ["customer_id"]


def _hash(column_name: str, salt: str):
    return F.sha2(F.concat(F.lit(salt), F.lit("|"), F.col(column_name).cast("string")), 256)


def build(spark, *, pii_salt: str):
    source = spark.table("polaris.bronze.customers")
    result = source.select(
        "customer_id",
        _hash("full_name", pii_salt).alias("full_name_hash"),
        F.when(F.col("phone").isNull(), F.lit(None))
        .otherwise(_hash("phone", pii_salt))
        .alias("phone_hash"),
        _hash("email", pii_salt).alias("email_hash"),
        "city",
        "district",
        F.lower("gender").alias("gender"),
        F.to_date("date_of_birth").alias("date_of_birth"),
        F.lower("loyalty_tier").alias("loyalty_tier"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("phone").isNull().alias("is_phone_missing"),
        F.col("_run_id").alias("_source_run_id"),
        F.current_timestamp().alias("_silver_updated_at"),
    )
    assert_no_rows(
        result,
        "customers.required",
        F.col("customer_id").isNull()
        | F.col("full_name_hash").isNull()
        | F.col("email_hash").isNull(),
    )
    assert_no_rows(
        result,
        "customers.loyalty_tier",
        ~F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
    )
    assert_unique(result, TARGET, KEYS)
    return result
