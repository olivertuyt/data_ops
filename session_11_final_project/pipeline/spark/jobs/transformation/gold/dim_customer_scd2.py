"""Full SCD2 snapshot for loyalty tier, starting only at configured go-live."""

from common.config import parse_iso_date, required_env
from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata

TARGET = "dim_customer_scd2"
DOMAIN = "customer"
KEYS = ["customer_version_key"]
PARTITION = None


def _current_source(spark):
    return spark.table("polaris.silver.customers").select(
        "customer_id",
        "loyalty_tier",
        F.sha2(F.concat_ws("|", F.col("customer_id"), F.col("loyalty_tier")), 256).alias(
            "attribute_hash"
        ),
    )


def _new_versions(source, effective_date: str):
    return (
        source.withColumn("effective_from", F.lit(effective_date).cast("date"))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "customer_version_key",
            F.sha2(
                F.concat_ws(
                    "|", F.col("customer_id"), F.col("attribute_hash"), F.lit(effective_date)
                ),
                256,
            ),
        )
    )


def build(spark, args):
    go_live_date = required_env("PIPELINE_GO_LIVE_DATE")
    parse_iso_date(go_live_date)
    effective_date = max(parse_iso_date(args.end_date), parse_iso_date(go_live_date)).isoformat()
    source = _current_source(spark)
    if not spark.catalog.tableExists("polaris.serving.dim_customer_scd2"):
        return add_candidate_metadata(_new_versions(source, go_live_date), args)

    previous = spark.table("polaris.serving.dim_customer_scd2").select(
        "customer_id",
        "loyalty_tier",
        "attribute_hash",
        "effective_from",
        "effective_to",
        "is_current",
        "customer_version_key",
    )
    current_previous = previous.where(F.col("is_current")).select(
        "customer_id", F.col("attribute_hash").alias("previous_attribute_hash")
    )
    changed = (
        source.join(current_previous, "customer_id", "left")
        .where(
            F.col("previous_attribute_hash").isNull()
            | (F.col("attribute_hash") != F.col("previous_attribute_hash"))
        )
        .drop("previous_attribute_hash")
    )
    changed_ids = changed.select("customer_id").distinct().withColumn("_changed", F.lit(True))
    retained = (
        previous.join(changed_ids, "customer_id", "left")
        .withColumn(
            "effective_to",
            F.when(
                F.col("is_current") & F.col("_changed"),
                F.date_sub(F.lit(effective_date).cast("date"), 1),
            ).otherwise(F.col("effective_to")),
        )
        .withColumn(
            "is_current",
            F.when(F.col("is_current") & F.col("_changed"), F.lit(False)).otherwise(
                F.col("is_current")
            ),
        )
        .drop("_changed")
    )
    result = retained.unionByName(_new_versions(changed, effective_date))
    return add_candidate_metadata(result, args)
