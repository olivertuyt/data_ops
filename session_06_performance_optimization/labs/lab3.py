import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from spark_common import build_spark, get_logger

log = get_logger(__name__)

STATES = ["SP", "RJ", "MG", "PR", "RS", "BA"]
N_CUSTOMERS = 100_000
SCD2_VERSIONS = 20


def gen_orders(spark: SparkSession, n: int):
    return (
        spark.range(n)
        .withColumn("customer_id", F.col("id") % F.lit(N_CUSTOMERS))
        .withColumn("amount", F.round(F.rand(1) * F.lit(500) + 10, 2))
        .select("id", "customer_id", "amount")
    )


def gen_customers_scd2(spark: SparkSession):
    # SCD2: each customer has SCD2_VERSIONS history rows; only the latest is current.
    return (
        spark.range(N_CUSTOMERS * SCD2_VERSIONS)
        .withColumn("customer_id", F.col("id") % F.lit(N_CUSTOMERS))
        .withColumn("version", (F.col("id") / F.lit(N_CUSTOMERS)).cast("int"))
        .withColumn("is_current", (F.col("version") == F.lit(SCD2_VERSIONS - 1)))
        .withColumn(
            "customer_state",
            F.element_at(
                F.array(*[F.lit(s) for s in STATES]),
                (F.col("customer_id") % F.lit(len(STATES)) + 1).cast("int"),
            ),
        )
        .select("customer_id", "customer_state", "is_current")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5_000_000)
    args = parser.parse_args()

    spark = build_spark(
        "lab3",
        {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.shuffle.partitions": "50",
        },
    )

    orders = gen_orders(spark, args.rows)
    customers = gen_customers_scd2(spark)

    joined = orders.join(customers, on="customer_id", how="inner")

    w = Window.partitionBy("customer_id").orderBy(F.desc("amount"))
    result = joined.withColumn("rank", F.rank().over(w)).filter(F.col("rank") == 1)

    n = result.count()
    log.info("lab3 result count: %d", n)

    print(f"\nSpark UI: {spark.sparkContext.uiWebUrl}")
    spark.stop()


if __name__ == "__main__":
    main()
