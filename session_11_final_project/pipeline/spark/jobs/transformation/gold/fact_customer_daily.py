"""Daily customer snapshot with spend, repeat-purchase, and inactivity lifecycle."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata, date_spine

TARGET = "fact_customer_daily"
DOMAIN = "customer"
KEYS = ["metric_date", "customer_id"]
PARTITION = "days(metric_date)"


def build(spark, args):
    customers = spark.table("polaris.silver.customers").select(
        "customer_id", "loyalty_tier", "city", "district"
    )
    eligible = spark.table("polaris.silver.direct_orders").where(F.col("is_revenue_eligible"))
    first_orders = eligible.groupBy("customer_id").agg(
        F.min("order_date").alias("customer_first_order_date")
    )
    pre_window = (
        eligible.where(F.col("order_date") < F.lit(args.start_date).cast("date"))
        .groupBy("customer_id")
        .agg(F.max("order_date").alias("last_order_before_window"))
    )
    daily = (
        eligible.where(F.col("order_date") <= F.lit(args.end_date).cast("date"))
        .groupBy(F.col("order_date").alias("metric_date"), "customer_id")
        .agg(
            F.sum("eligible_net_revenue").cast("decimal(24,2)").alias("daily_net_spend"),
            F.countDistinct("order_id").alias("daily_order_count"),
        )
    )
    grid = (
        customers.crossJoin(date_spine(spark, args.start_date, args.end_date))
        .join(first_orders, "customer_id", "left")
        .join(pre_window, "customer_id", "left")
        .join(daily, ["metric_date", "customer_id"], "left")
        .fillna({"daily_net_spend": 0, "daily_order_count": 0})
    )
    history = (
        Window.partitionBy("customer_id")
        .orderBy("metric_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    prior = (
        Window.partitionBy("customer_id")
        .orderBy("metric_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    month_to_date = (
        Window.partitionBy("customer_id", F.year("metric_date"), F.month("metric_date"))
        .orderBy("metric_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    result = (
        grid.withColumn(
            "prior_order_in_window",
            F.max(F.when(F.col("daily_order_count") > 0, F.col("metric_date"))).over(prior),
        )
        .withColumn(
            "prior_order_date",
            F.coalesce("prior_order_in_window", "last_order_before_window"),
        )
        .withColumn(
            "last_order_in_window",
            F.max(F.when(F.col("daily_order_count") > 0, F.col("metric_date"))).over(history),
        )
        .withColumn(
            "last_order_date",
            F.coalesce("last_order_in_window", "last_order_before_window"),
        )
        .withColumn("month_to_date_spend", F.sum("daily_net_spend").over(month_to_date))
        .withColumn(
            "is_repeat_within_30_days",
            (F.col("daily_order_count") > 0)
            & F.col("prior_order_date").isNotNull()
            & (F.datediff("metric_date", "prior_order_date") <= 30),
        )
        .withColumn(
            "lifecycle_stage",
            F.when(F.col("last_order_date").isNull(), F.lit("never_purchased"))
            .when(
                (F.col("daily_order_count") > 0)
                & (F.col("metric_date") == F.col("customer_first_order_date")),
                F.lit("first_order"),
            )
            .when(F.col("is_repeat_within_30_days"), F.lit("repeat_30d"))
            .when(F.datediff("metric_date", "last_order_date") > 30, F.lit("inactive"))
            .otherwise(F.lit("active")),
        )
        .withColumn("days_since_last_order", F.datediff("metric_date", "last_order_date"))
        .drop("prior_order_in_window", "last_order_in_window", "last_order_before_window")
    )
    return add_candidate_metadata(result, args)
