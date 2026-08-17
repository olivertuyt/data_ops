"""Direct-channel product rating metrics; marketplace ratings do not exist."""

from pyspark.sql import functions as F

from transformation.gold.common import add_candidate_metadata

TARGET = "fact_product_rating_daily"
DOMAIN = "customer"
KEYS = ["metric_date", "category", "sales_channel"]
PARTITION = "days(metric_date)"
# Reviews are sparse events. A valid logical window can contain no reviews, and
# marketplace ratings must never be fabricated to make the candidate non-empty.
ALLOW_EMPTY = True


def build(spark, args):
    reviews = spark.table("polaris.silver.product_reviews")
    products = spark.table("polaris.silver.products").select("product_id", "category")
    orders = spark.table("polaris.silver.direct_orders").select("order_id", "channel")
    result = (
        reviews.join(products, "product_id")
        .join(orders, "order_id")
        .where(
            F.to_date("created_at").between(
                F.lit(args.start_date).cast("date"), F.lit(args.end_date).cast("date")
            )
        )
        .groupBy(
            F.to_date("created_at").alias("metric_date"),
            "category",
            F.concat(F.lit("direct:"), F.col("channel")).alias("sales_channel"),
        )
        .agg(
            F.avg("rating").alias("average_rating"),
            F.count("review_id").alias("review_count"),
        )
    )
    return add_candidate_metadata(result, args)
