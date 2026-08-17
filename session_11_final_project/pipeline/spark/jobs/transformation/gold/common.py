"""Technical helpers shared by Gold candidate builders."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def in_window(column: str, start_date: str, end_date: str):
    return F.col(column).between(F.lit(start_date).cast("date"), F.lit(end_date).cast("date"))


def add_candidate_metadata(frame: DataFrame, args) -> DataFrame:
    return (
        frame.withColumn("run_id", F.lit(args.run_id))
        .withColumn("source_window_start", F.lit(args.start_date).cast("date"))
        .withColumn("source_window_end", F.lit(args.end_date).cast("date"))
        .withColumn("candidate_created_at", F.current_timestamp())
    )


def date_spine(spark, start_date: str, end_date: str, column_name: str = "metric_date"):
    return spark.sql(
        f"SELECT explode(sequence(DATE '{start_date}', DATE '{end_date}', "
        f"interval 1 day)) AS {column_name}"
    )
