from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame, ds: str) -> DataFrame:
    return df.filter(
        F.col("amount") > 0
        # TODO Bug3: NULL customer_id rows are not filtered
        # Fix: add & F.col("customer_id").isNotNull()
    ).withColumn(
        "processed_at",
        F.current_timestamp(),
        # TODO Bug1: non-deterministic — retrying produces different processed_at values
        # Fix: replace F.current_timestamp() with F.lit(ds).cast("timestamp")
    )
