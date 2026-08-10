from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame, ds: str) -> DataFrame:
    return (
        df.filter(F.col("customer_id").isNotNull())
        .filter(F.col("amount") > 0)
        .withColumn("processed_at", F.lit(ds))
    )
