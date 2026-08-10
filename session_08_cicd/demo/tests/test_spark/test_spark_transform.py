import time

from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from spark_transform import transform

SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("order_date", StringType()),
    ]
)


def test_transform_happy_case(spark):
    df = spark.createDataFrame(
        [("o1", "c1", 100.0, "2026-01-15"), ("o2", "c2", 200.0, "2026-01-15")], SCHEMA
    )
    out = transform(df, ds="2026-01-15")
    assert out.count() == 2
    assert "processed_at" in out.columns


def test_processed_at_is_deterministic(spark):
    df = spark.createDataFrame([("o1", "c1", 100.0, "2026-01-15")], SCHEMA)
    out1 = transform(df, ds="2026-01-15").collect()[0]["processed_at"]
    time.sleep(1)
    out2 = transform(df, ds="2026-01-15").collect()[0]["processed_at"]
    assert out1 == out2


def test_null_customer_id_handled(spark):
    df = spark.createDataFrame([("o1", None, 100.0, "2026-01-15")], SCHEMA)
    out = transform(df, ds="2026-01-15")
    assert out.filter(out.customer_id.isNull()).count() == 0


def test_empty_dataframe_no_crash(spark):
    df = spark.createDataFrame([], SCHEMA)
    out = transform(df, ds="2026-01-15")
    assert out is not None
    assert out.count() == 0


def test_schema_explicit_not_inferred(spark):
    df = spark.createDataFrame([("o1", "c1", None, "2026-01-15")], SCHEMA)
    out = transform(df, ds="2026-01-15")
    assert out is not None
    amount_type = dict(out.dtypes)["amount"]
    assert amount_type == "double"
