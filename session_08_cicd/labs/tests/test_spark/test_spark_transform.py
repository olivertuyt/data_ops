import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("order_date", StringType()),
    ]
)


def test_transform_happy_case(spark):
    # TODO: create a 2-row DataFrame, call transform
    # assert count == 2 and "processed_at" in columns
    raise NotImplementedError


def test_processed_at_is_deterministic(spark):
    # Bug1: current_timestamp() changes on every call — retrying a failed run produces different processed_at values
    # TODO: call transform twice with 1-second sleep in between
    # assert both processed_at values are equal (literal ds, not now())
    raise NotImplementedError


def test_null_customer_id_handled(spark):
    # Bug3: transform does not drop NULL customer_id rows
    # TODO: create row with customer_id=None, assert output has 0 rows with null customer_id
    raise NotImplementedError


def test_empty_dataframe_no_crash(spark):
    # TODO: call transform on empty DataFrame, assert count == 0
    raise NotImplementedError


def test_schema_explicit_not_inferred(spark):
    # TODO: create row with amount=None using explicit SCHEMA
    # assert dict(out.dtypes)["amount"] == "double"
    raise NotImplementedError
