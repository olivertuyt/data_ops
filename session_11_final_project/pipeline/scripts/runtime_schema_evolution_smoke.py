"""Exercise additive and breaking schema evolution against the live Iceberg catalog."""

from __future__ import annotations

from common.iceberg import ensure_compatible_table, merge_upsert
from common.spark_session import create_spark_session
from pyspark.sql import types as T

TABLE = "polaris.runtime_smoke.schema_evolution_20260815"


def main() -> None:
    spark = create_spark_session("shopvn-runtime-schema-evolution")
    try:
        base_schema = T.StructType(
            [
                T.StructField("record_id", T.StringType(), False),
                T.StructField("amount", T.LongType(), False),
            ]
        )
        additive_schema = T.StructType(
            [
                *base_schema.fields,
                T.StructField("new_nullable_note", T.StringType(), True),
            ]
        )
        if not spark.catalog.tableExists(TABLE):
            merge_upsert(
                spark,
                spark.createDataFrame([("base", 1)], base_schema),
                TABLE,
                ["record_id"],
            )
        merge_upsert(
            spark,
            spark.createDataFrame([("additive", 2, "accepted")], additive_schema),
            TABLE,
            ["record_id"],
        )

        incompatible_schema = T.StructType(
            [
                T.StructField("record_id", T.StringType(), False),
                T.StructField("amount", T.StringType(), False),
                T.StructField("new_nullable_note", T.StringType(), True),
            ]
        )
        incompatible = spark.createDataFrame(
            [("breaking", "not-a-long", None)], incompatible_schema
        )
        try:
            ensure_compatible_table(spark, TABLE, incompatible)
        except RuntimeError as exc:
            if "Breaking schema change" not in str(exc):
                raise
        else:
            raise RuntimeError("Incompatible type change was not blocked")

        table = spark.table(TABLE)
        amount_type = table.schema["amount"].dataType
        if not isinstance(amount_type, T.LongType):
            raise RuntimeError(f"Schema was corrupted after blocked change: {amount_type}")
        row_count = table.where("record_id IN ('base', 'additive')").count()
        if row_count != 2:
            raise RuntimeError(f"Unexpected schema smoke row count: {row_count}")
        print("SCHEMA_EVOLUTION_PASS additive=accepted incompatible=blocked rows=2")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
