"""Write a deterministic Iceberg probe through Spark for cross-engine validation."""

from __future__ import annotations

from common.iceberg import merge_upsert
from common.spark_session import create_spark_session
from pyspark.sql import types as T

TABLE = "polaris.runtime_smoke.catalog_compatibility"
PROBE_ID = "spark-to-trino"
PROBE_VALUE = 20260815


def main() -> None:
    spark = create_spark_session("shopvn-runtime-catalog-smoke")
    try:
        schema = T.StructType(
            [
                T.StructField("probe_id", T.StringType(), False),
                T.StructField("probe_value", T.LongType(), False),
            ]
        )
        frame = spark.createDataFrame([(PROBE_ID, PROBE_VALUE)], schema)
        merge_upsert(spark, frame, TABLE, ["probe_id"])
        result = spark.table(TABLE).where(f"probe_id = '{PROBE_ID}'").collect()
        if len(result) != 1 or result[0]["probe_value"] != PROBE_VALUE:
            raise RuntimeError(f"Unexpected Spark catalog probe result: {result}")
        print(f"SPARK_CATALOG_WRITE_PASS rows={len(result)} sum={result[0]['probe_value']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
