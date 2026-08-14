"""Small, explicit Iceberg helpers used by the reference jobs."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession


def quote_identifier(value: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split("."))


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {quote_identifier(namespace)}")


def table_exists(spark: SparkSession, table_name: str) -> bool:
    catalog, namespace, table = table_name.split(".", 2)
    return spark.catalog.tableExists(f"{catalog}.{namespace}.{table}")


def ensure_compatible_table(
    spark: SparkSession,
    table_name: str,
    source: DataFrame,
    partition_clause: str | None = None,
) -> None:
    """Create a table, add new columns, and reject silent type changes."""
    namespace = ".".join(table_name.split(".")[:2])
    ensure_namespace(spark, namespace)
    source.createOrReplaceTempView("_schema_source")
    if not table_exists(spark, table_name):
        partition = f" PARTITIONED BY ({partition_clause})" if partition_clause else ""
        spark.sql(
            f"CREATE TABLE {quote_identifier(table_name)} USING iceberg{partition} "
            "AS SELECT * FROM _schema_source WHERE 1 = 0"
        )
        return

    target_types = {field.name: field.dataType for field in spark.table(table_name).schema}
    for field in source.schema:
        if field.name not in target_types:
            spark.sql(
                f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN "
                f"{quote_identifier(field.name)} {field.dataType.simpleString()}"
            )
        elif target_types[field.name] != field.dataType:
            raise RuntimeError(
                f"Breaking schema change for {table_name}.{field.name}: "
                f"target={target_types[field.name].simpleString()}, "
                f"source={field.dataType.simpleString()}"
            )


def merge_upsert(
    spark: SparkSession,
    source: DataFrame,
    table_name: str,
    keys: Sequence[str],
    partition_clause: str | None = None,
) -> None:
    if not keys:
        raise ValueError("At least one merge key is required")
    missing = sorted(set(keys) - set(source.columns))
    if missing:
        raise ValueError(f"Missing merge columns for {table_name}: {missing}")
    ensure_compatible_table(spark, table_name, source, partition_clause)
    source.createOrReplaceTempView("_merge_source")
    condition = " AND ".join(
        f"t.{quote_identifier(key)} <=> s.{quote_identifier(key)}" for key in keys
    )
    spark.sql(
        f"MERGE INTO {quote_identifier(table_name)} t "
        f"USING _merge_source s ON {condition} "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def assert_unique_non_null(
    spark: SparkSession, table_name: str, keys: Sequence[str]
) -> None:
    key_sql = ", ".join(quote_identifier(key) for key in keys)
    null_sql = " OR ".join(f"{quote_identifier(key)} IS NULL" for key in keys)
    null_count = spark.sql(
        f"SELECT count(*) AS n FROM {quote_identifier(table_name)} WHERE {null_sql}"
    ).first()["n"]
    duplicate_count = spark.sql(
        f"SELECT count(*) AS n FROM ("
        f"SELECT {key_sql}, count(*) AS c FROM {quote_identifier(table_name)} "
        f"GROUP BY {key_sql} HAVING c > 1)"
    ).first()["n"]
    if null_count or duplicate_count:
        raise RuntimeError(
            f"Key validation failed for {table_name}: "
            f"null_keys={null_count}, duplicate_keys={duplicate_count}"
        )
