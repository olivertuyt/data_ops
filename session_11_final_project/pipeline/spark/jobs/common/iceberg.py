"""Explicit Iceberg schema and idempotent write helpers."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession


def quote_identifier(value: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split("."))


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {quote_identifier(namespace)}")


def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def ensure_compatible_table(
    spark: SparkSession,
    table_name: str,
    source: DataFrame,
    partition_clause: str | None = None,
) -> None:
    """Create a table, allow additive columns, and reject removals/type changes."""
    ensure_namespace(spark, ".".join(table_name.split(".")[:2]))
    source.createOrReplaceTempView("_schema_source")
    if not table_exists(spark, table_name):
        partition = f" PARTITIONED BY ({partition_clause})" if partition_clause else ""
        # Dynamic values are quoted identifiers or code-owned partition constants.
        spark.sql(
            f"CREATE TABLE {quote_identifier(table_name)} USING iceberg{partition} "  # nosec B608
            "AS SELECT * FROM _schema_source WHERE 1 = 0"
        )
        return

    target_types = {field.name: field.dataType for field in spark.table(table_name).schema}
    source_types = {field.name: field.dataType for field in source.schema}
    removed = sorted(set(target_types) - set(source_types))
    if removed:
        raise RuntimeError(
            f"Breaking schema change for {table_name}: missing source columns {removed}"
        )
    for name, data_type in source_types.items():
        if name not in target_types:
            spark.sql(
                f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN "
                f"{quote_identifier(name)} {data_type.simpleString()}"
            )
        elif target_types[name] != data_type:
            raise RuntimeError(
                f"Breaking schema change for {table_name}.{name}: "
                f"target={target_types[name].simpleString()}, "
                f"source={data_type.simpleString()}"
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
    # The table and all key identifiers are quoted before SQL construction.
    spark.sql(
        f"MERGE INTO {quote_identifier(table_name)} t "  # nosec B608
        f"USING _merge_source s ON {condition} "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def assert_unique_non_null(frame: DataFrame, dataset: str, keys: Sequence[str]) -> None:
    null_condition = None
    for key in keys:
        current = frame[key].isNull()
        null_condition = current if null_condition is None else null_condition | current
    null_count = frame.where(null_condition).limit(1).count()
    duplicate_count = frame.groupBy(*keys).count().where("count > 1").limit(1).count()
    if null_count or duplicate_count:
        raise RuntimeError(
            f"Key validation failed for {dataset}: "
            f"null_key_groups={null_count}, duplicate_key_groups={duplicate_count}"
        )
