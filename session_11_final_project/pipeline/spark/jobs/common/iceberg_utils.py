import os

def create_namespace_if_needed(spark, namespace):
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {namespace}"
    )


def create_table_if_needed(spark, table_name, df):
    if not spark.catalog.tableExists(table_name):
        (
            df.limit(0)
            .writeTo(table_name)
            .using("iceberg")
            .create()
        )


def merge_into_iceberg(spark, target_table, source_df, merge_condition):
    temp_view = "bronze_source"

    source_df.createOrReplaceTempView(temp_view)

    columns = source_df.columns

    update_set = ", ".join(
        f"target.`{column}` = source.`{column}`"
        for column in columns
    )

    insert_columns = ", ".join(
        f"`{column}`"
        for column in columns
    )

    insert_values = ", ".join(
        f"source.`{column}`"
        for column in columns
    )

    sql = f"""
    MERGE INTO {target_table} AS target
    USING {temp_view} AS source
    ON {merge_condition}

    WHEN MATCHED THEN UPDATE SET
        {update_set}

    WHEN NOT MATCHED THEN INSERT
        ({insert_columns})
    VALUES
        ({insert_values})
    """

    spark.sql(sql)