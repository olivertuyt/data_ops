# Custom Airflow image for the DataOps course.
# Base = the exact Airflow the course uses; the only additions are what session 4
# needs: the DuckDB driver (>=1.4.0 for MERGE INTO) and the DuckDB provider, so
# DAGs can talk to DuckDB through SQLExecuteQueryOperator / the common-sql check
# operators. DAG code and the warehouse file are mounted as volumes, not baked in.
FROM apache/airflow:2.9.3

RUN pip install --no-cache-dir \
        duckdb==1.5.4 \
        airflow-provider-duckdb==0.2.0
