ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/jars /opt/airflow/landing \
    && chown -R airflow:root /opt/airflow/landing \
    && chmod -R u+rwX,g+rwX /opt/airflow/landing

ARG ICEBERG_VERSION=1.10.1
ARG POSTGRES_JDBC_VERSION=42.7.5
RUN curl -fsSL -o /opt/airflow/jars/iceberg-spark-runtime.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o /opt/airflow/jars/iceberg-aws-bundle.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o /opt/airflow/jars/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && test -s /opt/airflow/jars/iceberg-spark-runtime.jar \
    && test -s /opt/airflow/jars/iceberg-aws-bundle.jar \
    && test -s /opt/airflow/jars/postgresql.jar

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
      -r /tmp/requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/opt/shopvn/spark/jobs
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/airflow/jars/iceberg-spark-runtime.jar,/opt/airflow/jars/iceberg-aws-bundle.jar,/opt/airflow/jars/postgresql.jar pyspark-shell"
