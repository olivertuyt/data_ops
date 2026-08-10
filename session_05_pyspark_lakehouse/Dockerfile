# Airflow image for the lakehouse session. Adds Java 11 + a bundled Spark (via
# pyspark) so SparkSubmitOperator can submit the transform job to the standalone
# Spark master in client mode, plus the MinIO and Trino clients used by the DAG.
FROM apache/airflow:2.9.0-python3.8

USER root
# Temurin 11 matches the apache/spark:3.5.0 executors' JVM, so driver and executor
# Java agree and no Java-17 --add-opens workarounds are needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg ca-certificates \
    && mkdir -p /etc/apt/keyrings \
    && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
       | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" \
       > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends temurin-11-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # Resolve JAVA_HOME independent of CPU arch (amd64/arm64).
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
# requirements_local.txt = pyspark (ships spark-submit + the Spark 3.5.0 jars) and
# the minio/trino clients the validate/reconcile tasks use. The Spark provider gives
# the SparkSubmitOperator and pulls in Airflow, so it is image-only (not in the
# local requirements).
COPY requirements_local.txt /tmp/requirements_local.txt
RUN pip install --no-cache-dir \
    -r /tmp/requirements_local.txt \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-openlineage==1.8.0
ENV SPARK_HOME=/home/airflow/.local/lib/python3.8/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
# OpenLineage Spark listener must be in $SPARK_HOME/jars/ (bootstrap classpath).
# Adding it via --jars or spark.jars loads it too late for spark.extraListeners.
RUN wget -q -O "${SPARK_HOME}/jars/openlineage-spark_2.12-1.9.1.jar" \
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.9.1/openlineage-spark_2.12-1.9.1.jar"
