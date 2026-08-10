FROM apache/airflow:2.9.3-python3.11

USER root
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
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-hashicorp \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.5.0 \
    hvac==2.3.0 \
    trino==0.328.0 \
    psycopg2-binary==2.9.9 \
    minio==7.2.7

RUN pip install --no-cache-dir \
    great-expectations==0.18.19 \
    duckdb==1.5.5 \
    airflow-provider-duckdb==0.2.0
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
