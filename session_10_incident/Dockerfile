FROM apache/airflow:2.9.0-python3.8

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
COPY requirements_local.txt /tmp/requirements_local.txt
RUN pip install --no-cache-dir \
    -r /tmp/requirements_local.txt \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-openlineage==1.8.0 \
    apache-airflow-providers-trino==4.3.2
ENV SPARK_HOME=/home/airflow/.local/lib/python3.8/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
RUN wget -q -O "${SPARK_HOME}/jars/openlineage-spark_2.12-1.22.0.jar" \
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.22.0/openlineage-spark_2.12-1.22.0.jar"
