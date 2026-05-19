FROM apache/airflow:3.2.1

USER root

# Install Java 17 (needed for PySpark local mode)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark \
    pandas \
    pyarrow \
    cloudscraper \
    beautifulsoup4 \
    elasticsearch \
    requests
