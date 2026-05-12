FROM apache/airflow:3.1.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY . /opt/airflow/app
RUN chown -R airflow:root /opt/airflow/app

WORKDIR /opt/airflow/app

USER airflow

RUN uv pip install .
RUN uv pip install apache-airflow-providers-databricks==7.13.0 apache-airflow-providers-common-sql==1.35.0