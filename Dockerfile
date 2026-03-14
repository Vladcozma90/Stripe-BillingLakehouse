FROM apache/airflow:3.1.7

USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY . /opt/airflow/app
WORKDIR /opt/airflow/app

RUN uv pip install .

USER airflow