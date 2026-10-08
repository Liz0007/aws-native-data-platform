FROM apache/airflow:3.3.2

# Some dbt dependencies (e.g. mmh3) have no prebuilt wheel on ARM64,
# so pip compiles them. The base image has no C compiler.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# dbt lives in its own virtualenv so its pins cannot conflict with Airflow's.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN grep -qiE '^dbt-core==' /tmp/requirements-dbt.txt \
    || (echo "requirements-dbt.txt must pin dbt-core==<version>" && exit 1)
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Amazon provider (GlueJobOperator, S3KeySensor) is installed against Airflow's own constraints.
ARG AIRFLOW_VERSION=3.3.2
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-amazon