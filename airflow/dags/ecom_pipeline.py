"""Bronze -> silver (Glue/Iceberg) -> gold (dbt on Athena), orchestrated by Airflow.

Created paused: unpause it deliberately. Retries repeat *any* failure, including
deterministic data-quality failures from the Glue job; those will fail 3 times
before surfacing. Accepted trade-off for transient Glue errors.
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set; run scripts/make_airflow_env.sh")
    return value


BUCKET = _env("LAKE_BUCKET")
GLUE_JOB = _env("GLUE_JOB_NAME")
REGION = _env("AWS_REGION")

DBT_CMD = (
    "/opt/airflow/dbt-venv/bin/dbt build "
    "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
)

with DAG(
    dag_id="ecom_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30)},
    tags=["ecom", "glue", "dbt"],
) as dag:
    wait_for_bronze = S3KeySensor(
        task_id="wait_for_bronze",
        bucket_name=BUCKET,
        bucket_key="bronze/*",
        wildcard_match=True,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
    )

    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name=GLUE_JOB,
        region_name=REGION,
        verbose=True,
        wait_for_completion=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_CMD,
        append_env=True,
        env={"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"},
    )

    wait_for_bronze >> bronze_to_silver >> dbt_build
