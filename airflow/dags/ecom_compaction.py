"""Weekly Iceberg compaction (rewrite_data_files) via the same Glue job with --COMPACT true."""
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

GLUE_JOB = os.environ.get("GLUE_JOB_NAME")
REGION = os.environ.get("AWS_REGION")
if not GLUE_JOB or not REGION:
    raise RuntimeError("GLUE_JOB_NAME / AWS_REGION not set; run scripts/make_airflow_env.sh")

with DAG(
    dag_id="ecom_compaction",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["ecom", "iceberg"],
) as dag:
    GlueJobOperator(
        task_id="compact_silver",
        job_name=GLUE_JOB,
        region_name=REGION,
        script_args={"--COMPACT": "true"},
        verbose=True,
        wait_for_completion=True,
    )
