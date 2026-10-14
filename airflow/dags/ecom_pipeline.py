"""Bronze -> silver (Glue/Iceberg) -> gold (dbt on Athena), gated by the ingestion marker.

The ingestion layer (Lambda + Step Functions) writes
`markers/bronze/<execution>.success` once a batch is validated, nothing is
left in landing/ or quarantine/, and the bronze catalog is refreshed. This
DAG waits for that marker rather than for "any object under bronze/", so it
never transforms a half-landed batch.

Marker handling:
  * snapshot_markers records which markers exist when the DAG decides to go.
    Everything they cover is already in bronze, so the Glue job will read it.
  * consume_markers deletes exactly that set after dbt succeeds. A marker
    written while the run was in progress is not in the set, so it survives
    and triggers the next run: no batch is skipped.
  * If any task fails, the markers stay, and clearing the failed task
    resumes the same run.

Created paused: unpause it deliberately. Retries repeat *any* failure,
including deterministic data-quality failures from the Glue job; those will
fail 4 times (the first attempt plus 3 retries) before surfacing. Accepted
trade-off for transient Glue errors.
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator

MARKER_PREFIX = "markers/bronze/"


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


@task
def snapshot_markers() -> list[str]:
    keys = S3Hook().list_keys(bucket_name=BUCKET, prefix=MARKER_PREFIX) or []
    if not keys:
        raise RuntimeError("the sensor passed but no marker is present; was it consumed by another run?")
    return keys


@task
def consume_markers(keys: list[str]) -> int:
    S3Hook().delete_objects(bucket=BUCKET, keys=keys)
    return len(keys)


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
        bucket_key=f"{MARKER_PREFIX}*",
        wildcard_match=True,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 2,
    )

    marker_keys = snapshot_markers()

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

    consumed = consume_markers(marker_keys)

    wait_for_bronze >> marker_keys >> bronze_to_silver >> dbt_build >> consumed
