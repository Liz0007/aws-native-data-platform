#!/usr/bin/env bash
# Writes airflow/.env from `terraform output`. Run from repo root after `terraform apply`.
set -euo pipefail
cd "$(dirname "$0")/.."
tf() { terraform -chdir=terraform output -raw "$1"; }

OUT=airflow/.env
if [ -f "$OUT" ]; then
  FERNET_KEY=$(grep '^FERNET_KEY=' "$OUT" | cut -d= -f2-)   # keep existing key
fi
if [ -z "${FERNET_KEY:-}" ]; then
  FERNET_KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
fi

bucket=$(tf lake_bucket); region=$(tf aws_region); job=$(tf bronze_to_silver_job)
gold=$(tf gold_database); silver=$(tf silver_database); wg=$(tf athena_workgroup)

cat > "$OUT" <<ENV
FERNET_KEY=$FERNET_KEY
AIRFLOW_UID=$(id -u)
AIRFLOW_CONN_AWS_DEFAULT=aws://
LAKE_BUCKET=$bucket
GLUE_JOB_NAME=$job
AWS_REGION=$region
AWS_DEFAULT_REGION=$region
DBT_GOLD_SCHEMA=$gold
SILVER_SCHEMA=$silver
DBT_ATHENA_WORKGROUP=$wg
DBT_S3_STAGING_DIR=s3://$bucket/athena-results/dbt/
DBT_S3_DATA_DIR=s3://$bucket/gold/
ENV
chmod 600 "$OUT"
echo "wrote $OUT"
