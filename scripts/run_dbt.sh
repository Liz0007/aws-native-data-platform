#!/usr/bin/env bash
# Runs dbt against Athena, with bucket-specific settings read from Terraform.
#
# Run from the repo root. Any arguments are passed through to dbt:
#
#     ./scripts/run_dbt.sh build            # models + all tests, in DAG order
#     ./scripts/run_dbt.sh test             # tests only
#     ./scripts/run_dbt.sh build -s fct_orders+
#
# `build` runs source tests before the models that depend on them, so a
# broken silver contract stops the run before any gold table is rebuilt.

set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET=$(terraform -chdir=terraform output -raw lake_bucket)

export AWS_REGION=$(terraform -chdir=terraform output -raw aws_region)
export DBT_GOLD_SCHEMA=$(terraform -chdir=terraform output -raw gold_database)
export SILVER_SCHEMA=$(terraform -chdir=terraform output -raw silver_database)
export DBT_ATHENA_WORKGROUP=$(terraform -chdir=terraform output -raw athena_workgroup)
export DBT_S3_STAGING_DIR="s3://$BUCKET/athena-results/dbt/"
export DBT_S3_DATA_DIR="s3://$BUCKET/gold/"

cd dbt
exec dbt "${@:-build}" --profiles-dir .
