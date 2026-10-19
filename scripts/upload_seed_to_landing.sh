#!/usr/bin/env bash
# Uploads the committed seed data into landing/, the front door of the lake.
#
# The local export is laid out as   lake/<topic>/<date>/file.jsonl
# and landing/ keeps that layout:    landing/<topic>/<date>/file.jsonl
#
# Nothing here partitions the data. The ingestion Lambda validates each file
# as it arrives and writes it to bronze/topic_name=<topic>/event_date=<d>/,
# taking <d> from the records' own `ingested_at` (UTC). It replaces
# upload_seed_to_bronze.sh, which wrote to bronze/ directly and so skipped
# validation.
#
# Run from the repo root, after `terraform apply`:
#
#     ./scripts/upload_seed_to_landing.sh
#     ./scripts/close_bronze_batch.sh        # crawl + write the marker

set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET=$(terraform -chdir=terraform output -raw lake_bucket)
REGION=$(terraform -chdir=terraform output -raw aws_region)
SRC="data/seed/lake"

if [ ! -d "$SRC" ]; then
  echo "No seed data at $SRC — run ./scripts/export_seed_data.sh first." >&2
  exit 1
fi

echo "==> Uploading to s3://$BUCKET/landing/"
aws s3 cp "$SRC" "s3://$BUCKET/landing/" --recursive --exclude '*' --include '*.jsonl' \
  --region "$REGION" --only-show-errors

echo "==> Uploaded. The Lambda routes each file within seconds; watch it with:"
echo "    aws s3 ls s3://$BUCKET/landing/ --recursive --region $REGION | wc -l    # falls to 0"
echo "    aws s3 ls s3://$BUCKET/quarantine/ --recursive --region $REGION         # should stay empty"
