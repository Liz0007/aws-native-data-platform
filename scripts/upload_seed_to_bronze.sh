#!/usr/bin/env bash
# Uploads the committed seed data into the bronze layer.
#
# The local export is laid out as   lake/<topic>/<date>/file.jsonl
# Bronze uses Hive-style partitions: bronze/topic_name=<topic>/event_date=<date>/
#
# That naming is what lets the Glue crawler infer `topic_name` and
# `event_date` as partition COLUMNS rather than opaque folder names
# (partition_0, partition_1), which in turn lets Athena prune partitions in
# a WHERE clause instead of scanning everything.
#
# The partition is `topic_name`, not `topic`, because each JSON record
# already contains a `topic` field. A partition key that collides with a
# field name produces a table with duplicate columns, which Athena refuses
# to read: "HIVE_INVALID_METADATA ... duplicate columns". Renaming the
# partition is the right fix — bronze should preserve exactly what the
# upstream platform emitted, so the record itself is left untouched.
#
# Run from the repo root, after `terraform apply`:
#
#     ./scripts/upload_seed_to_bronze.sh

set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET=$(terraform -chdir=terraform output -raw lake_bucket)
REGION=$(terraform -chdir=terraform output -raw aws_region)
SRC="data/seed/lake"

if [ ! -d "$SRC" ]; then
  echo "No seed data at $SRC — run ./scripts/export_seed_data.sh first." >&2
  exit 1
fi

echo "==> Uploading to s3://$BUCKET/bronze/"

for topic_dir in "$SRC"/*/; do
  topic=$(basename "$topic_dir")
  for date_dir in "$topic_dir"*/; do
    date=$(basename "$date_dir")
    dest="s3://$BUCKET/bronze/topic_name=$topic/event_date=$date/"
    n=$(find "$date_dir" -name '*.jsonl' | wc -l | tr -d ' ')
    echo "    $topic / $date  ($n files)"
    aws s3 cp "$date_dir" "$dest" --recursive --exclude '*' --include '*.jsonl' \
      --region "$REGION" --only-show-errors
  done
done

echo "==> Uploaded"
aws s3 ls "s3://$BUCKET/bronze/" --recursive --region "$REGION" | wc -l | \
  awk '{print "    " $1 " objects in bronze/"}'

echo
echo "Next: run the crawler so the schema and partitions appear in the catalog"
echo "    aws glue start-crawler --name $(terraform -chdir=terraform output -raw bronze_crawler) --region $REGION"