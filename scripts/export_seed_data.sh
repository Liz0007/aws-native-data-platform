#!/usr/bin/env bash
# Exports the local platform's data lake as seed data for this project.
#
# The source is real output from real-time-ecommerce-platform: events its
# services actually produced, archived by its data-pipeline service as
# newline-delimited JSON partitioned by topic and date. Using it here means
# this pipeline processes genuine event data rather than a synthetic CSV,
# and the two projects tell one connected story — while staying separate
# repos with no runtime dependency.
#
# Run from THIS repo's root, with the ecommerce platform's stack running:
#
#     ./scripts/export_seed_data.sh ~/path/to/real-time-ecommerce-platform
#
# Then commit data/seed/ so this repo stands alone.

set -euo pipefail

PLATFORM_DIR="${1:-}"
if [ -z "$PLATFORM_DIR" ] || [ ! -f "$PLATFORM_DIR/docker-compose.yml" ]; then
  echo "Usage: $0 <path-to-real-time-ecommerce-platform>" >&2
  exit 1
fi

DEST="$(pwd)/data/seed"
mkdir -p "$DEST"

echo "==> Copying data lake out of the data-pipeline container"
# The lake lives on a named volume inside the container, so it is copied out
# rather than read from the host filesystem.
(cd "$PLATFORM_DIR" && docker compose cp data-pipeline:/data/lake "$DEST/lake-tmp")

# Flatten to <topic>/<date>/<file>.jsonl, dropping the container path prefix.
rm -rf "$DEST/lake"
mv "$DEST/lake-tmp" "$DEST/lake"

echo "==> Summary"
total=0
for topic_dir in "$DEST"/lake/*/; do
  topic=$(basename "$topic_dir")
  count=$(find "$topic_dir" -name '*.jsonl' -exec cat {} + | wc -l | tr -d ' ')
  files=$(find "$topic_dir" -name '*.jsonl' | wc -l | tr -d ' ')
  printf "    %-22s %6s records in %s files\n" "$topic" "$count" "$files"
  total=$((total + count))
done
echo "    ----"
printf "    %-22s %6s records\n" "TOTAL" "$total"
du -sh "$DEST/lake" | awk '{print "    on disk: " $1}'

# Quoted, so it survives a repo path containing spaces.
sample=$(find "$DEST/lake" -name '*.jsonl' | head -1)
if [ -n "$sample" ]; then
  echo
  echo "Sample record:"
  head -1 "$sample" | python3 -m json.tool
fi