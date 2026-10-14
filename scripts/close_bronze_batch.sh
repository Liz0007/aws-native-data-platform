#!/usr/bin/env bash
# Starts the close-batch state machine and waits for it to finish.
#
#   ./scripts/close_bronze_batch.sh [max_quarantined]
#
# Exit status is 0 only if the execution SUCCEEDED, i.e. landing/ drained,
# quarantine/ was within the limit, the crawler succeeded and the marker
# was written.

set -euo pipefail
cd "$(dirname "$0")/.."

MAX_Q="${1:-0}"
REGION=$(terraform -chdir=terraform output -raw aws_region)
ARN=$(terraform -chdir=terraform output -raw close_batch_state_machine)

EXEC_ARN=$(aws stepfunctions start-execution --state-machine-arn "$ARN" \
  --input "{\"max_quarantined\": $MAX_Q}" --region "$REGION" \
  --query executionArn --output text)
echo "started: $EXEC_ARN"

while true; do
  STATUS=$(aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" \
    --region "$REGION" --query status --output text)
  [ "$STATUS" != "RUNNING" ] && break
  sleep 10
done

echo "status:  $STATUS"
if [ "$STATUS" != "SUCCEEDED" ]; then
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --region "$REGION" \
    --query '{error: error, cause: cause}' --output json
  exit 1
fi
