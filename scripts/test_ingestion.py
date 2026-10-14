"""End-to-end proof that ingestion rejects bad files and refuses to close a batch.

Run from the repo root against the deployed stack (needs boto3 and AWS
credentials; `--dry-run` shows the plan without touching AWS):

    python scripts/test_ingestion.py [--dry-run]

What it does, and what must happen at each step
  1. Preflight: landing/ and quarantine/ must be empty, or it stops. A
     leftover quarantined file would fail the "clean" batch for the wrong reason.
  2. Drops three structurally bad files into landing/ (malformed JSON, a
     missing envelope field, a record for the wrong topic).
  3. Expects the Lambda to move all three to quarantine/, each with a reason
     naming the problem, and expects nothing from them in bronze/.
  4. Starts the close-batch state machine. Expects it to FAIL with
     QuarantineNotEmpty and to write no marker.
  5. Removes the quarantined test files, runs the state machine again, and
     expects SUCCEEDED plus a marker. It then deletes that marker so a test
     run cannot start Airflow.

Only files named zz_ingest_test_* are created or deleted. No valid test file
is sent: it would add a real order to bronze and, through silver, to gold.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time

TOPIC = "order-created"
DATE = "2026-09-30"
PREFIX = f"{TOPIC}/{DATE}/zz_ingest_test_"

GOOD_LINE = {"topic": TOPIC, "partition": 99, "offset": 1, "kafka_timestamp": 1790777184002,
             "ingested_at": "2026-09-30T14:00:00.000000+00:00",
             "payload": {"id": "test-order", "customer_id": "c", "status": "pending", "total_amount": "10.00"}}

# name -> (file content, text the quarantine reason must contain)
BAD_FILES = {
    "malformed_json": (json.dumps(GOOD_LINE) + "\n{this is not json\n", "not valid JSON"),
    "missing_field": (json.dumps({k: v for k, v in GOOD_LINE.items() if k != "offset"}) + "\n",
                      "missing field 'offset'"),
    "wrong_topic": (json.dumps({**GOOD_LINE, "topic": "payment-processed"}) + "\n",
                    "does not match the path topic"),
}


def tf(name: str) -> str:
    root = pathlib.Path(__file__).resolve().parents[1]
    return subprocess.run(["terraform", f"-chdir={root / 'terraform'}", "output", "-raw", name],
                          check=True, capture_output=True, text=True).stdout.strip()


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {label}{('  (' + detail + ')') if detail else ''}")
    if not ok:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="print the plan; touch nothing in AWS")
    args = parser.parse_args()

    if args.dry_run:
        print("Plan (nothing is touched):")
        for name, (content, expect) in BAD_FILES.items():
            print(f"  upload landing/{PREFIX}{name}.jsonl, expect quarantine reason containing {expect!r}")
            print(f"      {content.splitlines()[-1][:110]}")
        print("  start close-batch -> expect FAILED / QuarantineNotEmpty, no new marker")
        print("  delete the quarantined test files, start close-batch again -> expect SUCCEEDED + marker")
        print("  delete that marker")
        return

    import boto3  # imported here so --dry-run works without it

    region, bucket, machine = tf("aws_region"), tf("lake_bucket"), tf("close_batch_state_machine")
    s3 = boto3.client("s3", region_name=region)
    sfn = boto3.client("stepfunctions", region_name=region)

    def keys(prefix: str) -> list[str]:
        out, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix, **({"ContinuationToken": token} if token else {})}
            page = s3.list_objects_v2(**kw)
            out += [o["Key"] for o in page.get("Contents", [])]
            token = page.get("NextContinuationToken")
            if not token:
                return out

    def run_batch() -> tuple[str, str, str]:
        arn = sfn.start_execution(stateMachineArn=machine, input=json.dumps({"max_quarantined": 0}))["executionArn"]
        while True:
            d = sfn.describe_execution(executionArn=arn)
            if d["status"] != "RUNNING":
                return d["status"], d.get("error", ""), arn.rsplit(":", 1)[-1]
            time.sleep(10)

    print("1. Preflight")
    check("landing/ is empty", keys("landing/") == [])
    check("quarantine/ is empty", keys("quarantine/") == [])
    markers_before = set(keys("markers/bronze/"))

    print("2. Upload three bad files")
    for name, (content, _) in BAD_FILES.items():
        s3.put_object(Bucket=bucket, Key=f"landing/{PREFIX}{name}.jsonl", Body=content.encode())

    deadline = time.time() + 90
    while keys("landing/") and time.time() < deadline:
        time.sleep(3)
    check("Lambda drained landing/", keys("landing/") == [])

    print("3. Quarantine and reasons")
    quarantined = keys("quarantine/")
    check("exactly three files quarantined", len(quarantined) == 3, str(len(quarantined)))
    for name, (_, expect) in BAD_FILES.items():
        body = s3.get_object(Bucket=bucket, Key=f"quarantine-reasons/{PREFIX}{name}.jsonl.json")["Body"].read()
        reason = json.loads(body)["errors"]
        check(f"{name}: reason mentions {expect!r}", any(expect in e for e in reason), reason[0])
    check("none of them reached bronze/", not [k for k in keys("bronze/") if "zz_ingest_test_" in k])

    print("4. Close-batch must refuse")
    status, error, _ = run_batch()
    check("execution FAILED with QuarantineNotEmpty", (status, error) == ("FAILED", "QuarantineNotEmpty"),
          f"{status} / {error}")
    check("no marker was written", set(keys("markers/bronze/")) == markers_before)

    print("5. Clear quarantine, close-batch must succeed")
    for key in keys("quarantine/") + keys("quarantine-reasons/"):
        if "zz_ingest_test_" in key:
            s3.delete_object(Bucket=bucket, Key=key)
    check("quarantine/ is empty again", keys("quarantine/") == [])
    status, error, name = run_batch()
    check("execution SUCCEEDED", status == "SUCCEEDED", f"{status} {error}")
    marker = f"markers/bronze/{name}.success"
    check("marker written", marker in keys("markers/bronze/"), marker)
    s3.delete_object(Bucket=bucket, Key=marker)
    print("\nAll checks passed. (Test marker removed so Airflow is not triggered.)")


if __name__ == "__main__":
    main()
