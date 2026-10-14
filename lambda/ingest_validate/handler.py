"""S3-triggered Lambda: validate each file landing in landing/ and route it.

    landing/<topic>/<date>/<file>.jsonl
        valid    -> bronze/topic_name=<topic>/event_date=<date>/<file>.jsonl
        invalid  -> quarantine/<topic>/<date>/<file>.jsonl
                    quarantine-reasons/<topic>/<date>/<file>.jsonl.json
    and the landing object is deleted once routing has succeeded.

Bronze keys are topic + event_date + file name (the landing date folder is
NOT part of the key). So a replay of the same bytes rewrites the same objects
harmlessly, but two different files that share a name could map to one key.
The handler therefore refuses to overwrite: if a target bronze key already
holds different bytes, the file is quarantined with a "name collision" reason
and nothing is written for any of its dates. To replace a bronze file on
purpose, delete the old object first.

Retry behaviour (each case is covered in tests/test_ingest_failure_modes.py):
  * the landing object is deleted last, so any earlier failure leaves it in
    landing/, where the batch-close state machine sees it and refuses to proceed;
  * a file spanning midnight writes one partition at a time, so a failure
    between them leaves the first partition visible until the retry completes
    the rest; the retry converges to the same final state;
  * if bronze (or quarantine) is written but the landing delete fails, the
    retry rewrites identical bytes and deletes;
  * a duplicate S3 event for a file already routed finds nothing in landing
    and does nothing.

Unexpected errors (S3 throttling, permissions) are NOT swallowed: they
propagate so Lambda's asynchronous retry runs, and the file stays in landing
where the batch-close state machine will see it and refuse to proceed.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from urllib.parse import unquote_plus

from validation import parse_object_key, validate_file

LANDING = "landing/"
BRONZE = "bronze/"
QUARANTINE = "quarantine/"
REASONS = "quarantine-reasons/"

# Files are small (tens of events). Anything this large is a producer fault,
# and reading it fully into Lambda memory is not worth the risk.
MAX_OBJECT_BYTES = 25 * 1024 * 1024

log = logging.getLogger()
log.setLevel(logging.INFO)


def _is_no_such_key(exc: Exception) -> bool:
    return getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404")


def _bronze_key(topic: str, event_date: str, filename: str) -> str:
    return f"{BRONZE}topic_name={topic}/event_date={event_date}/{filename}"


def _quarantine(s3, bucket: str, key: str, errors: list[str], records: int) -> None:
    relative = key[len(LANDING):]
    s3.copy_object(Bucket=bucket, Key=QUARANTINE + relative,
                   CopySource={"Bucket": bucket, "Key": key})
    reason = {
        "source_key": key,
        "records_seen": records,
        "errors": errors,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(Bucket=bucket, Key=f"{REASONS}{relative}.json",
                  Body=json.dumps(reason, indent=2).encode("utf-8"),
                  ContentType="application/json")


def _get_or_none(s3, bucket: str, key: str) -> dict | None:
    """The GetObject response (body not yet read), or None if the key does not exist.

    Any other error propagates so Lambda retries. Needs s3:ListBucket, or a
    missing key comes back as AccessDenied instead of NoSuchKey.
    """
    try:
        return s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        if _is_no_such_key(exc):
            return None
        raise


def _existing_bytes(s3, bucket: str, key: str) -> bytes | None:
    """Bytes already stored at key, or None. Bronze objects only: they are
    written by this function from files under the size limit."""
    response = _get_or_none(s3, bucket, key)
    return None if response is None else response["Body"].read()


def route_object(s3, bucket: str, key: str, allowed_topics: list[str]) -> dict:
    """Validate and route one landing object. Returns a small outcome dict."""
    if not key.startswith(LANDING) or not key.endswith(".jsonl"):
        return {"key": key, "action": "ignored"}

    try:
        topic, filename = parse_object_key(key, LANDING, allowed_topics)
    except ValueError as exc:
        # Cannot even tell which topic this is. Quarantine it as-is.
        try:
            s3.head_object(Bucket=bucket, Key=key)
        except Exception as head_exc:  # noqa: BLE001
            if _is_no_such_key(head_exc):
                return {"key": key, "action": "missing"}
            raise
        _quarantine(s3, bucket, key, [str(exc)], records=0)
        s3.delete_object(Bucket=bucket, Key=key)
        return {"key": key, "action": "quarantined", "errors": [str(exc)]}

    # The response, not the bytes: the size check below must run before the
    # body is read, or an oversized file is loaded into memory anyway.
    response = _get_or_none(s3, bucket, key)
    if response is None:
        return {"key": key, "action": "missing"}  # duplicate event; already routed

    size = response.get("ContentLength", 0)
    if size > MAX_OBJECT_BYTES:
        errors = [f"file is {size} bytes, above the {MAX_OBJECT_BYTES} byte limit"]
        _quarantine(s3, bucket, key, errors, records=0)
        s3.delete_object(Bucket=bucket, Key=key)
        return {"key": key, "action": "quarantined", "errors": errors}

    body = response["Body"].read()
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        errors = [f"file is not valid UTF-8 ({exc.reason} at byte {exc.start})"]
        _quarantine(s3, bucket, key, errors, records=0)
        s3.delete_object(Bucket=bucket, Key=key)
        return {"key": key, "action": "quarantined", "errors": errors}

    # split("\n"), not splitlines(): splitlines also breaks on characters that
    # are legal inside a JSON string and would corrupt a valid record.
    result = validate_file(text.split("\n"), expected_topic=topic)

    if not result.ok:
        _quarantine(s3, bucket, key, result.errors, result.records)
        s3.delete_object(Bucket=bucket, Key=key)
        return {"key": key, "action": "quarantined", "errors": result.errors}

    # Build every target first and refuse to overwrite different content, so a
    # collision on any partition writes nothing for the others.
    targets = [(_bronze_key(topic, event_date, filename), ("\n".join(lines) + "\n").encode("utf-8"))
               for event_date, lines in sorted(result.by_date.items())]
    errors = []
    for target, body in targets:
        existing = _existing_bytes(s3, bucket, target)
        if existing is not None and existing != body:
            errors.append(f"bronze key {target} already exists with different content "
                          "(another file with the same name, topic and event date?)")
    if errors:
        _quarantine(s3, bucket, key, errors, result.records)
        s3.delete_object(Bucket=bucket, Key=key)
        return {"key": key, "action": "quarantined", "errors": errors}

    written = []
    for target, body in targets:
        s3.put_object(Bucket=bucket, Key=target, Body=body)
        written.append(target)
    s3.delete_object(Bucket=bucket, Key=key)
    return {"key": key, "action": "routed", "records": result.records, "bronze_keys": written}


def objects_in_event(event: dict) -> list[tuple[str, str]]:
    """(bucket, key) for each S3 record. S3 events URL-encode keys."""
    return [(r["s3"]["bucket"]["name"], unquote_plus(r["s3"]["object"]["key"]))
            for r in event.get("Records", [])]


def lambda_handler(event, _context):
    import boto3  # imported here so the module is importable without boto3 (unit tests)

    allowed = [t for t in os.environ["ALLOWED_TOPICS"].split(",") if t]
    s3 = boto3.client("s3")

    outcomes = []
    for bucket, key in objects_in_event(event):
        outcome = route_object(s3, bucket, key, allowed)
        log.info(json.dumps(outcome))
        outcomes.append(outcome)
    return {"results": outcomes}
