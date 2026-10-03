"""Validates the Glue job's payload schemas against the real seed data.

The Glue job declares an explicit schema per topic. If a field name is wrong,
Spark does not fail — from_json simply returns null for that column, and the
error shows up much later as an empty silver table. This checks the declared
names against every record in data/seed/, in seconds, with no AWS or Spark.

    python scripts/check_payload_schemas.py
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

# Must match PAYLOAD_SCHEMAS in glue/jobs/bronze_to_silver.py.
DECLARED = {
    "order-created": {"id", "customer_id", "status", "total_amount"},
    "payment-processed": {
        "order_id", "status", "amount", "payment_method",
        "provider_transaction_id", "failure_reason", "processed_at",
    },
    "inventory-reserved": {"order_id", "status", "reserved_at", "expires_at"},
    "order-confirmed": {"order_id", "status", "payment_status", "inventory_status"},
    "order-cancelled": {"order_id", "status", "payment_status", "inventory_status"},
}

ENVELOPE = {"topic", "partition", "offset", "kafka_timestamp", "ingested_at", "payload"}

SEED = Path("data/seed/lake")


def main() -> int:
    if not SEED.is_dir():
        print(f"no seed data at {SEED}", file=sys.stderr)
        return 1

    actual = defaultdict(set)      # topic -> every payload key seen
    always = {}                    # topic -> keys present in every record
    counts = defaultdict(int)
    envelope_problems = set()

    for path in sorted(SEED.rglob("*.jsonl")):
        topic = path.parent.parent.name
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            counts[topic] += 1

            missing_envelope = ENVELOPE - record.keys()
            if missing_envelope:
                envelope_problems.add(f"{topic}: envelope missing {sorted(missing_envelope)}")

            keys = set(record.get("payload", {}).keys())
            actual[topic] |= keys
            always[topic] = keys if topic not in always else (always[topic] & keys)

    ok = True

    for problem in sorted(envelope_problems):
        print(f"ENVELOPE  {problem}")
        ok = False

    for topic in sorted(set(DECLARED) | set(actual)):
        declared = DECLARED.get(topic, set())
        seen = actual.get(topic, set())

        if topic not in actual:
            print(f"MISSING   {topic}: declared but no records found in seed")
            ok = False
            continue
        if topic not in DECLARED:
            print(f"UNKNOWN   {topic}: {counts[topic]} records, no schema declared")
            ok = False
            continue

        wrong = declared - seen          # declared but never present -> silent nulls
        extra = seen - declared          # present but not declared -> silently dropped
        optional = seen - always[topic]  # present in some records only

        status = "OK"
        if wrong or extra:
            status = "MISMATCH"
            ok = False

        print(f"{status}  {topic}: {counts[topic]} records, {len(seen)} payload fields")
        if wrong:
            print(f"declared but never seen (would be null): {sorted(wrong)}")
        if extra:
            print(f"present but not declared (would be dropped): {sorted(extra)}")
        if optional:
            print(f"nullable (absent from some records): {sorted(optional)}")

    print()
    print("all schemas match the seed data" if ok else "schema problems found — fix before running the Glue job")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
