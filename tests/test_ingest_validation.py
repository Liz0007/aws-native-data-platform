"""Tests for the ingestion Lambda. Standard library only (runs under
`python -m unittest` or pytest); S3 is an in-memory fake, so no AWS needed.

    python -m unittest tests.test_ingest_validation -v
"""

from __future__ import annotations

import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lambda" / "ingest_validate"))

import handler  # noqa: E402
from validation import check_record, parse_object_key, validate_file  # noqa: E402

TOPICS = ["order-created", "order-confirmed", "order-cancelled",
          "payment-processed", "inventory-reserved"]
BUCKET = "lake"


class NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    """Just enough of the S3 client for route_object."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NoSuchKey()
        data = self.objects[Key]

        class Body:
            def read(_self):
                return data

        return {"Body": Body(), "ContentLength": len(data)}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NoSuchKey()
        return {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))


def record(topic="order-created", offset=1, ingested_at="2026-09-30T14:00:00.000000+00:00",
           payload=None, **overrides):
    body = {"topic": topic, "partition": 0, "offset": offset,
            "kafka_timestamp": 1790777184002, "ingested_at": ingested_at,
            "payload": {"id": f"o-{offset}", "status": "pending"} if payload is None else payload}
    body.update(overrides)
    return json.dumps(body)


def file_of(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


class ObjectKeyTests(unittest.TestCase):
    def test_topic_and_filename_come_from_the_path(self):
        self.assertEqual(
            parse_object_key("landing/order-created/2026-09-30/f1.jsonl", "landing/", TOPICS),
            ("order-created", "f1.jsonl"))

    def test_unknown_topic_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_object_key("landing/orders/2026-09-30/f1.jsonl", "landing/", TOPICS)

    def test_key_without_a_file_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_object_key("landing/order-created", "landing/", TOPICS)


class RecordTests(unittest.TestCase):
    def test_valid_record_yields_its_utc_date(self):
        date, problems = check_record(json.loads(record()), "order-created")
        self.assertEqual((date, problems), ("2026-09-30", []))

    def test_offset_timestamps_are_converted_to_utc_for_the_date(self):
        # 23:30 at UTC-02:00 is 01:30 the next day in UTC.
        obj = json.loads(record(ingested_at="2026-09-30T23:30:00-02:00"))
        self.assertEqual(check_record(obj, "order-created")[0], "2026-10-01")

    def test_missing_field(self):
        obj = json.loads(record())
        del obj["offset"]
        self.assertIn("missing field 'offset'", check_record(obj, "order-created")[1])

    def test_booleans_do_not_pass_as_integers(self):
        obj = json.loads(record(partition=True))
        self.assertTrue(any("'partition' must be an integer" in p
                            for p in check_record(obj, "order-created")[1]))

    def test_payload_must_be_an_object(self):
        obj = json.loads(record(payload="not-an-object"))
        self.assertTrue(any("'payload' must be a JSON object" in p
                            for p in check_record(obj, "order-created")[1]))

    def test_topic_must_match_the_path(self):
        obj = json.loads(record(topic="payment-processed"))
        self.assertTrue(any("does not match the path topic" in p
                            for p in check_record(obj, "order-created")[1]))

    def test_bad_timestamp(self):
        obj = json.loads(record(ingested_at="yesterday"))
        self.assertTrue(any("ISO-8601" in p for p in check_record(obj, "order-created")[1]))


class FileTests(unittest.TestCase):
    def test_records_are_grouped_by_date_and_kept_verbatim(self):
        a = record(offset=1, ingested_at="2026-09-30T23:59:59+00:00")
        b = record(offset=2, ingested_at="2026-10-01T00:00:01+00:00")
        result = validate_file([a, b, ""], "order-created")
        self.assertTrue(result.ok)
        self.assertEqual(result.by_date, {"2026-09-30": [a], "2026-10-01": [b]})

    def test_one_bad_line_rejects_the_whole_file_and_names_its_line(self):
        result = validate_file([record(offset=1), "{not json", record(offset=3)], "order-created")
        self.assertFalse(result.ok)
        self.assertTrue(result.errors[0].startswith("line 2: not valid JSON"))
        self.assertEqual(result.by_date, {})

    def test_empty_file_is_rejected(self):
        self.assertEqual(validate_file(["", "  "], "order-created").errors,
                         ["file contains no records"])

    def test_reported_errors_are_capped(self):
        result = validate_file(["{bad"] * 100, "order-created")
        self.assertLessEqual(len(result.errors), 11)
        self.assertTrue(result.errors[-1].startswith("... and "))


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.key = "landing/order-created/2026-09-30/f1.jsonl"

    def route(self, key=None):
        return handler.route_object(self.s3, BUCKET, key or self.key, TOPICS)

    def test_valid_file_goes_to_bronze_and_leaves_landing(self):
        self.s3.objects[self.key] = file_of(record(offset=1), record(offset=2))
        outcome = self.route()
        self.assertEqual(outcome["action"], "routed")
        self.assertEqual(self.s3.keys("bronze/"),
                         ["bronze/topic_name=order-created/event_date=2026-09-30/f1.jsonl"])
        self.assertEqual(self.s3.keys("landing/"), [])
        self.assertEqual(self.s3.keys("quarantine"), [])

    def test_file_spanning_midnight_is_split_across_partitions(self):
        self.s3.objects[self.key] = file_of(
            record(offset=1, ingested_at="2026-09-30T23:59:59+00:00"),
            record(offset=2, ingested_at="2026-10-01T00:00:01+00:00"))
        self.route()
        self.assertEqual(self.s3.keys("bronze/"), [
            "bronze/topic_name=order-created/event_date=2026-09-30/f1.jsonl",
            "bronze/topic_name=order-created/event_date=2026-10-01/f1.jsonl"])

    def test_malformed_json_is_quarantined_with_a_reason_and_never_reaches_bronze(self):
        self.s3.objects[self.key] = file_of(record(offset=1), "{this is not json")
        outcome = self.route()
        self.assertEqual(outcome["action"], "quarantined")
        self.assertEqual(self.s3.keys("bronze/"), [])
        self.assertEqual(self.s3.keys("landing/"), [])
        self.assertEqual(self.s3.keys("quarantine/"),
                         ["quarantine/order-created/2026-09-30/f1.jsonl"])
        reason = json.loads(self.s3.objects[
            "quarantine-reasons/order-created/2026-09-30/f1.jsonl.json"])
        self.assertIn("line 2: not valid JSON", reason["errors"][0])

    def test_quarantine_preserves_the_original_bytes(self):
        original = file_of(record(offset=1), "{broken")
        self.s3.objects[self.key] = original
        self.route()
        self.assertEqual(self.s3.objects["quarantine/order-created/2026-09-30/f1.jsonl"], original)

    def test_record_for_another_topic_is_quarantined(self):
        self.s3.objects[self.key] = file_of(record(topic="payment-processed"))
        self.assertEqual(self.route()["action"], "quarantined")

    def test_invalid_utf8_is_quarantined(self):
        self.s3.objects[self.key] = b'{"topic": "\xff\xfe"}\n'
        outcome = self.route()
        self.assertEqual(outcome["action"], "quarantined")
        self.assertIn("UTF-8", outcome["errors"][0])

    def test_unknown_topic_in_path_is_quarantined(self):
        key = "landing/mystery/2026-09-30/f1.jsonl"
        self.s3.objects[key] = file_of(record())
        self.assertEqual(self.route(key)["action"], "quarantined")
        self.assertEqual(self.s3.keys("bronze/"), [])

    def test_a_row_with_bad_business_values_still_reaches_bronze(self):
        # Ingestion checks structure only. A null order id is a silver rule's
        # job: bronze must keep what arrived so it can be replayed.
        self.s3.objects[self.key] = file_of(record(payload={"id": None, "status": "pending"}))
        self.assertEqual(self.route()["action"], "routed")
        self.assertEqual(len(self.s3.keys("bronze/")), 1)

    def test_replay_is_idempotent(self):
        content = file_of(record(offset=1))
        self.s3.objects[self.key] = content
        self.route()
        first = dict(self.s3.objects)
        self.s3.objects[self.key] = content  # the same file arrives again
        self.route()
        self.assertEqual(self.s3.objects, first)

    def test_duplicate_event_for_an_already_routed_file_is_a_no_op(self):
        self.assertEqual(self.route()["action"], "missing")
        self.assertEqual(self.s3.objects, {})

    def test_other_keys_are_ignored(self):
        for key in ("bronze/x.jsonl", "landing/order-created/2026-09-30/readme.txt"):
            self.s3.objects[key] = b"x"
            self.assertEqual(self.route(key)["action"], "ignored")

    def test_event_keys_are_url_decoded(self):
        event = {"Records": [{"s3": {"bucket": {"name": BUCKET},
                                     "object": {"key": "landing/order-created/2026-09-30/a+b%3D1.jsonl"}}}]}
        self.assertEqual(handler.objects_in_event(event),
                         [(BUCKET, "landing/order-created/2026-09-30/a b=1.jsonl")])


if __name__ == "__main__":
    unittest.main()
