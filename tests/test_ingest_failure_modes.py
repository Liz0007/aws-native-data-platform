"""Failure-injection tests for the ingestion Lambda: partial writes, failed
deletes, and bronze key collisions. Standard library only.

    python -m unittest tests.test_ingest_failure_modes -v
"""

from __future__ import annotations

import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lambda" / "ingest_validate"))

import handler  # noqa: E402
from tests.test_ingest_validation import BUCKET, TOPICS, FakeS3, file_of, record  # noqa: E402


class Boom(Exception):
    """Stands in for a throttling or permissions error from S3."""


class FlakyS3(FakeS3):
    """FakeS3 that raises on the Nth call of a method, once, until cleared."""

    def __init__(self):
        super().__init__()
        self.fail_on: dict[str, int] = {}   # method -> raise on that call number
        self.calls: dict[str, int] = {}

    def _tick(self, method):
        self.calls[method] = self.calls.get(method, 0) + 1
        if self.fail_on.get(method) == self.calls[method]:
            raise Boom(f"{method} call {self.calls[method]}")

    def put_object(self, Bucket, Key, Body, **kw):
        self._tick("put_object")
        super().put_object(Bucket, Key, Body, **kw)

    def delete_object(self, Bucket, Key):
        self._tick("delete_object")
        super().delete_object(Bucket, Key)

    def clear_faults(self):
        self.fail_on, self.calls = {}, {}


MIDNIGHT = file_of(record(offset=1, ingested_at="2026-09-30T23:59:59+00:00"),
                   record(offset=2, ingested_at="2026-10-01T00:00:01+00:00"))
KEY = "landing/order-created/2026-09-30/f1.jsonl"


def route(s3, key=KEY):
    return handler.route_object(s3, BUCKET, key, TOPICS)


def clean_run_state(content: bytes) -> dict:
    s3 = FakeS3()
    s3.objects[KEY] = content
    route(s3)
    return dict(s3.objects)


class RetryConvergenceTests(unittest.TestCase):
    """After any single failure, a retry must end in the same state as a clean run."""

    def test_failure_between_two_date_partitions_then_retry(self):
        s3 = FlakyS3()
        s3.objects[KEY] = MIDNIGHT
        s3.fail_on["put_object"] = 2          # second bronze partition fails
        with self.assertRaises(Boom):
            route(s3)
        # the file is still in landing, so the batch cannot close on it ...
        self.assertIn(KEY, s3.objects)
        # ... and the first partition is already visible (documented transient state)
        self.assertEqual(len(s3.keys("bronze/")), 1)
        s3.clear_faults()
        route(s3)                              # the retry
        self.assertEqual(s3.objects, clean_run_state(MIDNIGHT))

    def test_bronze_written_but_landing_delete_fails_then_retry(self):
        s3 = FlakyS3()
        s3.objects[KEY] = MIDNIGHT
        s3.fail_on["delete_object"] = 1
        with self.assertRaises(Boom):
            route(s3)
        self.assertIn(KEY, s3.objects)
        self.assertEqual(len(s3.keys("bronze/")), 2)
        s3.clear_faults()
        route(s3)
        self.assertEqual(s3.objects, clean_run_state(MIDNIGHT))

    def test_quarantined_but_landing_delete_fails_then_retry(self):
        bad = file_of(record(offset=1), "{broken")
        s3 = FlakyS3()
        s3.objects[KEY] = bad
        s3.fail_on["delete_object"] = 1
        with self.assertRaises(Boom):
            route(s3)
        self.assertIn(KEY, s3.objects)                       # still in landing ...
        self.assertEqual(len(s3.keys("quarantine/")), 1)     # ... and already quarantined
        s3.clear_faults()
        route(s3)
        self.assertNotIn(KEY, s3.objects)
        self.assertEqual(s3.keys("quarantine/"), ["quarantine/order-created/2026-09-30/f1.jsonl"])
        self.assertEqual(s3.keys("bronze/"), [])


class BronzeCollisionTests(unittest.TestCase):
    """Bronze keys ignore the landing date folder, so two different files that
    share a name can map to the same key. The second must not overwrite."""

    FIRST = "landing/order-created/2026-09-30/f1.jsonl"
    SECOND = "landing/order-created/2026-10-01/f1.jsonl"
    BRONZE = "bronze/topic_name=order-created/event_date=2026-10-01/f1.jsonl"

    def setUp(self):
        self.s3 = FakeS3()
        self.first = file_of(record(offset=1, ingested_at="2026-10-01T08:00:00+00:00"))
        self.second = file_of(record(offset=2, ingested_at="2026-10-01T09:00:00+00:00"))
        self.s3.objects[self.FIRST] = self.first
        route(self.s3, self.FIRST)

    def test_different_file_with_same_name_and_date_is_quarantined_not_overwritten(self):
        self.s3.objects[self.SECOND] = self.second
        outcome = route(self.s3, self.SECOND)
        self.assertEqual(outcome["action"], "quarantined")
        self.assertIn("already exists with different content", outcome["errors"][0])
        self.assertEqual(self.s3.objects[self.BRONZE], self.first)       # first file intact
        self.assertNotIn(self.SECOND, self.s3.objects)                   # landing drained
        self.assertEqual(self.s3.objects["quarantine/order-created/2026-10-01/f1.jsonl"], self.second)

    def test_identical_replay_is_still_accepted(self):
        self.s3.objects[self.SECOND] = self.first                        # same bytes, new folder
        self.assertEqual(route(self.s3, self.SECOND)["action"], "routed")
        self.assertEqual(self.s3.objects[self.BRONZE], self.first)
        self.assertEqual(self.s3.keys("quarantine/"), [])

    def test_collision_on_any_partition_writes_nothing_for_the_others(self):
        # existing bronze object occupies the 10-01 partition of f2.jsonl
        self.s3.objects["bronze/topic_name=order-created/event_date=2026-10-01/f2.jsonl"] = b"something else\n"
        key = "landing/order-created/2026-09-30/f2.jsonl"
        self.s3.objects[key] = MIDNIGHT
        self.assertEqual(route(self.s3, key)["action"], "quarantined")
        self.assertNotIn("bronze/topic_name=order-created/event_date=2026-09-30/f2.jsonl", self.s3.objects)


class OversizeTests(unittest.TestCase):
    def test_oversized_file_is_quarantined_without_reading_its_body(self):
        class Unread(FakeS3):
            body_reads = 0
            def get_object(self, Bucket, Key):
                response = super().get_object(Bucket, Key)
                outer = self
                inner = response["Body"]
                class CountingBody:
                    def read(_self):
                        outer.body_reads += 1
                        return inner.read()
                if Key.startswith("landing/"):
                    response["ContentLength"] = handler.MAX_OBJECT_BYTES + 1
                    response["Body"] = CountingBody()
                return response

        s3 = Unread()
        s3.objects[KEY] = file_of(record())
        outcome = route(s3)
        self.assertEqual(outcome["action"], "quarantined")
        self.assertIn("byte limit", outcome["errors"][0])
        self.assertEqual(s3.body_reads, 0)          # the guard ran before any read
        self.assertEqual(s3.keys("bronze/"), [])


if __name__ == "__main__":
    unittest.main()
