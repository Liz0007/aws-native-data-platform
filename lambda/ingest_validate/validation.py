"""Envelope validation for files arriving in landing/. Pure Python, no AWS.

What this layer checks, and what it deliberately does not
---------------------------------------------------------
Ingestion checks that a file is *structurally* what the platform promises to
emit: every line is a JSON object with the six envelope fields, of the right
types, on the topic its path says. It does NOT judge business content
(a null order id, an unknown status, a negative amount): those are the
silver job's validity rules, which fail the run with a named rule. Keeping
the two apart means a file that merely carries bad *values* still reaches
bronze, where the replay guarantee ("bronze stores what arrived") holds.

A file is accepted or rejected as a whole. Splitting good lines from bad ones
would make bronze depend on partial reads of a file the producer wrote as one
unit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from typing import Iterable, Sequence

ENVELOPE_FIELDS = ("topic", "partition", "offset", "kafka_timestamp", "ingested_at", "payload")
INT_FIELDS = ("partition", "offset", "kafka_timestamp")

# A rejected file reports at most this many problems; the first few are
# enough to diagnose, and a corrupt file can have millions.
MAX_REPORTED_ERRORS = 10


@dataclass
class ValidationResult:
    records: int = 0
    # event_date (UTC, YYYY-MM-DD) -> the file's original lines for that date
    by_date: dict[str, list[str]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def parse_object_key(key: str, prefix: str, allowed_topics: Sequence[str]) -> tuple[str, str]:
    """landing/<topic>/<anything...>/<file>.jsonl  ->  (topic, file name).

    Raises ValueError with a readable reason if the key is not in that shape.
    """
    if not key.startswith(prefix):
        raise ValueError(f"key does not start with {prefix!r}")
    parts = key[len(prefix):].split("/")
    if len(parts) < 2 or not parts[-1]:
        raise ValueError("expected landing/<topic>/.../<file>.jsonl")
    topic, filename = parts[0], parts[-1]
    if topic not in allowed_topics:
        raise ValueError(f"unknown topic {topic!r} in path (allowed: {', '.join(allowed_topics)})")
    return topic, filename


def parse_ingested_at(value: object) -> datetime:
    """Parse an ISO-8601 timestamp to an aware UTC datetime.

    A timestamp with no offset is read as UTC rather than rejected: refusing
    it could not protect anything, and the partition date is a UTC date.
    """
    if not isinstance(value, str):
        raise ValueError("not a string")
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_int(value: object) -> bool:
    # bool is a subclass of int in Python; true/false must not pass as numbers.
    return isinstance(value, int) and not isinstance(value, bool)


def check_record(obj: object, expected_topic: str) -> tuple[str | None, list[str]]:
    """Return (event_date or None, problems) for one parsed JSON value."""
    if not isinstance(obj, dict):
        return None, [f"expected a JSON object, got {type(obj).__name__}"]

    problems: list[str] = []
    for name in ENVELOPE_FIELDS:
        if name not in obj:
            problems.append(f"missing field {name!r}")
    if problems:
        return None, problems

    if obj["topic"] != expected_topic:
        problems.append(f"topic {obj['topic']!r} does not match the path topic {expected_topic!r}")
    for name in INT_FIELDS:
        if not _is_int(obj[name]):
            problems.append(f"{name!r} must be an integer, got {obj[name]!r}")
    if not isinstance(obj["payload"], dict):
        problems.append(f"'payload' must be a JSON object, got {type(obj['payload']).__name__}")

    event_date = None
    try:
        event_date = parse_ingested_at(obj["ingested_at"]).date().isoformat()
    except (ValueError, TypeError) as exc:
        problems.append(f"'ingested_at' is not an ISO-8601 timestamp: {obj['ingested_at']!r} ({exc})")

    return (event_date if not problems else None), problems


def validate_file(lines: Iterable[str], expected_topic: str) -> ValidationResult:
    """Validate every non-blank line; group accepted lines by event date.

    event_date comes from each record's own `ingested_at` (UTC), not from when
    the file arrived, so replaying a file always lands it in the same
    partition. A file whose records span midnight is therefore split across
    two partitions, each copy carrying only that date's lines.
    """
    result = ValidationResult()
    total_problems = 0

    for number, raw in enumerate(lines, start=1):
        text = raw.rstrip("\r")
        if not text.strip():
            continue
        result.records += 1

        try:
            obj = json.loads(text)
        except json.JSONDecodeError as exc:
            problems, event_date = [f"not valid JSON ({exc.msg} at column {exc.colno})"], None
        else:
            event_date, problems = check_record(obj, expected_topic)

        if problems:
            total_problems += len(problems)
            for problem in problems:
                if len(result.errors) < MAX_REPORTED_ERRORS:
                    result.errors.append(f"line {number}: {problem}")
        elif event_date is not None:
            result.by_date.setdefault(event_date, []).append(text)

    if result.records == 0:
        result.errors.append("file contains no records")
    elif total_problems > len(result.errors):
        result.errors.append(f"... and {total_problems - len(result.errors)} more problem(s)")

    if result.errors:
        result.by_date = {}  # a rejected file contributes nothing to bronze
    return result
