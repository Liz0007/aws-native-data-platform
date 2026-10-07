#!/usr/bin/env python3
"""Proves that every silver validity rule fires, and fires only when it should.

For each topic this injects a file of deliberately bad events into bronze,
runs the Glue job, and checks that the job FAILS and names exactly the rules
that were broken. A rule that has only ever been seen passing on good data has
not really been tested; this is what shows it can fail.

How the rows are built
    The rule set is read from glue/jobs/bronze_to_silver.py itself (required
    fields and allowed values by parsing the source, the few fixed rules by
    name), so the test cannot drift from the job silently. For each rule one
    row is built that breaks that rule and no other, plus control rows that
    must break none. The job's failure message lists every broken rule with a
    count, so the expected outcome is "each rule exactly once". That checks
    both directions: every rule fires, and none fires on a row it shouldn't.

Safety
    Injected rows use Kafka partition 99, which does not exist in the seed
    data. The job validates a topic before writing it, so a failing run never
    writes the bad rows to silver. The injected file is deleted afterwards
    whether the run passes or not, and leftovers from an interrupted run are
    removed at the start.

Run from the repo root:

    python3 scripts/test_validity_rules.py --dry-run     # no AWS: show the plan
    python3 scripts/test_validity_rules.py               # all five topics
    python3 scripts/test_validity_rules.py --topic payment-processed

Needs: python3, the aws CLI, terraform (for the bucket and job names).
"""

import argparse
import ast
import json
import re
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
JOB_SOURCE = REPO / "glue" / "jobs" / "bronze_to_silver.py"

TEST_FILE = "zz_validity_test.jsonl"
PARTITION = 99                      # no real Kafka partition; marks injected rows
GOOD_TS = 1790777184002             # 2026-09-30, the same day as the seed data
FUTURE_TS = 4102444800000           # 2100-01-01
INGESTED_AT = "2026-09-30T14:00:00.000000+00:00"
GOOD_TIME = INGESTED_AT
EARLIER_TIME = "2026-09-30T13:59:00.000000+00:00"
LATER_TIME = "2026-09-30T14:15:00.000000+00:00"

# Same order the job processes them in. A failing topic stops the run, so
# every topic before it must already be clean.
TOPICS = [
    "order-created",
    "payment-processed",
    "inventory-reserved",
    "order-confirmed",
    "order-cancelled",
]

FUTURE_RULE = "event_timestamp in the future"

# Rules with fixed names in the job (the rest are derived from its tables).
# Each maps to the overrides that break that rule and nothing else.
FIXED_RULES = {
    "order-created": {
        "total_amount <= 0": {"payload": {"total_amount": "0.00"}},
    },
    "payment-processed": {
        "amount < 0": {"payload": {"amount": "-5.00"}},
        "payment_failed without failure_reason": {
            "payload": {"status": "payment_failed", "failure_reason": None}},
        "payment_succeeded with failure_reason": {
            "payload": {"status": "payment_succeeded", "failure_reason": "card_declined"}},
    },
    "inventory-reserved": {
        "expires_at not after reserved_at": {
            "payload": {"reserved_at": GOOD_TIME, "expires_at": EARLIER_TIME}},
    },
}

GOOD_PAYLOAD = {
    "order-created": {
        "id": "x", "customer_id": "test-customer", "status": "pending",
        "total_amount": "10.00"},
    "payment-processed": {
        "order_id": "x", "status": "payment_succeeded", "amount": "10.00",
        "payment_method": "card", "provider_transaction_id": "txn_test",
        "failure_reason": None, "processed_at": GOOD_TIME},
    "inventory-reserved": {
        "order_id": "x", "status": "inventory_reserved",
        "reserved_at": GOOD_TIME, "expires_at": LATER_TIME},
    "order-confirmed": {
        "order_id": "x", "status": "confirmed",
        "payment_status": "succeeded", "inventory_status": "reserved"},
    "order-cancelled": {
        "order_id": "x", "status": "cancelled",
        "payment_status": "failed", "inventory_status": "unavailable"},
}

# order-created calls its identifier `id`; every other topic calls it order_id.
ID_KEY = {"order-created": "id"}

# Columns the job casts. Nulling these with real nulls would only test the
# not-null check; unparseable text tests the case that matters, a cast that
# silently returns null instead of raising.
UNPARSEABLE = {
    "amount": "12.3x", "total_amount": "12.3x",
    "processed_at": "garbage", "reserved_at": "garbage",
}

TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT", "EXPIRED"}


# --------------------------------------------------------------------------
# Rules, read from the job
# --------------------------------------------------------------------------

def load_job_rules():
    """Returns (required, accepted, fixed_names) parsed from the job source."""
    tree = ast.parse(JOB_SOURCE.read_text())
    tables = {}
    fixed = set()

    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in ("REQUIRED_AFTER_PARSE", "ACCEPTED_VALUES")):
            tables[node.targets[0].id] = ast.literal_eval(node.value)

        if isinstance(node, ast.FunctionDef) and node.name == "validity_rules":
            for call in ast.walk(node):
                if (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "append" and call.args
                        and isinstance(call.args[0], ast.Tuple)
                        and call.args[0].elts
                        and isinstance(call.args[0].elts[0], ast.Constant)
                        and isinstance(call.args[0].elts[0].value, str)):
                    fixed.add(call.args[0].elts[0].value)

    missing = {"REQUIRED_AFTER_PARSE", "ACCEPTED_VALUES"} - tables.keys()
    if missing:
        raise SystemExit(f"could not find {sorted(missing)} in {JOB_SOURCE}")
    return tables["REQUIRED_AFTER_PARSE"], tables["ACCEPTED_VALUES"], fixed


def check_in_sync(required, fixed):
    """Stops before touching AWS if the job has rules this test doesn't know."""
    known = {n for rules in FIXED_RULES.values() for n in rules} | {FUTURE_RULE}
    problems = []
    if fixed != known:
        problems.append(
            f"fixed rules differ. In the job but not the test: {sorted(fixed - known)}. "
            f"In the test but not the job: {sorted(known - fixed)}.")
    if set(required) != set(TOPICS):
        problems.append(f"topics differ: job has {sorted(required)}, test has {sorted(TOPICS)}.")
    if problems:
        raise SystemExit("test is out of sync with the job:\n  " + "\n  ".join(problems))


# --------------------------------------------------------------------------
# Rows
# --------------------------------------------------------------------------

def make_row(topic, offset, payload=None, kafka_timestamp=GOOD_TS):
    body = dict(GOOD_PAYLOAD[topic])
    body[ID_KEY.get(topic, "order_id")] = f"test-order-{offset}"
    body.update(payload or {})
    return {
        "topic": topic, "partition": PARTITION, "offset": offset,
        "kafka_timestamp": kafka_timestamp, "ingested_at": INGESTED_AT,
        "payload": body,
    }


def null_out(topic, column):
    if column == "event_timestamp":
        return {"kafka_timestamp": None}
    key = ID_KEY.get(topic, column) if column == "order_id" else column
    return {"payload": {key: UNPARSEABLE.get(column)}}


def invalid_value(topic, column):
    overrides = {column: "zz_invalid"}
    if (topic, column) == ("payment-processed", "failure_reason"):
        # A failure with a bad reason, so only the allowed-values rule trips
        # and not "succeeded with failure_reason".
        overrides["status"] = "payment_failed"
    return {"payload": overrides}


def build_plan(topic, required, accepted):
    """One bad row per rule, plus controls that must break nothing."""
    cases = []                                          # (rule name, row spec)
    for column in required[topic]:
        cases.append((f"{column} is null", null_out(topic, column)))
    for column, allowed in accepted.get(topic, {}).items():
        cases.append((f"{column} not in {sorted(allowed)}", invalid_value(topic, column)))
    for name, spec in FIXED_RULES.get(topic, {}).items():
        cases.append((name, spec))
    cases.append((FUTURE_RULE, {"kafka_timestamp": FUTURE_TS}))

    rows, controls, offset = [], [], 1
    controls.append(make_row(topic, offset)); offset += 1
    if topic == "order-cancelled":
        # inventory_status is null on 233 of the seed's 495 cancellations,
        # because payment can fail before inventory responds. It is valid by
        # design, so this control must NOT be flagged.
        controls.append(make_row(topic, offset, {"inventory_status": None})); offset += 1

    bad = []
    for name, spec in cases:
        bad.append((name, make_row(topic, offset, **spec))); offset += 1

    rows = controls + [row for _, row in bad]
    return {
        "rows": rows, "controls": controls, "bad": bad,
        "expected": {name: 1 for name, _ in bad},
        "invalid": len(bad),
    }


# --------------------------------------------------------------------------
# Reading the job's verdict
# --------------------------------------------------------------------------

FAILURE_RE = re.compile(
    r"(?P<topic>[a-z-]+): (?P<invalid>\d+)/(?P<total>\d+) rows \([\d.]+%\) "
    r"broke validity rules, above the limit of [\d.]+%: (?P<rules>.*)$", re.S)


def parse_failure(message):
    """-> (topic, invalid, total, {rule: count}) or None if it isn't our error."""
    m = FAILURE_RE.search(message or "")
    if not m:
        return None
    rules = {}
    for item in m["rules"].strip().split("; "):
        parsed = re.fullmatch(r"(.*) \((\d+)\)", item.strip(), re.S)
        if not parsed:
            return None
        rules[parsed.group(1)] = int(parsed.group(2))
    return m["topic"], int(m["invalid"]), int(m["total"]), rules


def judge(topic, plan, state, message):
    """Returns a list of problems; empty means this topic passed."""
    problems = []
    if state != "FAILED":
        return [f"job ended {state}; expected FAILED, so the bad rows were not rejected"]

    parsed = parse_failure(message)
    if parsed is None:
        return ["could not read the failure message (is it truncated, or a different "
                f"error?): {(message or '')[:400]}"]

    failed_topic, invalid, _total, rules = parsed
    if failed_topic != topic:
        problems.append(f"failed on {failed_topic}, expected {topic}")
    if invalid != plan["invalid"]:
        problems.append(f"{invalid} rows flagged invalid, expected {plan['invalid']} "
                        "(a control row was flagged, or a bad row slipped through)")

    expected = plan["expected"]
    for rule in expected:
        if rule not in rules:
            problems.append(f"rule never fired: {rule}")
    for rule, count in rules.items():
        if rule not in expected:
            problems.append(f"unexpected rule fired: {rule} ({count})")
        elif count != 1:
            problems.append(f"{rule} fired {count} times, expected 1")
    return problems


# --------------------------------------------------------------------------
# AWS
# --------------------------------------------------------------------------

def aws(*args, data=None):
    result = subprocess.run(["aws", *args], input=data, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:2])} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def terraform_output(name):
    result = subprocess.run(
        ["terraform", "-chdir=terraform", "output", "-raw", name],
        cwd=REPO, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"terraform output {name} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def object_key(topic, event_date):
    return f"bronze/topic_name={topic}/event_date={event_date}/{TEST_FILE}"


def start_job_run(job, region, timeout=300):
    """Starts a run, waiting out ConcurrentRunsExceededException.

    The job allows one run at a time, and Glue can refuse a new run for a short
    while after the previous one has already reported a terminal state. This
    was observed here: a run starting straight after a FAILED run was refused.
    That one error is retried, up to `timeout` seconds; any other error is not,
    so a genuine problem still fails immediately.
    """
    deadline = time.time() + timeout
    while True:
        try:
            return aws("glue", "start-job-run", "--job-name", job, "--region", region,
                       # explicit, so a locally edited default cannot weaken the test
                       "--arguments", json.dumps({"--MAX_INVALID_RATIO": "0"}),
                       "--query", "JobRunId", "--output", "text")
        except RuntimeError as error:
            if "ConcurrentRunsExceededException" not in str(error) or time.time() > deadline:
                raise
            log("   previous run still holds the job's slot; retrying in 10s")
            time.sleep(10)


def run_job(job, region):
    run_id = start_job_run(job, region)
    started = time.time()
    while True:
        time.sleep(15)
        state, message = json.loads(aws(
            "glue", "get-job-run", "--job-name", job, "--run-id", run_id,
            "--region", region, "--query", "JobRun.[JobRunState,ErrorMessage]",
            "--output", "json"))
        if state in TERMINAL:
            return state, message or "", time.time() - started


def log(text):
    print(text, flush=True)


# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--topic", action="append", choices=TOPICS,
                        help="test only this topic (repeatable); default is all")
    parser.add_argument("--event-date", default="2026-09-30",
                        help="an existing bronze partition date (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="build and print the plan; touch nothing in AWS")
    parser.add_argument("--skip-clean-run", action="store_true",
                        help="skip the final run that confirms the job recovers")
    args = parser.parse_args()
    topics = args.topic or TOPICS

    required, accepted, fixed = load_job_rules()
    check_in_sync(required, fixed)
    plans = {t: build_plan(t, required, accepted) for t in topics}

    log(f"{'topic':20} {'rules':>5} {'bad rows':>9} {'control rows':>13}")
    for t, p in plans.items():
        log(f"{t:20} {len(p['expected']):>5} {len(p['bad']):>9} {len(p['controls']):>13}")
    log(f"{'':20} {sum(len(p['expected']) for p in plans.values()):>5} rules in total")

    if args.dry_run:
        topic = topics[0]
        log(f"\nExample, {topic}:")
        for name, row in plans[topic]["bad"][:2]:
            log(f"  breaks only '{name}':\n    {json.dumps(row)}")
        return 0

    bucket = terraform_output("lake_bucket")
    region = terraform_output("aws_region")
    job = terraform_output("bronze_to_silver_job")

    log("\nRemoving leftovers from any interrupted run")
    for t in TOPICS:
        aws("s3", "rm", f"s3://{bucket}/{object_key(t, args.event_date)}", "--region", region)

    results = {}
    for topic in topics:
        plan = plans[topic]
        key = f"s3://{bucket}/{object_key(topic, args.event_date)}"
        log(f"\n== {topic}: injecting {plan['invalid']} bad rows + "
            f"{len(plan['controls'])} control row(s)")
        aws("s3", "cp", "-", key, "--region", region,
            data="".join(json.dumps(r) + "\n" for r in plan["rows"]))
        try:
            state, message, seconds = run_job(job, region)
        finally:
            aws("s3", "rm", key, "--region", region)    # always, pass or fail
        problems = judge(topic, plan, state, message)
        results[topic] = problems
        log(f"   job {state} after {seconds:.0f}s: " + ("PASS" if not problems else "FAIL"))
        for p in problems:
            log(f"   - {p}")

    if not args.skip_clean_run:
        log("\n== Control: run again with the injected rows removed")
        try:
            state, message, seconds = run_job(job, region)
            ok = state == "SUCCEEDED"
            results["clean rerun"] = [] if ok else [f"job ended {state}: {message[:300]}"]
            log(f"   job {state} after {seconds:.0f}s: " + ("PASS" if ok else "FAIL"))
        except RuntimeError as error:
            # Report it with the other results rather than losing them to a traceback.
            results["clean rerun"] = [str(error)]
            log(f"   could not run: {error}")

    log("\nSummary")
    for name, problems in results.items():
        log(f"  {'PASS' if not problems else 'FAIL'}  {name}")
    failed = [n for n, p in results.items() if p]
    log("\nAll checks passed." if not failed else f"\n{len(failed)} check(s) failed.")
    log("Now confirm nothing leaked into silver (should be 0 for every table):")
    log("  SELECT count(*) FROM <silver table> WHERE kafka_partition = 99")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())