"""Control-flow tests for the close-batch state machine.

Renders the real template and runs it through a small interpreter
(tests/asl_sim.py) with the AWS tasks mocked. This proves the branching,
the polling budgets and the failure states. It does NOT prove that the AWS
SDK integrations accept the parameters: only a real execution does that
(see scripts/test_ingestion.py).

    python -m unittest tests.test_close_batch_state_machine -v
"""
import json
import pathlib
import re
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import asl_sim  # noqa: E402

TEMPLATE = pathlib.Path(__file__).resolve().parents[1] / "terraform" / "ingest_state_machine.asl.json.tftpl"
VARS = {"bucket": "b", "crawler_name": "c", "max_landing_polls": "15", "max_crawler_polls": "20"}

READY = {"State": "READY", "LastCrawl": {"Status": "SUCCEEDED"}}
RUNNING = {"State": "RUNNING", "LastCrawl": {"Status": "SUCCEEDED"}}


class CrawlerRunning(Exception):
    code = "Glue.CrawlerRunningException"


def load():
    raw = TEMPLATE.read_text()
    unknown = set(re.findall(r"\$\{(\w+)\}", raw)) - set(VARS)
    assert not unknown, f"template uses undeclared variables: {unknown}"
    return json.loads(re.sub(r"\$\{(\w+)\}", lambda m: VARS[m.group(1)], raw))


def execute(input_doc, landing, quarantine, crawler, start_raises=False):
    landing_it, crawler_it, calls = iter(landing), iter(crawler), []

    def s3_list(p):
        return {"KeyCount": next(landing_it) if p["Prefix"] == "landing/" else quarantine}

    def start(_p):
        calls.append("startCrawler")
        if start_raises:
            raise CrawlerRunning()
        return {}

    def put(_p):
        calls.append("putObject")
        return {}

    env = {"s3:listObjectsV2": s3_list, "glue:startCrawler": start,
           "glue:getCrawler": lambda _p: {"Crawler": next(crawler_it)}, "s3:putObject": put}
    outcome, _trace = asl_sim.run(load(), env, dict(input_doc))
    return outcome, calls


class CloseBatchTests(unittest.TestCase):
    def test_clean_batch_writes_the_marker(self):
        outcome, calls = execute({"max_quarantined": 0}, [3, 1, 0], 0, [RUNNING, READY])
        self.assertEqual(outcome, "SUCCEEDED")
        self.assertEqual(calls, ["startCrawler", "putObject"])

    def test_missing_max_quarantined_defaults_to_zero(self):
        self.assertEqual(execute({}, [0], 0, [READY])[0], "SUCCEEDED")
        self.assertEqual(execute({}, [0], 1, [READY])[0], "FAIL:QuarantineNotEmpty")

    def test_quarantined_file_stops_the_batch_before_the_crawler_and_marker(self):
        outcome, calls = execute({"max_quarantined": 0}, [0], 1, [])
        self.assertEqual(outcome, "FAIL:QuarantineNotEmpty")
        self.assertEqual(calls, [])

    def test_quarantine_up_to_the_limit_is_allowed(self):
        self.assertEqual(execute({"max_quarantined": 1}, [0], 1, [READY])[0], "SUCCEEDED")

    def test_landing_that_never_drains_fails_after_the_budget(self):
        outcome, calls = execute({"max_quarantined": 0}, [5] * 100, 0, [])
        self.assertEqual((outcome, calls), ("FAIL:LandingNotDrained", []))

    def test_crawler_already_running_is_waited_for_not_treated_as_an_error(self):
        outcome, calls = execute({"max_quarantined": 0}, [0], 0, [RUNNING, READY], start_raises=True)
        self.assertEqual(outcome, "SUCCEEDED")

    def test_failed_crawl_writes_no_marker(self):
        failed = {"State": "READY", "LastCrawl": {"Status": "FAILED"}}
        outcome, calls = execute({"max_quarantined": 0}, [0], 0, [failed])
        self.assertEqual(outcome, "FAIL:CrawlerFailed")
        self.assertNotIn("putObject", calls)

    def test_crawler_that_never_finishes_times_out_without_a_marker(self):
        outcome, calls = execute({"max_quarantined": 0}, [0], 0, [RUNNING] * 100)
        self.assertEqual(outcome, "FAIL:CrawlerTimedOut")
        self.assertNotIn("putObject", calls)


if __name__ == "__main__":
    unittest.main()
