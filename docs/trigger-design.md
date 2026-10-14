# How data gets into bronze, and what triggers the transform

This replaces an earlier version of this note that had Step Functions
triggering the whole pipeline. The design changed once Airflow took over
bronze → silver → gold, and Lambda and Step Functions were moved to the stage
before bronze.

## The obvious design, and why it fails

    new object in S3  ->  S3 event  ->  start the pipeline

That works when files arrive occasionally. It does not work here. The
upstream platform's `data-pipeline` service flushes every 10 seconds or 100
records across five topics. Measured on the seed export: **11,296 events in
296 files over roughly 10 minutes**, about 30 files a minute, or ~43,000
objects a day.

One Glue run per object would mean ~43,000 job starts a day, each with a
1–2 minute startup, overlapping permanently, for a handful of records each.

## What this project does instead

Two different kinds of work, two different tools.

**Per file: Lambda.** Every object landing under `landing/` triggers a
Lambda that validates the file and routes it. At 43,000 invocations a day
that is cheap, and files must be checked as they arrive.

| Outcome | Where it goes |
|---|---|
| Valid | `bronze/topic_name=<topic>/event_date=<date>/<file>` |
| Structurally invalid | `quarantine/<topic>/<date>/<file>`, with the reasons in `quarantine-reasons/` |

**Per batch: Step Functions.** A state machine, run hourly if enabled or
started by hand, closes the batch:

1. Wait until `landing/` is empty (the Lambda has routed everything), within
   a polling budget.
2. Fail with `QuarantineNotEmpty` if any file is quarantined. A quarantined
   file means the bronze data is incomplete, so the batch must not be
   announced as ready.
3. Run the bronze crawler and wait for it, so new partitions are in the
   catalog before anything reads them.
4. Write `markers/bronze/<execution>.success`.

**Transform: Airflow.** `ecom_pipeline` waits for the marker, then runs the
Glue job and dbt. Only one scheduler ever starts the Glue job, so the job's
one-concurrent-run limit is never contended.

## What each layer checks

| Layer | Checks | Failure |
|---|---|---|
| Ingestion (Lambda) | Structure: each line is a JSON object with the six envelope fields, right types, topic matches the path, parseable timestamp | File quarantined whole |
| Silver (Glue) | Content: 40 validity rules on parsed values | Run fails, names the rule |
| Gold (dbt) | Business logic across tables | Test fails |

A record with a null order id is *structurally* fine, so it reaches bronze
and is stopped at silver. Bronze therefore keeps everything that was
well-formed when it arrived, which is what makes it replayable.

## Decisions worth defending

- **event_date comes from the record's `ingested_at` (UTC)**, not from when
  the file arrived. Replaying a file always lands it in the same partition.
  A file whose records cross midnight is split into two partitions.
- **Whole-file accept or reject.** Splitting good lines from bad would make
  bronze depend on a partial reading of what the producer wrote as a unit.
- **Idempotent routing.** Bronze keys depend only on the file name and the
  records. A replay overwrites the same objects, and a duplicate S3 event
  for an already-routed file finds nothing in `landing/` and does nothing.
- **The marker is consumed by exact key set.** Airflow snapshots the markers
  that exist when it starts, and deletes only those after dbt succeeds. A
  batch closed during a run survives to trigger the next run.
- **Quarantine must be cleared by a person.** The state machine refuses
  while quarantine holds files. That is deliberate: it turns a bad file into
  a loud failure instead of a silent gap.

## Known limits

- The crawler wait trusts `State == READY` and `LastCrawl.Status ==
  SUCCEEDED` after a 30-second initial wait. A crawl that has not yet moved
  out of READY after 30 seconds could be mistaken for a finished one.
- Quarantine is counted in total, not per batch, so it needs to be emptied
  before the next batch can close.
- Landing files above 25 MB are quarantined rather than read; the producer's
  files are tens of kilobytes.
- Lambda failures that are not validation failures (throttling, permissions)
  propagate for Lambda's own retries. There is no dead-letter queue; a file
  that keeps failing stays in `landing/`, and the state machine reports
  `LandingNotDrained`.
