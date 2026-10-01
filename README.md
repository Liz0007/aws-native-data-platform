# AWS-Native Data Platform

A medallion-architecture analytics pipeline built on AWS managed services:
raw events land in S3, Glue transforms them into Iceberg tables, dbt builds
business aggregates, and Step Functions orchestrates the whole thing.

Built with Terraform, AWS Glue (PySpark), Apache Iceberg, Athena, dbt,
Lambda and Step Functions.

## Where the data comes from

The source is real event data from a companion project,
[real-time-ecommerce-platform](https://github.com/<your-username>/real-time-ecommerce-platform)
— a Kafka-based event-driven system whose `data-pipeline` service archives
every event to a data lake. That archive is exported here as seed data:
**11,296 events across five topics**, covering 2,824 complete order
lifecycles.

Using genuine events rather than a synthetic dataset means the schemas,
the duplicate-delivery semantics, and the small-files problem are all real
rather than contrived. The seed is committed, so this repo stands alone
with no runtime dependency on the other project.

| Topic | Records |
|---|---|
| `order-created` | 2,824 |
| `payment-processed` | 2,824 |
| `inventory-reserved` | 2,824 |
| `order-confirmed` | 2,329 |
| `order-cancelled` | 495 |

## Architecture

```
data/seed/lake/            (committed seed, exported from the platform)
        │
        ▼
   S3  bronze/             raw JSONL, Hive-partitioned by topic_name + event_date
        │                  → Glue Crawler → Glue Data Catalog
        ▼
   Glue PySpark job        flatten · type · dedupe on (topic, partition, offset)
        │
        ▼
   S3  silver/             Iceberg tables, one per topic, MERGE INTO
        │
        ▼
   dbt (dbt-athena)        SQL models + tests
        │
        ▼
   S3  gold/               Iceberg aggregates: daily metrics, funnel, failures
        │
        ▼
   Athena                  ad-hoc queries over every layer

   EventBridge schedule ──► Step Functions ──► Glue job ──► dbt
   S3 _SUCCESS marker ──► Lambda ──┘
```

## Key design decisions

### Bronze is one table; silver is five

Every topic shares the same envelope at bronze — `topic`, `partition`,
`offset`, `kafka_timestamp`, `ingested_at`, and an unparsed `payload`. Five
tables would differ only in name, so bronze is a single table with `topic`
as a partition column. Because it is a *partition*, filtering on it prunes
rather than scans.

Silver parses `payload`, and there the schemas genuinely diverge:
`order-created` carries `customer_id` and `total_amount`,
`payment-processed` carries `failure_reason` and `provider_transaction_id`.
Those don't belong in one table.

### Partition keys must not collide with record fields

Bronze is partitioned by `topic_name`, not `topic`, because every record
already contains a `topic` field. Partitioning by `topic=` makes the
crawler register the column twice — once from the data, once from the
prefix — and Athena then rejects the table outright with
`HIVE_INVALID_METADATA: Table descriptor contains duplicate columns`.

Renaming the partition key is the correct fix rather than stripping the
field from the data: bronze's purpose is to preserve exactly what the
upstream platform emitted, and the duplication is genuinely present in the
source.

### A crawler on bronze, Iceberg everywhere else

Bronze's schema is genuinely unknown to this project — it is whatever the
upstream platform emitted — so a crawler discovers it. Silver and gold are
schemas this project owns and defines, and Iceberg registers its own tables
in the Glue Catalog, so no crawler is needed. Knowing which case each tool
fits matters more than using both everywhere.

### Iceberg for silver and gold

- `MERGE INTO` on `(topic, partition, offset)` makes reruns idempotent by
  construction, rather than relying on a dedupe step being remembered. The
  upstream platform delivers at-least-once, so duplicates are a real
  possibility, not a hypothetical.
- `OPTIMIZE ... REWRITE DATA USING BIN_PACK` addresses a genuine
  small-files problem: the seed arrives as ~300 files averaging 10 records
  each, because the upstream service flushes on a short interval.
- Snapshots give time travel and a readable commit history.

### Scheduled batches, not per-object triggers

The obvious design — S3 `PutObject` triggers Lambda triggers the pipeline —
does not survive contact with a real stream. The upstream service flushes
every 10 seconds across five topics, roughly 43,000 objects a day. One
pipeline run per object would mean 43,000 Glue job starts, each with a
1–2 minute startup, overlapping constantly.

So the primary trigger is an **EventBridge schedule** running micro-batches,
with **Glue job bookmarks** so each run reads only new files. The
S3 → Lambda path is kept, but fires on a `_SUCCESS` completion marker
rather than on every object, which preserves event-driven invocation
without the volume problem. Step Functions guards against overlapping
executions, since a run can outlast its interval.

This is the main reason the pipeline is idempotent: a schedule can
reprocess a window, and `MERGE INTO` makes that harmless.

## Status

🚧 In progress.

| Phase | Status |
|---|---|
| Seed data exported from the platform | ✅ |
| Terraform: S3, IAM, Glue Catalog, bronze crawler, Athena | 🚧 |
| Glue job: bronze → silver (Iceberg) | ⬜ |
| dbt: silver → gold | ⬜ |
| Step Functions + Lambda + EventBridge | ⬜ |
| Redshift Serverless | ⬜ optional |
| Airflow (self-hosted, alternative orchestrator) | ⬜ optional |

## Cost

Everything except Redshift is serverless and effectively free at this
volume: S3 holds megabytes, Athena bills $5/TB scanned, Glue bills
~$0.44/DPU-hour for runs lasting minutes, and Lambda and Step Functions sit
inside the free tier. A full build-and-demo cycle costs a few dollars.

Redshift Serverless is the exception at roughly $2.88/hour while active,
which is why it is scoped as a separate, timeboxed phase rather than a
standing part of the stack.