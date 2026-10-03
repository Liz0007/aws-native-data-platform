"""Bronze -> Silver: parse, type, deduplicate, write Iceberg.

Bronze holds every event in one table, with an unparsed `payload` string and
a schema identical across topics. Silver splits that into one typed table per
topic, because once payload is parsed the schemas genuinely diverge.

Three things this job is responsible for:

  * Parsing. payload is JSON text at bronze; each topic gets an explicit
    schema here rather than letting Spark infer one, so a change upstream
    surfaces as a null column rather than a silently different table.

  * Deduplication. The upstream platform delivers at-least-once, so the same
    (partition, offset) can appear twice. Silver is keyed on that pair and
    written with MERGE INTO, which makes reruns idempotent by construction —
    important because the pipeline is schedule-driven and a window can be
    reprocessed.

    Duplicates are not identical rows: a redelivered event is archived
    again with a later ingested_at, so the copies differ in a non-key
    column. Picking one arbitrarily would make the output depend on Spark's
    scheduling, so the earliest ingested_at wins — the first time the
    platform observed the event. See dedupe().

  * File consolidation. Bronze arrives as ~300 JSON files averaging 10
    records each, because the upstream service flushes on a short timer.
    Writing Parquet through Iceberg produces far fewer, larger files — but
    as a side effect of how Spark writes (one file per Spark partition per
    Iceberg partition), not as compaction. write.target-file-size-bytes is
    a target honoured where Iceberg can, not a guarantee about any given
    write.

    Actual compaction is a separate, explicit operation: rewrite_data_files,
    run when --COMPACT is true. A table merged into repeatedly accumulates
    small files regardless of how any single write behaved, which is what
    compaction exists to fix.

Job arguments:
    --BRONZE_DATABASE   Glue database holding the crawled bronze table
    --BRONZE_TABLE      bronze table name
    --SILVER_DATABASE   Glue database the silver tables are created in
    --WAREHOUSE         s3://<bucket>/silver/ — Iceberg warehouse root
    --COMPACT           "true" to run rewrite_data_files after writing
    --MAX_INVALID_RATIO fraction of rows allowed to break validity rules
                        before the run fails; "0" means fail on any
    --MIN_BRONZE_ROWS   fail if bronze holds fewer rows than this
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_DATABASE", "BRONZE_TABLE", "SILVER_DATABASE",
     "WAREHOUSE", "COMPACT", "MAX_INVALID_RATIO", "MIN_BRONZE_ROWS"],
)

# Iceberg catalog name, configured in the job's --conf arguments (see the
# Terraform that declares this job).
CATALOG = "glue_catalog"

# Payload schemas, one per topic. Every field is read as a string and cast
# explicitly in PAYLOAD_COLUMNS below: the upstream services emit JSON where
# amounts and timestamps are already strings, and an explicit cast documents
# the intended type instead of leaving it to inference.
PAYLOAD_SCHEMAS = {
    "order-created": StructType([
        StructField("id", StringType()),
        StructField("customer_id", StringType()),
        StructField("status", StringType()),
        StructField("total_amount", StringType()),
    ]),
    "payment-processed": StructType([
        StructField("order_id", StringType()),
        StructField("status", StringType()),
        StructField("amount", StringType()),
        StructField("payment_method", StringType()),
        StructField("provider_transaction_id", StringType()),
        StructField("failure_reason", StringType()),
        StructField("processed_at", StringType()),
    ]),
    "inventory-reserved": StructType([
        StructField("order_id", StringType()),
        StructField("status", StringType()),
        StructField("reserved_at", StringType()),
        StructField("expires_at", StringType()),
    ]),
    "order-confirmed": StructType([
        StructField("order_id", StringType()),
        StructField("status", StringType()),
        StructField("payment_status", StringType()),
        StructField("inventory_status", StringType()),
    ]),
    "order-cancelled": StructType([
        StructField("order_id", StringType()),
        StructField("status", StringType()),
        StructField("payment_status", StringType()),
        StructField("inventory_status", StringType()),
    ]),
}

# Typed projections of the parsed payload. order-created calls its identifier
# `id` while every other topic calls it `order_id`; normalising that here
# means silver and gold can join on order_id without special cases.
#
# Each entry is a function returning the columns, not the columns
# themselves. pyspark's column functions (F.col, F.to_timestamp, ...) assert
# that a SparkContext is already active, and a module-level list would be
# built at import time — before main() creates one — failing with a bare
# AssertionError that names nothing.
PAYLOAD_COLUMNS = {
    "order-created": lambda: [
        F.col("p.id").alias("order_id"),
        F.col("p.customer_id").alias("customer_id"),
        F.col("p.status").alias("status"),
        F.col("p.total_amount").cast("decimal(12,2)").alias("total_amount"),
    ],
    "payment-processed": lambda: [
        F.col("p.order_id").alias("order_id"),
        F.col("p.status").alias("status"),
        F.col("p.amount").cast("decimal(12,2)").alias("amount"),
        F.col("p.payment_method").alias("payment_method"),
        F.col("p.provider_transaction_id").alias("provider_transaction_id"),
        F.col("p.failure_reason").alias("failure_reason"),
        F.to_timestamp("p.processed_at").alias("processed_at"),
    ],
    "inventory-reserved": lambda: [
        F.col("p.order_id").alias("order_id"),
        F.col("p.status").alias("status"),
        F.to_timestamp("p.reserved_at").alias("reserved_at"),
        F.to_timestamp("p.expires_at").alias("expires_at"),
    ],
    "order-confirmed": lambda: [
        F.col("p.order_id").alias("order_id"),
        F.col("p.status").alias("status"),
        F.col("p.payment_status").alias("payment_status"),
        F.col("p.inventory_status").alias("inventory_status"),
    ],
    "order-cancelled": lambda: [
        F.col("p.order_id").alias("order_id"),
        F.col("p.status").alias("status"),
        F.col("p.payment_status").alias("payment_status"),
        F.col("p.inventory_status").alias("inventory_status"),
    ],
}

# (kafka_partition, kafka_offset) uniquely identifies an event WITHIN A
# TOPIC, which is what makes the MERGE idempotent. It is sufficient here
# only because each topic gets its own silver table: offsets restart per
# topic, so the same pair recurs across topics. Combining topics into one
# table later would require adding `topic` to this key.
#
# Where duplicates carry different values, dedupe() decides which wins.
MERGE_KEYS = ["kafka_partition", "kafka_offset"]


# Columns this job reads from bronze. Checked at startup so a crawler that
# produced something different fails immediately, naming the problem, rather
# than surfacing as an AnalysisException deep in a transform.
REQUIRED_BRONZE_COLUMNS = {
    "topic_name", "payload", "partition", "offset", "kafka_timestamp", "ingested_at",
}

# Columns that must be populated after parsing, per topic. A null here means
# the payload did not match the declared schema — a cast failed, or a field
# was renamed upstream — which from_json reports as null rather than as an
# error. See quality_report().
REQUIRED_AFTER_PARSE = {
    "order-created": ["order_id", "customer_id", "total_amount", "event_timestamp"],
    "payment-processed": ["order_id", "status", "amount", "processed_at", "event_timestamp"],
    "inventory-reserved": ["order_id", "status", "reserved_at", "event_timestamp"],
    "order-confirmed": ["order_id", "status", "event_timestamp"],
    "order-cancelled": ["order_id", "status", "event_timestamp"],
}


def check_bronze(bronze: DataFrame) -> None:
    """Fails fast if bronze is not the shape this job expects."""
    missing = REQUIRED_BRONZE_COLUMNS - set(bronze.columns)
    if missing:
        raise ValueError(
            f"bronze is missing columns this job reads: {sorted(missing)}. "
            f"Found: {sorted(bronze.columns)}. Has the crawler run, and is "
            f"the partition named topic_name rather than topic?"
        )


def payload_as_json(bronze: DataFrame):
    """Returns payload as JSON text, whatever the crawler inferred it to be.

    The crawler reads JSON, so it usually infers `payload` as a struct of the
    union of all topics' fields rather than as a string. from_json() requires
    a string, so a struct has to be serialised back to JSON first. Handling
    both means the job works whether the table was crawled or declared, and
    whether a future crawl changes its mind.
    """
    dtype = bronze.schema["payload"].dataType
    if isinstance(dtype, StringType):
        return F.col("payload")
    print(f"payload inferred as {dtype.simpleString()}; serialising to JSON for parsing")
    return F.to_json(F.col("payload"))


def silver_table(topic: str) -> str:
    """order-created -> glue_catalog.<db>.silver_order_created"""
    return f"{CATALOG}.{ARGS['SILVER_DATABASE']}.silver_" + topic.replace("-", "_")


def build(bronze: DataFrame, topic: str) -> DataFrame:
    """Parses one topic out of bronze into its typed silver shape."""
    parsed = (
        bronze.filter(F.col("topic_name") == topic)
        .withColumn("p", F.from_json(payload_as_json(bronze), PAYLOAD_SCHEMAS[topic]))
    )

    df = parsed.select(
        # `partition` and `offset` are awkward to quote in SQL, so they are
        # renamed here rather than fought with in every downstream query.
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("bigint").alias("kafka_offset"),
        F.col("topic_name").alias("topic"),
        # Event time, from the Kafka record, not the time this pipeline saw
        # it. Partitioning on ingest time would move rows between partitions
        # on a backfill.
        (F.col("kafka_timestamp") / 1000).cast("timestamp").alias("event_timestamp"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        *PAYLOAD_COLUMNS[topic](),
    ).withColumn("event_date", F.to_date("event_timestamp"))

    return dedupe(df)


def dedupe(df: DataFrame) -> DataFrame:
    """Reduces each (kafka_partition, kafka_offset) to exactly one row.

    Needed for two reasons. MERGE INTO fails outright if the source holds
    two rows matching the same target row, which happens whenever a
    redelivered event lands in the same run. And the result must be
    deterministic: dropDuplicates() keeps an arbitrary row, so with rows
    that differ it would make the table depend on Spark's scheduling, and
    two runs over identical input could disagree.

    The copies do differ. A redelivered event is archived a second time
    with a later ingested_at, so that column varies between duplicates even
    though the event itself is the same.

    The earliest ingested_at wins: it records when the platform first
    observed the event, which is the more useful fact and is stable under
    reprocessing. Remaining ties mean the rows are genuinely identical, so
    any choice among them is the same choice.
    """
    ranked = Window.partitionBy(*MERGE_KEYS).orderBy(F.col("ingested_at").asc())
    return (
        df.withColumn("_rank", F.row_number().over(ranked))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# Allowed values per topic and column, taken from the upstream services that
# emit them. Nulls are not checked here — required columns are covered by
# REQUIRED_AFTER_PARSE, and optional ones (failure_reason) may be null.
#
# The order-confirmed entry doubles as a per-row consistency rule: a
# confirmed order can only have succeeded payment AND reserved inventory.
ACCEPTED_VALUES = {
    "order-created": {
        "status": {"pending"},
    },
    "payment-processed": {
        "status": {"payment_succeeded", "payment_failed"},
        "payment_method": {"card", "paypal", "bank_transfer"},
        "failure_reason": {"insufficient_funds", "card_declined", "provider_timeout"},
    },
    "inventory-reserved": {
        "status": {"inventory_reserved", "inventory_unavailable"},
    },
    "order-confirmed": {
        "status": {"confirmed"},
        "payment_status": {"succeeded"},
        "inventory_status": {"reserved"},
    },
    "order-cancelled": {
        "status": {"cancelled"},
        "payment_status": {"succeeded", "failed"},
        # Deliberately nullable, and not in REQUIRED_AFTER_PARSE. Payment and
        # inventory are processed in parallel upstream; when payment fails
        # first, the order is cancelled before the inventory result arrives,
        # so the event carries inventory_status = null. In the seed data that
        # is 233 of 495 cancellations. It is the saga's race condition
        # recorded faithfully, not a parsing failure.
        "inventory_status": {"reserved", "unavailable"},
    },
}

# Events stamped further ahead than this are treated as invalid. A small
# allowance absorbs clock skew between the upstream hosts and this job.
FUTURE_TOLERANCE = "INTERVAL 1 HOUR"


def validity_rules(topic: str) -> list[tuple[str, object]]:
    """Per-row rules for one topic, as (description, condition-is-invalid).

    Built inside a function rather than at module level because pyspark's
    column functions require an active SparkContext — see PAYLOAD_COLUMNS.

    These are structural checks on a single row. Rules spanning topics —
    every order reaching exactly one terminal state, payment amount matching
    the order total — belong in the dbt tests over silver, where they are
    plain SQL across tables.
    """
    rules = [(f"{c} is null", F.col(c).isNull()) for c in REQUIRED_AFTER_PARSE[topic]]

    for column, allowed in ACCEPTED_VALUES.get(topic, {}).items():
        rules.append((
            f"{column} not in {sorted(allowed)}",
            F.col(column).isNotNull() & ~F.col(column).isin(*sorted(allowed)),
        ))

    if topic == "order-created":
        rules.append(("total_amount <= 0", F.col("total_amount") <= 0))

    if topic == "payment-processed":
        rules.append(("amount < 0", F.col("amount") < 0))
        # A failure needs a reason, and a success must not carry one.
        rules.append((
            "payment_failed without failure_reason",
            (F.col("status") == "payment_failed") & F.col("failure_reason").isNull(),
        ))
        rules.append((
            "payment_succeeded with failure_reason",
            (F.col("status") == "payment_succeeded") & F.col("failure_reason").isNotNull(),
        ))

    if topic == "inventory-reserved":
        # expires_at is null for an unavailable reservation, so only checked when set.
        rules.append((
            "expires_at not after reserved_at",
            F.col("expires_at").isNotNull() & (F.col("expires_at") <= F.col("reserved_at")),
        ))

    rules.append((
        "event_timestamp in the future",
        F.col("event_timestamp") > F.current_timestamp() + F.expr(FUTURE_TOLERANCE),
    ))

    return rules


def quality_report(df: DataFrame, topic: str, max_invalid_ratio: float) -> DataFrame:
    """Applies validity_rules() to one topic and fails if too many rows break them.

    from_json() and casts never raise on bad input: an unmatched field
    becomes null, "12.3x" cast to decimal becomes null. So without explicit
    rules, a change upstream would land silently as nulls or nonsense values.

    Rows are counted rather than quarantined. Quarantining (writing bad rows
    to a rejects table and continuing) is the right answer once someone
    reviews the rejects; until then, failing the run is better than
    publishing bad rows into gold.
    """
    rules = validity_rules(topic)

    total = df.count()
    if total == 0:
        # A topic can legitimately be empty for a window — no cancellations,
        # say. An empty bronze as a whole is caught in main().
        print(f"WARNING {topic}: no rows in this batch")
        return df

    # One pass computes every rule's violation count.
    counts = df.select([
        F.sum(F.when(condition, 1).otherwise(0)).alias(f"r{i}")
        for i, (_, condition) in enumerate(rules)
    ]).collect()[0]

    any_invalid = None
    for _, condition in rules:
        any_invalid = condition if any_invalid is None else (any_invalid | condition)
    invalid = df.filter(any_invalid)
    invalid_count = invalid.count()
    ratio = invalid_count / total

    print(f"{topic}: {total} rows, {invalid_count} invalid ({ratio:.2%}), {len(rules)} rules")

    broken = [(name, counts[f"r{i}"]) for i, (name, _) in enumerate(rules) if counts[f"r{i}"]]
    for name, n in broken:
        print(f"    {n:>6}  {name}")
    if invalid_count:
        print("    sample:")
        for row in invalid.limit(3).collect():
            print(f"      {row}")

    if ratio > max_invalid_ratio:
        raise ValueError(
            f"{topic}: {invalid_count}/{total} rows ({ratio:.2%}) broke validity rules, "
            f"above the limit of {max_invalid_ratio:.2%}: "
            + "; ".join(f"{name} ({n})" for name, n in broken)
        )

    return df


def delta(target: DataFrame, source: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Splits the source into rows to insert and rows to update.

    Exists because the MERGE's `WHEN MATCHED AND (<changed>)` guard does not
    stop rewrites in practice. Verified against this table's snapshot
    history: a rerun over identical data committed an `overwrite` snapshot
    that deleted and re-added every row. Under Iceberg's copy-on-write
    MERGE, the files to rewrite are selected by the ON clause, and on a
    full reprocess every key matches — so every file is rewritten with its
    unchanged rows carried through. The AND condition decides what a row
    becomes, not whether its file is touched.

    Computing the delta here instead means the MERGE only ever sees rows
    that are new or genuinely different, so only files holding those rows
    are rewritten, and an unchanged rerun commits nothing at all.

    inserts: keys not in the target
    updates: keys in the target whose non-key values differ, compared with
             null-safe equality so two nulls count as the same value
    """
    columns = source.columns
    non_key = [c for c in columns if c not in MERGE_KEYS]

    inserts = source.join(target.select(*MERGE_KEYS), on=MERGE_KEYS, how="left_anti")

    s, t = source.alias("s"), target.alias("t")
    joined = s.join(t, on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in MERGE_KEYS], how="inner")

    if not non_key:
        updates = source.limit(0)
    else:
        differs = None
        for c in non_key:
            cond = ~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
            differs = cond if differs is None else (differs | cond)
        updates = joined.filter(differs).select(*[F.col(f"s.{c}").alias(c) for c in columns])

    return inserts.select(*columns), updates


def merge_sql(table: str, columns: list[str]) -> str:
    """Builds the MERGE statement with explicit column lists.

    Deliberately not `UPDATE SET *` / `INSERT *`. Two reasons:

    * The shorthand requires source and target schemas to line up, and fails
      in a hard-to-read way when they drift — for instance after a new field
      is added to PAYLOAD_COLUMNS but the existing table has not been
      evolved. write() checks for that drift explicitly and reports it.

    * Naming the columns puts the real statement in the job log, which is
      what you want when working out why a row did or did not change.

    The MATCHED branch is also conditional. Events are immutable, so on a
    full reprocess every row matches, and an unconditional update would
    rewrite every data file on every run — churning snapshots and costing
    I/O to write identical values back. The condition compares each non-key
    column with <=> (null-safe equality), so an update happens only when
    something genuinely differs, which is the case that matters: a rerun
    after fixing a cast or a schema in this job.
    """
    keys = MERGE_KEYS
    payload = [c for c in columns if c not in keys]

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    insert_cols_only = ", ".join(columns)
    insert_vals_only = ", ".join(f"s.{c}" for c in columns)

    if not payload:
        # Nothing outside the key to compare or assign: an empty MATCHED
        # condition would be invalid SQL, and there is nothing an update
        # could change. Insert-only is the whole statement.
        return f"""
        MERGE INTO {table} t
        USING updates s
        ON {on}
        WHEN NOT MATCHED THEN INSERT ({insert_cols_only}) VALUES ({insert_vals_only})
    """

    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in payload)
    assignments = ", ".join(f"t.{c} = s.{c}" for c in payload)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)

    return f"""
        MERGE INTO {table} t
        USING updates s
        ON {on}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def write(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Creates the Iceberg table on first run, merges into it thereafter."""
    exists = spark.catalog.tableExists(table)

    if not exists:
        (
            df.writeTo(table)
            .using("iceberg")
            .partitionedBy(F.col("event_date"))
            .tableProperty("format-version", "2")
            # A target honoured where Iceberg can, during both writes and
            # rewrites — not a guarantee about any individual write.
            .tableProperty("write.target-file-size-bytes", "134217728")
            .create()
        )
        print(f"created {table} with {df.count()} rows")
        return

    # Fail clearly if this job now produces columns the table does not have,
    # rather than letting the MERGE report a confusing mismatch. Evolving
    # the table (ALTER TABLE ... ADD COLUMN) is a deliberate decision, not
    # something a nightly run should do silently.
    target = set(spark.table(table).columns)
    source = set(df.columns)
    if source - target:
        raise ValueError(
            f"{table} is missing columns this job produces: "
            f"{sorted(source - target)}. Evolve the table before rerunning, "
            f"e.g. ALTER TABLE {table} ADD COLUMN <name> <type>."
        )
    if target - source:
        print(f"note: {table} has columns this job no longer writes: {sorted(target - source)}")

    inserts, updates = delta(spark.table(table), df)
    n_inserts, n_updates = inserts.count(), updates.count()

    if n_inserts == 0 and n_updates == 0:
        # Nothing to write, so no MERGE and no snapshot. See delta() for why
        # this check exists rather than relying on the MERGE's own guard.
        print(f"{table}: unchanged ({df.count()} rows already present), nothing written")
        return

    inserts.unionByName(updates).createOrReplaceTempView("updates")
    spark.sql(merge_sql(table, df.columns))
    print(f"{table}: inserted {n_inserts}, updated {n_updates}")


def compact(spark: SparkSession, table: str) -> None:
    """Rewrites small data files into larger ones.

    This is the explicit compaction step. Writes alone do not compact: each
    run adds its own files, so a table merged into repeatedly accumulates
    small files even when every individual write was reasonably sized.

    rewrite_data_files is an Iceberg stored procedure, available because the
    job loads the Iceberg Spark extensions. The default bin-pack strategy
    combines files below the table's target size; min-input-files => 2 stops
    it rewriting a partition that already holds a single file.
    """
    # The procedure takes the table identifier without the catalog prefix.
    identifier = table.split(".", 1)[1]

    result = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{identifier}',
            options => map('min-input-files', '2')
        )
    """).collect()

    if result:
        row = result[0]
        print(
            f"compacted {table}: {row['rewritten_data_files_count']} files "
            f"rewritten into {row['added_data_files_count']} "
            f"({row['rewritten_bytes_count']} bytes)"
        )


def file_count(spark: SparkSession, table: str) -> int:
    """Data files currently in the table, from Iceberg's own metadata."""
    return spark.sql(f"SELECT count(*) AS n FROM {table}.files").collect()[0]["n"]


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(ARGS["JOB_NAME"], ARGS)

    source = f"{ARGS['BRONZE_DATABASE']}.{ARGS['BRONZE_TABLE']}"
    print(f"reading {source}")

    # Read through the Glue Data Catalog rather than from an S3 path, so the
    # crawler's partition metadata is used and `topic_name` / `event_date`
    # arrive as columns.
    bronze = spark.table(source)

    # Full reprocess every run. That is safe because the write is a MERGE on
    # (kafka_partition, kafka_offset), and cheap at this volume. For a larger
    # corpus the read would be narrowed with a predicate on the bronze
    # partitions, e.g. .filter(F.col("event_date") >= <watermark>), rather
    # than relying on Glue bookmarks, which do not apply to spark.table reads.
    check_bronze(bronze)

    # An empty bronze means the upstream export or upload failed, not that
    # there was nothing to do: fail rather than report a successful no-op.
    min_rows = int(ARGS["MIN_BRONZE_ROWS"])
    bronze_rows = bronze.count()
    if bronze_rows < min_rows:
        raise ValueError(
            f"bronze has {bronze_rows} rows, fewer than MIN_BRONZE_ROWS={min_rows}. "
            f"Check the upload to s3 and that the crawler has registered partitions."
        )
    print(f"bronze: {bronze_rows} rows")

    should_compact = ARGS["COMPACT"].lower() == "true"
    max_invalid_ratio = float(ARGS["MAX_INVALID_RATIO"])

    for topic in PAYLOAD_SCHEMAS:
        table = silver_table(topic)
        df = build(bronze, topic)
        quality_report(df, topic, max_invalid_ratio)
        write(spark, df, table)

        if should_compact:
            before = file_count(spark, table)
            compact(spark, table)
            print(f"{table}: {before} data files -> {file_count(spark, table)}")

    job.commit()


if __name__ == "__main__":
    main()