# The Glue job that builds silver from bronze.
#
# Terraform declares the job — its role, Glue version, worker count and the
# S3 location of its script — but the PySpark code itself is application
# code. aws_s3_object uploads it so a single `terraform apply` keeps the two
# in step; without it the job would point at a script that may be stale.

resource "aws_s3_object" "bronze_to_silver" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../glue/jobs/bronze_to_silver.py"

  # Re-uploads whenever the script changes, rather than only when the key
  # changes. Without this, edits to the job would never reach S3.
  etag = filemd5("${path.module}/../glue/jobs/bronze_to_silver.py")
}

resource "aws_glue_job" "bronze_to_silver" {
  name        = "${local.name_prefix}-bronze-to-silver"
  role_arn    = aws_iam_role.glue.arn
  description = "Parses, types and deduplicates bronze events into Iceberg silver tables"

  glue_version      = "5.0" # ships Spark 3.5 and the Iceberg runtime
  worker_type       = "G.1X"
  number_of_workers = 2     # minimum; the dataset is megabytes
  timeout           = 20    # minutes — a normal run takes 2-3

  command {
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.bronze_to_silver.key}"
    python_version  = "3"
  }

  execution_property {
    # A scheduled pipeline can fire again while the previous run is still
    # going. Capping concurrency at 1 makes the later run fail fast instead
    # of two jobs writing the same Iceberg tables at once.
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--JOB_NAME"        = "${local.name_prefix}-bronze-to-silver"
    "--BRONZE_DATABASE" = aws_glue_catalog_database.bronze.name
    "--BRONZE_TABLE"    = "bronze"
    "--SILVER_DATABASE" = aws_glue_catalog_database.silver.name
    "--WAREHOUSE"       = "s3://${aws_s3_bucket.lake.bucket}/silver/"

    # Compaction is separate from writing and pointless on a first load, so
    # it is off by default and enabled per run:
    #   aws glue start-job-run --arguments '{"--COMPACT":"true"}' ...
    "--COMPACT" = "false"

    # Fail the run if any row comes out with nulls in its required fields.
    # from_json and casts both return null rather than raising, so without a
    # threshold a schema change upstream would land silently as a silver
    # table full of nulls. Raise this only with a reason.
    "--MAX_INVALID_RATIO" = "0"

    # An empty bronze means a failed upload or an unregistered partition,
    # not a quiet window, so the run fails instead of succeeding as a no-op.
    "--MIN_BRONZE_ROWS" = "1"

    # Makes Spark's default catalog resolve tables through the Glue Data
    # Catalog. Without it, spark.table("<db>.<table>") searches an empty
    # local metastore and the crawled bronze table is invisible. This is
    # separate from the Iceberg glue_catalog configured below, which the
    # job uses only for writing silver.
    "--enable-glue-datacatalog" = "true"

    # Loads the Iceberg runtime into the job. Without it the Spark configs
    # below fail with a class-not-found error that doesn't name Iceberg.
    "--datalake-formats" = "iceberg"

    # Registers an Iceberg catalog called glue_catalog, backed by the Glue
    # Data Catalog for metadata and S3 for data. This is what lets the job
    # write to glue_catalog.<db>.<table> and have the table appear in the
    # catalog without a crawler.
    "--conf" = join(" --conf ", [
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lake.bucket}/silver/",
      "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    ])

    # Bookmarks are disabled deliberately: they apply to Glue DynamicFrame
    # reads, not the spark.table read this job uses. Idempotency comes from
    # the MERGE on (kafka_partition, kafka_offset) instead, so a full
    # reprocess is safe — see the job's module docstring.
    "--job-bookmark-option" = "job-bookmark-disable"

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.lake.bucket}/spark-logs/"
  }
}