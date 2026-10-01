# A dedicated workgroup rather than the account's default: it pins the
# result location, enforces that setting on every query, and gives this
# project its own query history and cost attribution.

resource "aws_athena_workgroup" "main" {
  name        = local.name_prefix
  description = "Queries against the ${local.name_prefix} lake"

  configuration {
    # Stops a client-side setting from redirecting results elsewhere.
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }

    # Athena bills per TB scanned. This dataset is megabytes, so a 1 GB cap
    # per query is unreachable in normal use but bounds a mistake such as an
    # accidental cross join.
    bytes_scanned_cutoff_per_query = 1073741824
  }

  force_destroy = true # allow destroy while the workgroup holds query history
}
