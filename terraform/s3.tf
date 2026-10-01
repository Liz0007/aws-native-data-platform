# One bucket, prefixed by purpose. Separate buckets per layer would be
# equally valid; one keeps IAM and teardown simple, and the prefixes are
# what the Glue catalog and Athena actually address.
#
#   bronze/  raw JSONL exactly as exported, Hive-partitioned by topic/date
#   silver/  Iceberg tables, flattened and typed, one per topic
#   gold/    Iceberg tables built by dbt
#   scripts/ PySpark job code, uploaded by Terraform
#   athena-results/ query output, required by Athena

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "lake" {
  bucket = "${local.name_prefix}-lake-${random_id.suffix.hex}"

  # Lets `terraform destroy` remove the bucket with objects still in it —
  # otherwise teardown fails and the bucket is silently left behind.
  force_destroy = true

  tags = { Name = "${local.name_prefix}-lake" }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# Athena results accumulate on every query and are never read again once
# the result is returned, so they expire quickly.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
}
