# Three catalog databases, one per medallion layer. They hold table
# metadata only; the data lives in S3.

resource "aws_glue_catalog_database" "bronze" {
  name        = local.bronze_db
  description = "Raw events exactly as exported, schema discovered by crawler"
}

resource "aws_glue_catalog_database" "silver" {
  name        = local.silver_db
  description = "Flattened, typed, deduplicated Iceberg tables — one per topic"
}

resource "aws_glue_catalog_database" "gold" {
  name        = local.gold_db
  description = "Business aggregates built by dbt"
}

# Bronze is crawled rather than declared, because its schema is genuinely
# unknown to this project — it is whatever the upstream platform emitted.
# Silver and gold are Iceberg, which registers its own tables in the
# catalog, so no crawler is needed there. Demonstrating both, and knowing
# which case each fits, is the point.
resource "aws_glue_crawler" "bronze" {
  name          = "${local.name_prefix}-bronze"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Discovers the raw event schema and its topic/date partitions"

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/bronze/"
  }

  # Without this, the crawler creates one table per topic folder. Combining
  # them gives a single table partitioned by topic and event_date, which is
  # what the Hive-style prefixes written by upload_seed_to_bronze.sh imply.
  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG" # never drop a table because a crawl saw no files
  }
}
