output "aws_region" {
  value = var.aws_region
}

output "lake_bucket" {
  description = "Bucket holding bronze/silver/gold, scripts and Athena results."
  value       = aws_s3_bucket.lake.bucket
}

output "bronze_database" {
  value = aws_glue_catalog_database.bronze.name
}

output "silver_database" {
  value = aws_glue_catalog_database.silver.name
}

output "gold_database" {
  value = aws_glue_catalog_database.gold.name
}

output "bronze_crawler" {
  value = aws_glue_crawler.bronze.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "bronze_to_silver_job" {
  description = "Start it with: aws glue start-job-run --job-name <this>"
  value       = aws_glue_job.bronze_to_silver.name
}
