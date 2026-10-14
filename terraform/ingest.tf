# Ingestion: everything that happens BEFORE data is in bronze.
#
#   landing/   producers drop JSONL here
#     │  S3 ObjectCreated  ──►  Lambda (validate + route, one file at a time)
#     ├──► bronze/       valid files, Hive-partitioned by topic_name + event_date
#     └──► quarantine/   structurally invalid files (reasons in quarantine-reasons/)
#
#   Step Functions "close batch" (hourly if enabled, or started by hand):
#     wait for landing/ to drain → refuse if anything is quarantined →
#     run the bronze crawler → write markers/bronze/<execution>.success
#
#   Airflow waits for that marker before it transforms bronze.
#
# Lambda reacts to each file (cheap, and files must be checked as they land);
# Step Functions acts once per batch (waiting, polling and failing loudly are
# what a state machine is for). Starting Glue per file is what does not scale,
# which is why the marker, not the file, is the trigger for the transform.

variable "ingest_schedule_enabled" {
  description = "Run the close-batch state machine hourly. Off by default so a demo account is not billed for idle crawler runs."
  type        = bool
  default     = false
}

locals {
  ingest_lambda_name = "${local.name_prefix}-ingest-validate"
  close_batch_name   = "${local.name_prefix}-close-bronze-batch"

  # Polling budgets for the state machine: 15 x 20 s for landing to drain,
  # 20 x 30 s for the crawler. Exceeding either fails the execution loudly.
  max_landing_polls = 15
  max_crawler_polls = 20
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------- Lambda

data "archive_file" "ingest_validate" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/ingest_validate"
  output_path = "${path.module}/.build/ingest_validate.zip"
  excludes    = ["__pycache__"]
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest_lambda" {
  name               = "${local.ingest_lambda_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_logs" {
  role       = aws_iam_role.ingest_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "ingest_lambda_s3" {
  # Read the landing file and, once routed, remove it. The same GetObject on
  # landing/ is what lets copy_object quarantine a file server-side.
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/landing/*"]
  }

  # Read-only on bronze: before writing a bronze key the handler compares any
  # existing object, so two different files with the same name cannot silently
  # overwrite each other.
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*"]
  }

  statement {
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/quarantine/*",
      "${aws_s3_bucket.lake.arn}/quarantine-reasons/*",
    ]
  }

  # Without ListBucket, S3 answers a read of a missing key with AccessDenied
  # instead of NoSuchKey, and the "duplicate event, file already routed" case
  # would look like a permissions failure and be retried forever.
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "ingest_lambda_s3" {
  name   = "${local.ingest_lambda_name}-s3"
  role   = aws_iam_role.ingest_lambda.id
  policy = data.aws_iam_policy_document.ingest_lambda_s3.json
}

# Declared explicitly so `terraform destroy` removes the logs too and the
# retention is bounded; otherwise Lambda creates the group itself and keeps
# it forever.
resource "aws_cloudwatch_log_group" "ingest_lambda" {
  name              = "/aws/lambda/${local.ingest_lambda_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "ingest_validate" {
  function_name = local.ingest_lambda_name
  role          = aws_iam_role.ingest_lambda.arn
  description   = "Validates files in landing/ and routes them to bronze/ or quarantine/"

  runtime       = "python3.12"
  architectures = ["arm64"]
  handler       = "handler.lambda_handler"
  memory_size   = 256
  timeout       = 60

  filename         = data.archive_file.ingest_validate.output_path
  source_code_hash = data.archive_file.ingest_validate.output_base64sha256

  environment {
    variables = {
      ALLOWED_TOPICS = join(",", local.topics)
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.ingest_lambda_logs,
    aws_cloudwatch_log_group.ingest_lambda,
  ]
}

resource "aws_lambda_permission" "s3_invoke_ingest" {
  statement_id   = "AllowS3Invoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.ingest_validate.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.lake.arn
  source_account = data.aws_caller_identity.current.account_id
}

# One notification configuration per bucket. It fires only for .jsonl objects
# under landing/, so the Lambda's own writes to bronze/ and quarantine/ can
# never trigger it again.
resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest_validate.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/"
    filter_suffix       = ".jsonl"
  }

  depends_on = [aws_lambda_permission.s3_invoke_ingest]
}

# ------------------------------------------------------- Step Functions

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "close_batch" {
  name               = "${local.close_batch_name}-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "close_batch" {
  # ListObjectsV2 needs ListBucket on the bucket itself.
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/markers/bronze/*"]
  }

  statement {
    effect  = "Allow"
    actions = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/${aws_glue_crawler.bronze.name}",
    ]
  }
}

resource "aws_iam_role_policy" "close_batch" {
  name   = "${local.close_batch_name}-access"
  role   = aws_iam_role.close_batch.id
  policy = data.aws_iam_policy_document.close_batch.json
}

resource "aws_sfn_state_machine" "close_batch" {
  name     = local.close_batch_name
  role_arn = aws_iam_role.close_batch.arn
  type     = "STANDARD"

  definition = templatefile("${path.module}/ingest_state_machine.asl.json.tftpl", {
    bucket            = aws_s3_bucket.lake.bucket
    crawler_name      = aws_glue_crawler.bronze.name
    max_landing_polls = local.max_landing_polls
    max_crawler_polls = local.max_crawler_polls
  })
}

# ---------------------------------------------------------- schedule

data "aws_iam_policy_document" "events_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_start_close_batch" {
  name               = "${local.close_batch_name}-schedule"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events_start_close_batch" {
  name = "start-execution"
  role = aws_iam_role.events_start_close_batch.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.close_batch.arn
    }]
  })
}

resource "aws_cloudwatch_event_rule" "close_batch_hourly" {
  name                = "${local.close_batch_name}-hourly"
  description         = "Closes a bronze batch every hour"
  schedule_expression = "rate(1 hour)"
  state               = var.ingest_schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "close_batch_hourly" {
  rule     = aws_cloudwatch_event_rule.close_batch_hourly.name
  arn      = aws_sfn_state_machine.close_batch.arn
  role_arn = aws_iam_role.events_start_close_batch.arn
  input    = jsonencode({ max_quarantined = 0 })
}

# ----------------------------------------------------------- outputs

output "ingest_lambda" {
  value = aws_lambda_function.ingest_validate.function_name
}

output "close_batch_state_machine" {
  description = "Start with scripts/close_bronze_batch.sh"
  value       = aws_sfn_state_machine.close_batch.arn
}
