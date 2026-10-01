# One role shared by the Glue crawler and (later) the Glue jobs. Splitting
# them would be better practice in a multi-team account; here it would add
# indirection without changing what anything can reach.

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# Grants the Glue catalog and CloudWatch Logs access every crawler and job
# needs. S3 access is deliberately NOT included — this managed policy only
# covers buckets named aws-glue-*, so the lake bucket is granted separately.
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject", # Iceberg rewrites and expires data files
    ]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${local.name_prefix}-glue-s3"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}
