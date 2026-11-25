# Infrastructure for the claims pipeline (reference deployment).
#
#   KMS key, S3 data lake bucket (raw/cleansed/curated/quarantine prefixes),
#   separate PHI vault bucket, Glue jobs, least-privilege IAM role, Step
#   Functions state machine, SNS alerts, and an EventBridge daily schedule.
#
# Redshift Serverless is referenced by name; create it separately or extend this module.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project     = "claims-pipeline"
      environment = var.environment
      data_class  = "phi"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name         = "claims-${var.environment}"
  lake_bucket  = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
  vault_bucket = "${local.name}-phi-vault-${data.aws_caller_identity.current.account_id}"
  code_prefix  = "s3://${aws_s3_bucket.lake.bucket}/code"
  entities     = ["providers", "eligibility", "claims", "claim_lines", "pharmacy"]
}

# --------------------------------------------------------------------- KMS
resource "aws_kms_key" "data" {
  description         = "Encrypts claims data at rest"
  enable_key_rotation = true
}

resource "aws_kms_key" "vault" {
  description         = "Encrypts the PHI vault; only the pipeline role and PHI stewards can use it"
  enable_key_rotation = true
}

resource "aws_secretsmanager_secret" "phi_token_key" {
  name       = "claims-pipeline/phi-tokenization-key"
  kms_key_id = aws_kms_key.vault.arn
}

# ---------------------------------------------------------------------- S3
resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
}

resource "aws_s3_bucket" "vault" {
  bucket = local.vault_bucket
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = { lake = aws_s3_bucket.lake.id, vault = aws_s3_bucket.vault.id }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "vault" {
  bucket = aws_s3_bucket.vault.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.vault.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "all" {
  for_each = { lake = aws_s3_bucket.lake.id, vault = aws_s3_bucket.vault.id }
  bucket   = each.value
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "raw-to-glacier"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# Deny any request that is not over TLS.
data "aws_iam_policy_document" "tls_only" {
  for_each = { lake = aws_s3_bucket.lake.arn, vault = aws_s3_bucket.vault.arn }
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [each.value, "${each.value}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = { lake = aws_s3_bucket.lake.id, vault = aws_s3_bucket.vault.id }
  bucket   = each.value
  policy   = data.aws_iam_policy_document.tls_only[each.key].json
}

# --------------------------------------------------------------------- IAM
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "LakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "VaultReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.vault.arn, "${aws_s3_bucket.vault.arn}/*"]
  }
  statement {
    sid       = "UseKeys"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn, aws_kms_key.vault.arn]
  }
  statement {
    sid       = "TokenKey"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.phi_token_key.arn]
  }
  statement {
    sid       = "RedshiftDataApi"
    actions   = ["redshift-data:BatchExecuteStatement", "redshift-data:DescribeStatement", "redshift-serverless:GetCredentials"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "data-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

# -------------------------------------------------------------------- Glue
locals {
  glue_common_args = {
    "--CLAIMS_ROOT"                      = "s3://${aws_s3_bucket.lake.bucket}"
    "--CONFIG_PATH"                      = "pipeline.yaml"
    "--extra-files"                      = "${local.code_prefix}/pipeline.yaml"
    "--additional-python-modules"        = "${local.code_prefix}/aws_healthcare_claims_pipeline-1.0.0-py3-none-any.whl"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }
}

resource "aws_glue_job" "raw_to_cleansed" {
  name              = "${local.name}-raw-to-cleansed"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  max_retries       = 0 # retries are handled by Step Functions
  timeout           = 60
  command {
    script_location = "${local.code_prefix}/jobs/glue_raw_to_cleansed.py"
    python_version  = "3"
  }
  default_arguments = local.glue_common_args
  execution_property {
    max_concurrent_runs = 6
  }
}

resource "aws_glue_job" "cleansed_to_curated" {
  name              = "${local.name}-cleansed-to-curated"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers * 2
  max_retries       = 0
  timeout           = 120
  command {
    script_location = "${local.code_prefix}/jobs/glue_cleansed_to_curated.py"
    python_version  = "3"
  }
  default_arguments = local.glue_common_args
}

resource "aws_glue_job" "load_redshift" {
  name         = "${local.name}-load-redshift"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "3.0"
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 60
  command {
    name            = "pythonshell"
    script_location = "${local.code_prefix}/jobs/glue_load_redshift.py"
    python_version  = "3.9"
  }
  default_arguments = {
    "--CURATED_PREFIX"            = "s3://${aws_s3_bucket.lake.bucket}/curated"
    "--IAM_ROLE_ARN"              = var.redshift_s3_role_arn
    "--WORKGROUP"                 = var.redshift_workgroup
    "--DATABASE"                  = var.redshift_database
    "--additional-python-modules" = "${local.code_prefix}/aws_healthcare_claims_pipeline-1.0.0-py3-none-any.whl"
  }
}

# -------------------------------------------------------- Step Functions
resource "aws_sns_topic" "alerts" {
  name              = "${local.name}-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [
      aws_glue_job.raw_to_cleansed.arn,
      aws_glue_job.cleansed_to_curated.arn,
      aws_glue_job.load_redshift.arn,
    ]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "orchestration"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../step_functions/claims_pipeline.asl.json", {
    raw_to_cleansed_job     = aws_glue_job.raw_to_cleansed.name
    cleansed_to_curated_job = aws_glue_job.cleansed_to_curated.name
    load_redshift_job       = aws_glue_job.load_redshift.name
    alerts_topic_arn        = aws_sns_topic.alerts.arn
  })
}

# ------------------------------------------------------------ Schedule
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-pipeline"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.pipeline.arn }]
  })
}

resource "aws_scheduler_schedule" "daily" {
  name                = "${local.name}-daily"
  schedule_expression = "cron(30 6 * * ? *)"
  flexible_time_window {
    mode = "OFF"
  }
  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    # <aws.scheduler.scheduled-time> is resolved by EventBridge Scheduler at run time.
    input = jsonencode({
      batch_date = "<aws.scheduler.scheduled-time>"
      entities   = local.entities
    })
  }
}
