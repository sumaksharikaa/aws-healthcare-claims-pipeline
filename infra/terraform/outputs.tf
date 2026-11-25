output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "phi_vault_bucket" {
  value = aws_s3_bucket.vault.bucket
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
