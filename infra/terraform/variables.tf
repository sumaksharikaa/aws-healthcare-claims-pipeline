variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "glue_workers" {
  type    = number
  default = 2
}

variable "redshift_workgroup" {
  type        = string
  description = "Redshift Serverless workgroup name"
  default     = "claims"
}

variable "redshift_database" {
  type    = string
  default = "dev"
}

variable "redshift_s3_role_arn" {
  type        = string
  description = "IAM role attached to Redshift that can read the curated prefix and decrypt with the data KMS key"
  default     = ""
}

variable "alert_email" {
  type    = string
  default = ""
}
