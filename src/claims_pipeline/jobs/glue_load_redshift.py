"""
AWS Glue Python-shell job: load curated Parquet into Redshift Serverless via the Data API.

Glue job arguments:
    --CURATED_PREFIX   s3://<bucket>/curated
    --IAM_ROLE_ARN     role Redshift assumes to read S3 (and decrypt with KMS)
    --WORKGROUP        Redshift Serverless workgroup
    --DATABASE         database name
"""

import logging
import sys

from awsglue.utils import getResolvedOptions  # type: ignore

from claims_pipeline.redshift import run_load

logging.basicConfig(level=logging.INFO)
args = getResolvedOptions(sys.argv, ["CURATED_PREFIX", "IAM_ROLE_ARN", "WORKGROUP", "DATABASE"])
run_load(args["CURATED_PREFIX"], args["IAM_ROLE_ARN"], args["WORKGROUP"], args["DATABASE"])
