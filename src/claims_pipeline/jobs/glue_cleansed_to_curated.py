"""
AWS Glue job: rebuild the curated dimensional model and marts.

Glue job arguments:
    --CLAIMS_ROOT   s3://<bucket>
    --CONFIG_PATH   path to pipeline.yaml
    --BATCH_DATE    YYYY-MM-DD (recorded in the audit log)
"""

import sys

from awsglue.utils import getResolvedOptions  # type: ignore

from claims_pipeline.config import load_config, normalize_batch_date
from claims_pipeline.pipeline import run
from claims_pipeline.spark import get_spark

args = getResolvedOptions(sys.argv, ["CLAIMS_ROOT", "CONFIG_PATH", "BATCH_DATE"])
cfg = load_config(args["CONFIG_PATH"], root_override=args["CLAIMS_ROOT"])
run(cfg, [normalize_batch_date(args["BATCH_DATE"])], ["cleansed_to_curated"], spark=get_spark())
