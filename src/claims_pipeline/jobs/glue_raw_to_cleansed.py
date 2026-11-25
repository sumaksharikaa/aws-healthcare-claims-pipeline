"""
AWS Glue job: raw -> cleansed for one entity and batch date.

Glue job arguments (set by Step Functions):
    --CLAIMS_ROOT   s3://<bucket>
    --CONFIG_PATH   path to pipeline.yaml (shipped with the job via --extra-files)
    --ENTITY        members | providers | eligibility | claims | claim_lines | pharmacy
    --BATCH_DATE    YYYY-MM-DD
"""

import sys

from awsglue.utils import getResolvedOptions  # type: ignore

from claims_pipeline.config import load_config, normalize_batch_date
from claims_pipeline.pipeline import run
from claims_pipeline.spark import get_spark

args = getResolvedOptions(sys.argv, ["CLAIMS_ROOT", "CONFIG_PATH", "ENTITY", "BATCH_DATE"])
cfg = load_config(args["CONFIG_PATH"], root_override=args["CLAIMS_ROOT"])
results = run(cfg, [normalize_batch_date(args["BATCH_DATE"])], ["raw_to_cleansed"], [args["ENTITY"]], spark=get_spark())
if any(r.status == "FAILED" for r in results):
    sys.exit(1)
