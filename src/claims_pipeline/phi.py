"""
PHI protection.

* Direct identifiers (name, SSN, street address, phone) are split off into a
  restricted PHI vault. In AWS that is a separate bucket with its own KMS key
  and an IAM policy that analysts do not get.
* Everything downstream uses `member_token`: a keyed SHA-256 of the member id.
  It is deterministic, so joins across claims, pharmacy and eligibility still
  work, but it cannot be reversed without the key. The key comes from AWS
  Secrets Manager and never appears in code or config.
* Quasi-identifiers are generalized (Safe Harbor style): date of birth becomes
  birth year plus an age band, and ZIP is cut to 3 digits.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from claims_pipeline.config import Config

log = logging.getLogger(__name__)
_DEV_KEY = "local-development-key-not-for-production"


def get_tokenization_key(cfg: Config) -> str:
    """Secrets Manager in AWS; environment variable locally; dev key as last resort."""
    if cfg.root.startswith("s3://"):
        import boto3  # only needed in AWS

        secret = boto3.client("secretsmanager").get_secret_value(SecretId=cfg.phi["secret_id"])
        return secret["SecretString"]
    key = os.environ.get(cfg.phi["key_env_var"])
    if key:
        return key
    log.warning("PHI token key not set; using the local development key")
    return _DEV_KEY


def tokenize(col: Column | str, key: str) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.when(c.isNull(), None).otherwise(F.sha2(F.concat(F.lit(key), F.lit("|"), c), 256))


def age_band(age: Column, bands: list[int]) -> Column:
    expr = None
    for lo, hi in zip(bands, bands[1:], strict=False):
        label = f"{lo}+" if hi >= 120 else f"{lo}-{hi - 1}"
        cond = (age >= lo) & (age < hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise("UNKNOWN")


def split_members(members: DataFrame, cfg: Config, key: str) -> tuple[DataFrame, DataFrame]:
    """Return (de-identified member rows, PHI vault rows)."""
    phi = cfg.phi
    tokenized = members.withColumn("member_token", tokenize("member_id", key))

    vault = tokenized.select(
        "member_token", "member_id", *phi["drop_columns"], "date_of_birth", "zip_code", "updated_at"
    )

    age = F.floor(F.months_between(F.col("updated_at"), F.col("date_of_birth")) / 12).cast("int")
    deid = (
        tokenized.withColumn("birth_year", F.year("date_of_birth"))
        .withColumn("age_band", age_band(age, phi["age_bands"]))
        .withColumn("zip3", F.substring("zip_code", 1, phi["zip_digits_kept"]))
        .drop("member_id", "date_of_birth", "zip_code", *phi["drop_columns"])
    )
    return deid, vault


def banned_columns(cfg: Config) -> set[str]:
    """Columns that may only exist in the raw zone and the PHI vault."""
    return set(cfg.phi["drop_columns"]) | {"member_id", "date_of_birth", "zip_code"}


def assert_no_direct_identifiers(df: DataFrame, cfg: Config) -> None:
    """Guardrail run before every write outside the raw zone and the PHI vault."""
    leaked = banned_columns(cfg) & set(df.columns)
    if leaked:
        raise ValueError(f"Direct identifiers present in analytics dataset: {sorted(leaked)}")
