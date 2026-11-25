"""
Raw -> cleansed transformations, as pure DataFrame functions.

Raw files arrive untyped (CSV) or loosely typed (JSON). Each conform_*
function casts to the target types with try_cast (bad values become nulls,
which the quality rules then catch), standardizes strings and adds the derived
columns the quality rules need.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from claims_pipeline.validation import invalid_icd10_count, npi_is_valid

# Raw schemas: every field arrives as a string so nothing is lost on read.
RAW_SCHEMAS: dict[str, StructType] = {
    "claims": StructType(
        [
            StructField(c, StringType())
            for c in [
                "claim_id",
                "original_claim_id",
                "frequency_code",
                "member_id",
                "billing_npi",
                "claim_type",
                "place_of_service",
                "service_from_date",
                "service_to_date",
                "received_ts",
                "total_billed",
            ]
        ]
        + [StructField("diagnosis_codes", ArrayType(StringType()))]
    ),
    "providers": StructType(
        [StructField(c, StringType()) for c in ["npi", "provider_name", "specialty", "taxonomy_code", "state"]]
    ),
}


def _try_cast(col: str, dtype: str):
    return F.expr(f"try_cast(`{col}` AS {dtype})").alias(col)


def _clean_str(col: str):
    t = F.trim(F.col(col).cast("string"))
    return F.when(t == "", None).otherwise(t).alias(col)


def latest_per_key(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the most recent row per key; rows with a null key are kept for quarantine."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    null_key = F.lit(False)
    for k in keys:
        null_key = null_key | F.col(k).isNull()
    return df.withColumn("__rn", F.row_number().over(w)).filter((F.col("__rn") == 1) | null_key).drop("__rn")


# ------------------------------------------------------------------- entities
def conform_members(raw: DataFrame) -> DataFrame:
    strings = ["member_id", "first_name", "last_name", "ssn", "address_line1", "city", "phone", "plan_id"]
    return raw.select(
        *[_clean_str(c) for c in strings],
        F.upper(F.trim("gender")).alias("gender"),
        F.upper(F.trim("state")).alias("state"),
        F.lpad(F.trim("zip_code"), 5, "0").alias("zip_code"),
        _try_cast("date_of_birth", "date"),
        _try_cast("updated_at", "timestamp"),
    )


def conform_providers(raw: DataFrame) -> DataFrame:
    out = raw.select(
        _clean_str("npi"),
        _clean_str("provider_name"),
        _clean_str("specialty"),
        _clean_str("taxonomy_code"),
        F.upper(F.trim("state")).alias("state"),
    )
    return out.withColumn("is_valid_npi", npi_is_valid("npi"))


def conform_eligibility(raw: DataFrame) -> DataFrame:
    return raw.select(
        _clean_str("member_id"),
        _clean_str("plan_id"),
        _try_cast("coverage_start", "date"),
        _try_cast("coverage_end", "date"),
    )


def conform_claims(raw: DataFrame) -> DataFrame:
    out = raw.select(
        *[
            _clean_str(c)
            for c in [
                "claim_id",
                "original_claim_id",
                "frequency_code",
                "member_id",
                "billing_npi",
                "claim_type",
                "place_of_service",
            ]
        ],
        _try_cast("service_from_date", "date"),
        _try_cast("service_to_date", "date"),
        _try_cast("received_ts", "timestamp"),
        _try_cast("total_billed", "decimal(12,2)"),
        F.transform(F.coalesce("diagnosis_codes", F.array()), lambda d: F.upper(F.trim(d))).alias("diagnosis_codes"),
    )
    return (
        out.withColumn("primary_dx", F.col("diagnosis_codes")[0])
        .withColumn("billing_npi_valid", npi_is_valid("billing_npi"))
        .withColumn("invalid_dx_count", invalid_icd10_count("diagnosis_codes"))
    )


def conform_claim_lines(raw: DataFrame) -> DataFrame:
    return raw.select(
        _clean_str("claim_id"),
        _try_cast("line_number", "int"),
        F.upper(F.trim("procedure_code")).alias("procedure_code"),
        _try_cast("units", "int"),
        *[_try_cast(c, "decimal(12,2)") for c in ["billed_amount", "allowed_amount", "paid_amount"]],
    )


def conform_pharmacy(raw: DataFrame) -> DataFrame:
    return raw.select(
        _clean_str("rx_claim_id"),
        _clean_str("member_id"),
        _clean_str("ndc"),
        _clean_str("prescriber_npi"),
        _try_cast("fill_date", "date"),
        _try_cast("days_supply", "int"),
        _try_cast("quantity", "decimal(10,3)"),
        *[_try_cast(c, "decimal(12,2)") for c in ["ingredient_cost", "dispensing_fee", "paid_amount"]],
    )


# --------------------------------------------------------------- claim logic
FREQ_LABEL = {"1": "ORIGINAL", "7": "ADJUSTED", "8": "VOIDED"}


def resolve_claim_versions(headers: DataFrame) -> DataFrame:
    """Collapse each claim family (original + replacements + voids) to its current version.

    The family id is the original claim id. The most recently received version
    wins; when two versions share a timestamp, a void beats a replacement, which
    beats an original. The result has one row per family with `claim_status`
    ORIGINAL / ADJUSTED / VOIDED and `version_count`.
    """
    fam = headers.withColumn("claim_family_id", F.coalesce("original_claim_id", "claim_id"))
    w = Window.partitionBy("claim_family_id").orderBy(
        F.col("received_ts").desc(), F.col("frequency_code").desc(), F.col("claim_id").desc()
    )
    counts = fam.groupBy("claim_family_id").agg(F.count("*").alias("version_count"))
    status_map = F.create_map(*[x for k, v in FREQ_LABEL.items() for x in (F.lit(k), F.lit(v))])
    return (
        fam.withColumn("__rn", F.row_number().over(w))
        .filter("__rn = 1")
        .drop("__rn")
        .join(counts, "claim_family_id")
        .withColumn("claim_status", status_map[F.col("frequency_code")])
    )


def flag_eligibility(claims: DataFrame, eligibility: DataFrame, member_col: str = "member_token") -> DataFrame:
    """Flag claims whose first service date falls inside a coverage span (range join).

    Ineligible claims are flagged, not dropped: a payer pends or denies them,
    and analytics still needs to see them.
    """
    spans = eligibility.select(
        F.col(member_col).alias("__m"),
        F.col("coverage_start").alias("__start"),
        F.coalesce("coverage_end", F.lit("9999-12-31").cast("date")).alias("__end"),
    )
    covered = (
        claims.select("claim_id", member_col, "service_from_date")
        .join(
            spans,
            (F.col(member_col) == F.col("__m")) & F.col("service_from_date").between(F.col("__start"), F.col("__end")),
        )
        .select("claim_id")
        .distinct()
        .withColumn("is_eligible", F.lit(True))
    )
    return claims.join(covered, "claim_id", "left").fillna({"is_eligible": False})
