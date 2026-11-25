"""
Curated (analytics) layer: dimensional model plus cost and utilization marts.

    dim_member            SCD Type 2 built from every member version, de-identified
    dim_provider          provider master (NPI-validated)
    fact_claim_line       current version of each claim, one row per service line
    fact_pharmacy         pharmacy claims
    agg_member_month      medical + Rx paid per member per month (the input to PMPM)
    agg_plan_month        PMPM cost by plan
    high_cost_claimants   members above the paid threshold, ranked
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31 00:00:00"
TRACKED = ["plan_id", "state", "zip3", "gender", "age_band"]


def build_dim_member(member_versions: DataFrame) -> DataFrame:
    """SCD2 from the full change history.

    Versions are ordered by updated_at. Consecutive versions with identical
    tracked attributes are collapsed (a re-sent record is not a change).
    effective_to is the next version's effective_from. Rebuilding from the full
    history every run is idempotent and handles late-arriving changes.
    """
    w = Window.partitionBy("member_token").orderBy("updated_at")
    hashed = member_versions.withColumn(
        "attr_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in TRACKED]), 256)
    )
    changed = (
        hashed.withColumn("__prev", F.lag("attr_hash").over(w))
        .filter(F.col("__prev").isNull() | (F.col("__prev") != F.col("attr_hash")))
        .drop("__prev")
    )
    return (
        changed.withColumn("effective_from", F.col("updated_at"))
        .withColumn(
            "effective_to",
            F.coalesce(F.lead("updated_at").over(w), F.lit(HIGH_DATE).cast("timestamp")),
        )
        .withColumn("is_current", F.col("effective_to") == F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn("member_sk", F.xxhash64("member_token", "effective_from"))
        .select("member_sk", "member_token", *TRACKED, "birth_year", "effective_from", "effective_to", "is_current")
    )


def build_dim_provider(providers: DataFrame) -> DataFrame:
    w = Window.partitionBy("npi").orderBy(F.col("batch_date").desc())
    return (
        providers.withColumn("__rn", F.row_number().over(w))
        .filter("__rn = 1")
        .select("npi", "provider_name", "specialty", "taxonomy_code", "state")
    )


def build_fact_claim_line(current_claims: DataFrame, lines: DataFrame, dim_member: DataFrame) -> DataFrame:
    """Current, non-voided claims joined to their lines, to the member version
    valid on the date of service, and to the plan that version was on."""
    # Early services (before the first recorded member version) map to that first version.
    first_w = Window.partitionBy("member_token")
    dm = dim_member.select(
        F.col("member_token").alias("__tok"),
        "member_sk",
        F.col("plan_id").alias("plan_id_at_service"),
        F.when(F.col("effective_from") == F.min("effective_from").over(first_w), F.lit("1900-01-01").cast("timestamp"))
        .otherwise(F.col("effective_from"))
        .alias("__from"),
        F.col("effective_to").alias("__to"),
    )
    claims = current_claims.filter(F.col("claim_status") != "VOIDED")
    joined = claims.join(lines.drop("batch_date"), "claim_id")
    svc_ts = F.col("service_from_date").cast("timestamp")
    pit = joined.join(
        dm, (F.col("member_token") == F.col("__tok")) & (svc_ts >= F.col("__from")) & (svc_ts < F.col("__to")), "left"
    )
    return pit.select(
        "claim_id",
        "claim_family_id",
        "line_number",
        "claim_status",
        "member_token",
        "member_sk",
        "plan_id_at_service",
        "billing_npi",
        "claim_type",
        "place_of_service",
        "service_from_date",
        F.trunc("service_from_date", "month").alias("service_month"),
        "primary_dx",
        "procedure_code",
        "units",
        "billed_amount",
        "allowed_amount",
        "paid_amount",
        "is_eligible",
        "received_ts",
    )


def build_fact_pharmacy(rx: DataFrame) -> DataFrame:
    return rx.withColumn("service_month", F.trunc("fill_date", "month")).drop("batch_date")


def build_member_month(fact_lines: DataFrame, fact_rx: DataFrame) -> DataFrame:
    med = fact_lines.groupBy("member_token", "service_month").agg(
        F.sum("paid_amount").alias("medical_paid"),
        F.countDistinct("claim_id").alias("medical_claims"),
    )
    rx = fact_rx.groupBy("member_token", "service_month").agg(
        F.sum("paid_amount").alias("rx_paid"),
        F.count("*").alias("rx_fills"),
    )
    return (
        med.join(rx, ["member_token", "service_month"], "full_outer")
        .fillna({"medical_paid": 0, "rx_paid": 0, "medical_claims": 0, "rx_fills": 0})
        .withColumn("total_paid", F.col("medical_paid") + F.col("rx_paid"))
    )


def build_plan_month(member_month: DataFrame, dim_member: DataFrame) -> DataFrame:
    """PMPM = total paid / member months, using each member's current plan."""
    plan = dim_member.filter("is_current").select("member_token", "plan_id")
    enrolled = plan.groupBy("plan_id").agg(F.countDistinct("member_token").alias("members_enrolled"))
    paid = (
        member_month.join(plan, "member_token")
        .groupBy("plan_id", "service_month")
        .agg(
            F.sum("medical_paid").alias("medical_paid"),
            F.sum("rx_paid").alias("rx_paid"),
            F.sum("total_paid").alias("total_paid"),
        )
    )
    return paid.join(enrolled, "plan_id").withColumn(
        "pmpm", F.round(F.col("total_paid") / F.col("members_enrolled"), 2)
    )


def build_high_cost_claimants(member_month: DataFrame, threshold: float) -> DataFrame:
    totals = member_month.groupBy("member_token").agg(
        F.sum("total_paid").alias("total_paid"),
        F.sum("medical_paid").alias("medical_paid"),
        F.sum("rx_paid").alias("rx_paid"),
    )
    w = Window.orderBy(F.col("total_paid").desc())
    return (
        totals.withColumn("cost_rank", F.dense_rank().over(w))
        .withColumn("percentile", F.round(1 - F.percent_rank().over(w), 4))
        .filter(F.col("total_paid") >= threshold)
    )
