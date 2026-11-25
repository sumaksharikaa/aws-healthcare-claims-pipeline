"""
Pipeline stages. Each stage maps to one AWS Glue job (see claims_pipeline/jobs/)
and to one state in the Step Functions workflow (infra/step_functions/).

    raw_to_cleansed     per entity and batch: type, validate, de-duplicate, tokenize PHI, quarantine
    cleansed_to_curated rebuild the dimensional model and marts from the cleansed zone

Local usage:
    python -m claims_pipeline.pipeline --start-date 2026-11-01 --end-date 2026-11-03
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from claims_pipeline import marts, phi, quality, redshift, storage, transforms
from claims_pipeline.config import ENTITIES, Config, load_config
from claims_pipeline.spark import get_spark

log = logging.getLogger("claims_pipeline")

RAW_FILES = {
    "members": ("csv", "members.csv"),
    "providers": ("json", "providers.json"),
    "eligibility": ("csv", "eligibility.csv"),
    "claims": ("json", "claim_headers.json"),
    "claim_lines": ("csv", "claim_lines.csv"),
    "pharmacy": ("parquet", "pharmacy.parquet"),
}
CONFORM = {
    "members": transforms.conform_members,
    "providers": transforms.conform_providers,
    "eligibility": transforms.conform_eligibility,
    "claims": transforms.conform_claims,
    "claim_lines": transforms.conform_claim_lines,
    "pharmacy": transforms.conform_pharmacy,
}
DEDUP_KEYS = {
    "members": (["member_id"], "updated_at"),
    "providers": (["npi"], "npi"),
    "claims": (["claim_id"], "received_ts"),
    "claim_lines": (["claim_id", "line_number"], "line_number"),
    "pharmacy": (["rx_claim_id"], "fill_date"),
}


@dataclass
class StepResult:
    run_id: str
    stage: str
    entity: str
    batch_date: str
    status: str = "RUNNING"
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    duplicates_removed: int = 0
    dq_failures: str = "{}"
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    duration_s: float = 0.0
    error: str = ""


# ------------------------------------------------------------- raw -> cleansed
def _member_ids(spark: SparkSession, cfg: Config) -> DataFrame:
    return storage.read_parquet(spark, cfg.path("phi_vault", "members")).select("member_id")


def raw_to_cleansed(spark: SparkSession, cfg: Config, entity: str, batch_date: str, res: StepResult) -> None:
    fmt, fname = RAW_FILES[entity]
    src = cfg.path("raw", entity, f"batch_date={batch_date}", fname)
    if not storage.path_exists(spark, src):
        res.status = "SKIPPED_NO_DATA"
        return

    raw = storage.read_raw(spark, fmt, src, transforms.RAW_SCHEMAS.get(entity))
    df = CONFORM[entity](raw)
    if entity in DEDUP_KEYS:
        keys, order = DEDUP_KEYS[entity]
        df = transforms.latest_per_key(df, keys, order)

    refs = {"members": _member_ids(spark, cfg)} if entity in ("claims", "pharmacy") else {}
    checked = quality.apply_rules(df, cfg.rules(entity), refs).cache()
    try:
        valid, invalid = quality.split_valid_invalid(checked)
        res.rows_in = raw.count()
        res.duplicates_removed = res.rows_in - checked.count()
        res.rows_quarantined = invalid.count()
        res.dq_failures = json.dumps(quality.failure_summary(invalid), sort_keys=True)

        key = phi.get_tokenization_key(cfg)
        # Quarantine holds tokens and failure reasons only. Investigators trace a
        # token back through the (restricted) raw zone, so no identifiers are copied here.
        if "member_id" in invalid.columns:
            invalid = invalid.withColumn("member_token", phi.tokenize("member_id", key))
        invalid = invalid.drop(*(phi.banned_columns(cfg) & set(invalid.columns)))
        phi.assert_no_direct_identifiers(invalid, cfg)
        storage.write_batch(invalid, cfg.path("quarantine", entity), batch_date)

        if entity == "members":
            deid, vault = phi.split_members(valid, cfg, key)
            storage.write_batch(vault, cfg.path("phi_vault", "members"), batch_date)
            out = deid
        elif "member_id" in valid.columns:
            out = valid.withColumn("member_token", phi.tokenize("member_id", key)).drop("member_id")
        else:
            out = valid

        phi.assert_no_direct_identifiers(out, cfg)
        storage.write_batch(out, cfg.path("cleansed", entity), batch_date)
        res.rows_out = res.rows_in - res.duplicates_removed - res.rows_quarantined
    finally:
        checked.unpersist()


# --------------------------------------------------------- cleansed -> curated
def _redshift_types(df: DataFrame) -> DataFrame:
    """Spark widens decimals on SUM (decimal(12,2) -> decimal(22,2)); pin money to
    decimal(18,2) so the Parquet types match the Redshift DDL exactly."""
    for f in df.schema.fields:
        if isinstance(f.dataType, DecimalType) and f.dataType.precision > 18:
            df = df.withColumn(f.name, F.col(f.name).cast("decimal(18,2)"))
    return df


def cleansed_to_curated(spark: SparkSession, cfg: Config, res: StepResult) -> None:
    def read(entity: str) -> DataFrame:
        return storage.read_parquet(spark, cfg.path("cleansed", entity))

    members, providers, elig = read("members"), read("providers"), read("eligibility")
    claims, lines, rx = read("claims"), read("claim_lines"), read("pharmacy")

    dim_member = marts.build_dim_member(members).cache()
    dim_provider = marts.build_dim_provider(providers)

    # Duplicate submissions across batches: keep the latest copy of each claim id.
    claims = transforms.latest_per_key(claims, ["claim_id"], "received_ts")
    lines = transforms.latest_per_key(lines, ["claim_id", "line_number"], "batch_date")
    current = transforms.flag_eligibility(transforms.resolve_claim_versions(claims), elig)

    fact_lines = marts.build_fact_claim_line(current, lines, dim_member).cache()
    fact_rx = marts.build_fact_pharmacy(transforms.latest_per_key(rx, ["rx_claim_id"], "batch_date"))
    member_month = marts.build_member_month(fact_lines, fact_rx).cache()

    outputs = {
        "dim_member": dim_member,
        "dim_provider": dim_provider,
        "fact_claim_line": fact_lines,
        "fact_pharmacy": fact_rx,
        "agg_member_month": member_month,
        "agg_plan_month": marts.build_plan_month(member_month, dim_member),
        "high_cost_claimants": marts.build_high_cost_claimants(member_month, float(cfg.marts["high_cost_threshold"])),
    }
    columns = {t.table: list(t.columns) for t in redshift.TABLES}
    for name, df in outputs.items():
        phi.assert_no_direct_identifiers(df, cfg)
        # Files hold exactly the Redshift column order, so COPY ... FORMAT AS PARQUET maps by position.
        df = _redshift_types(df.select(*columns[name]))
        if name in ("fact_claim_line", "fact_pharmacy"):
            # Partition on a copy: partition columns live in folder names, not in the files.
            df = df.withColumn("part_month", F.date_format("service_month", "yyyy-MM"))
            storage.write_table(df, cfg.path("curated", name), ["part_month"])
        else:
            storage.write_table(df, cfg.path("curated", name))

    res.rows_in = claims.count()
    res.rows_out = storage.read_parquet(spark, cfg.path("curated", "fact_claim_line")).count()
    for df in (dim_member, fact_lines, member_month):
        df.unpersist()


# ---------------------------------------------------------------- orchestration
def _run_step(fn, res: StepResult, results: list[StepResult]) -> None:
    t0 = time.perf_counter()
    try:
        fn(res)
        if res.status == "RUNNING":
            res.status = "SUCCEEDED"
    except Exception as exc:
        res.status, res.error = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        res.duration_s = round(time.perf_counter() - t0, 2)
        results.append(res)
        log.info(
            "%-19s %-11s %s %-15s in=%-5d dupes=%-3d quarantined=%-3d out=%-5d %s",
            res.stage,
            res.entity,
            res.batch_date,
            res.status,
            res.rows_in,
            res.duplicates_removed,
            res.rows_quarantined,
            res.rows_out,
            res.dq_failures,
        )


def run(
    cfg: Config,
    batch_dates: list[str],
    stages: list[str],
    entities: list[str] | None = None,
    spark: SparkSession | None = None,
) -> list[StepResult]:
    spark = spark or get_spark()
    run_id = str(uuid.uuid4())
    results: list[StepResult] = []
    try:
        if "raw_to_cleansed" in stages:
            for d in batch_dates:
                for ent in entities or ENTITIES:
                    _run_step(
                        lambda r, e=ent, d=d: raw_to_cleansed(spark, cfg, e, d, r),
                        StepResult(run_id, "raw_to_cleansed", ent, d),
                        results,
                    )
        if "cleansed_to_curated" in stages:
            _run_step(
                lambda r: cleansed_to_curated(spark, cfg, r),
                StepResult(run_id, "cleansed_to_curated", "all", batch_dates[-1]),
                results,
            )
    finally:
        if results:
            storage.append(spark.createDataFrame([asdict(r) for r in results]), cfg.path("audit"))
    return results


def main() -> None:
    p = argparse.ArgumentParser(description="Healthcare claims pipeline")
    p.add_argument("--config", default="conf/pipeline.yaml")
    p.add_argument("--root", help="Storage root: local folder or s3://bucket")
    p.add_argument("--batch-date")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--stages", default="raw_to_cleansed,cleansed_to_curated")
    p.add_argument("--entities", help="Comma-separated subset of " + ",".join(ENTITIES))
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.batch_date:
        dates = [a.batch_date]
    elif a.start_date and a.end_date:
        s, e = date.fromisoformat(a.start_date), date.fromisoformat(a.end_date)
        dates = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
    else:
        p.error("Provide --batch-date or --start-date/--end-date")
    cfg = load_config(a.config, root_override=a.root)
    ents = a.entities.split(",") if a.entities else None
    run(cfg, dates, a.stages.split(","), ents)


if __name__ == "__main__":
    main()
