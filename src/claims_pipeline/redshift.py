"""
Load curated Parquet from S3 into Amazon Redshift.

Each table loads through a staging table inside one transaction:

    BEGIN;
      CREATE TEMP TABLE stage (LIKE target);
      COPY stage FROM 's3://.../curated/<table>/' IAM_ROLE '...' FORMAT AS PARQUET;
      DELETE FROM target [USING stage WHERE <keys match>];
      INSERT INTO target SELECT ... FROM stage;
    END;

The curated zone is rebuilt in full every run, so every table is a full
refresh: rows that disappeared upstream (for example lines of a claim that was
voided later) must disappear from Redshift too, which a key-based upsert would
miss. The key-based path is kept for tables that move to incremental loads.
Re-running a load never duplicates rows, and readers never see a half-loaded
table (DELETE, unlike TRUNCATE, stays inside the transaction). Statements run through the Redshift Data API (no JDBC
driver or open network path needed from Glue or Lambda).
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableLoad:
    table: str
    merge_keys: tuple[str, ...]  # empty -> transactional full refresh
    columns: tuple[str, ...]


SCHEMA = "claims_mart"

TABLES: list[TableLoad] = [
    TableLoad(
        "dim_member",
        (),
        (
            "member_sk",
            "member_token",
            "plan_id",
            "state",
            "zip3",
            "gender",
            "age_band",
            "birth_year",
            "effective_from",
            "effective_to",
            "is_current",
        ),
    ),
    TableLoad("dim_provider", (), ("npi", "provider_name", "specialty", "taxonomy_code", "state")),
    TableLoad(
        "fact_claim_line",
        (),
        (
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
            "primary_dx",
            "procedure_code",
            "units",
            "billed_amount",
            "allowed_amount",
            "paid_amount",
            "is_eligible",
            "received_ts",
            "service_month",
        ),
    ),
    TableLoad(
        "fact_pharmacy",
        (),
        (
            "rx_claim_id",
            "ndc",
            "prescriber_npi",
            "fill_date",
            "days_supply",
            "quantity",
            "ingredient_cost",
            "dispensing_fee",
            "paid_amount",
            "member_token",
            "service_month",
        ),
    ),
    TableLoad(
        "agg_member_month",
        (),
        ("member_token", "service_month", "medical_paid", "medical_claims", "rx_paid", "rx_fills", "total_paid"),
    ),
    TableLoad(
        "agg_plan_month",
        (),
        ("plan_id", "service_month", "medical_paid", "rx_paid", "total_paid", "members_enrolled", "pmpm"),
    ),
    TableLoad(
        "high_cost_claimants", (), ("member_token", "total_paid", "medical_paid", "rx_paid", "cost_rank", "percentile")
    ),
]


def build_load_sql(t: TableLoad, curated_prefix: str, iam_role_arn: str) -> list[str]:
    target = f"{SCHEMA}.{t.table}"
    stage = f"stage_{t.table}"
    cols = ", ".join(t.columns)
    s3 = f"{curated_prefix.rstrip('/')}/{t.table}/"
    stmts = [
        "BEGIN;",
        f"CREATE TEMP TABLE {stage} (LIKE {target});",
        # Parquet COPY maps columns by position; the pipeline writes files in exactly this order.
        f"COPY {stage} FROM '{s3}' IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET;",
    ]
    if t.merge_keys:
        on = " AND ".join(f"{target}.{k} = {stage}.{k}" for k in t.merge_keys)
        stmts.append(f"DELETE FROM {target} USING {stage} WHERE {on};")
    else:
        stmts.append(f"DELETE FROM {target};")  # full refresh, still transactional (TRUNCATE would commit)
    stmts += [f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage};", "END;"]
    return stmts


def run_load(
    curated_prefix: str,
    iam_role_arn: str,
    workgroup: str,
    database: str,
    tables: list[str] | None = None,
    poll_seconds: float = 2.0,
) -> None:
    import boto3

    client = boto3.client("redshift-data")
    for t in TABLES:
        if tables and t.table not in tables:
            continue
        sqls = build_load_sql(t, curated_prefix, iam_role_arn)
        # batch_execute_statement runs the statements as one transaction.
        resp = client.batch_execute_statement(WorkgroupName=workgroup, Database=database, Sqls=sqls[1:-1])
        while True:
            desc = client.describe_statement(Id=resp["Id"])
            if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(poll_seconds)
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Load of {t.table} {desc['Status']}: {desc.get('Error')}")
        log.info("Loaded %s", t.table)


def main() -> None:
    p = argparse.ArgumentParser(description="Load curated tables into Redshift Serverless")
    p.add_argument("--curated-prefix", required=True, help="s3://bucket/curated")
    p.add_argument("--iam-role-arn", required=True)
    p.add_argument("--workgroup", required=True)
    p.add_argument("--database", default="dev")
    p.add_argument("--tables")
    p.add_argument("--dry-run", action="store_true", help="Print the SQL instead of running it")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.dry_run:
        for t in TABLES:
            print("\n".join(build_load_sql(t, a.curated_prefix, a.iam_role_arn)), "\n")
        return
    run_load(a.curated_prefix, a.iam_role_arn, a.workgroup, a.database, a.tables.split(",") if a.tables else None)


if __name__ == "__main__":
    main()
