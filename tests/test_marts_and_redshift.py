import re
from datetime import date, datetime
from decimal import Decimal

from claims_pipeline import marts, redshift

MEMBER_SCHEMA = (
    "member_token string, plan_id string, state string, zip3 string, gender string, age_band string, "
    "birth_year int, updated_at timestamp"
)


def _versions(spark):
    return spark.createDataFrame(
        [
            ("t1", "PPO-GOLD", "CT", "061", "F", "35-49", 1980, datetime(2026, 1, 1)),
            ("t1", "PPO-GOLD", "CT", "061", "F", "35-49", 1980, datetime(2026, 11, 2)),  # re-send, no change
            ("t1", "HMO-BASIC", "CT", "061", "F", "35-49", 1980, datetime(2026, 11, 3)),  # plan change
            ("t2", "PPO-SILVER", "TX", "752", "M", "50-64", 1970, datetime(2026, 1, 1)),
        ],
        MEMBER_SCHEMA,
    )


def test_dim_member_scd2_collapses_unchanged_versions(spark):
    dm = marts.build_dim_member(_versions(spark))
    t1 = sorted(dm.filter("member_token = 't1'").collect(), key=lambda r: r.effective_from)
    assert [r.plan_id for r in t1] == ["PPO-GOLD", "HMO-BASIC"]
    assert t1[0].effective_to == datetime(2026, 11, 3) and not t1[0].is_current
    assert t1[1].is_current and t1[1].effective_to == datetime(9999, 12, 31)
    assert dm.filter("is_current").count() == 2


def test_fact_point_in_time_plan_and_voids_excluded(spark):
    dm = marts.build_dim_member(_versions(spark))
    claims = spark.createDataFrame(
        [
            ("c1", "c1", "ORIGINAL", "t1", date(2026, 10, 15)),
            ("c2", "c2", "ADJUSTED", "t1", date(2026, 11, 20)),
            ("c3", "c3", "VOIDED", "t1", date(2026, 11, 20)),
            ("c4", "c4", "ORIGINAL", "t2", date(2025, 6, 1)),
        ],
        "claim_id string, claim_family_id string, claim_status string, member_token string, service_from_date date",
    ).selectExpr(
        "*",
        "'1234567893' as billing_npi",
        "'PROFESSIONAL' as claim_type",
        "'11' as place_of_service",
        "'I10' as primary_dx",
        "true as is_eligible",
        "current_timestamp() as received_ts",
    )
    lines = spark.createDataFrame(
        [(c, 1, "99213", 1, Decimal("100"), Decimal("80"), Decimal("60")) for c in ("c1", "c2", "c3", "c4")],
        "claim_id string, line_number int, procedure_code string, units int, billed_amount decimal(12,2), "
        "allowed_amount decimal(12,2), paid_amount decimal(12,2)",
    ).selectExpr("*", "'2026-11-01' as batch_date")
    fact = {r.claim_id: r for r in marts.build_fact_claim_line(claims, lines, dm).collect()}
    assert set(fact) == {"c1", "c2", "c4"}
    assert fact["c1"].plan_id_at_service == "PPO-GOLD"
    assert fact["c2"].plan_id_at_service == "HMO-BASIC"
    assert fact["c4"].member_sk is not None  # service before first version -> first version


def test_member_month_and_high_cost(spark):
    lines = spark.createDataFrame(
        [("t1", date(2026, 10, 1), "c1", Decimal("6000")), ("t2", date(2026, 10, 1), "c2", Decimal("100"))],
        "member_token string, service_month date, claim_id string, paid_amount decimal(12,2)",
    )
    rx = spark.createDataFrame(
        [("t2", date(2026, 10, 1), Decimal("50")), ("t3", date(2026, 11, 1), Decimal("20"))],
        "member_token string, service_month date, paid_amount decimal(12,2)",
    )
    mm = {(r.member_token, r.service_month): r for r in marts.build_member_month(lines, rx).collect()}
    assert mm[("t2", date(2026, 10, 1))].total_paid == Decimal("150")
    assert mm[("t3", date(2026, 11, 1))].medical_paid == 0  # Rx-only member kept (full outer join)
    hc = marts.build_high_cost_claimants(marts.build_member_month(lines, rx), 5000).collect()
    assert [(r.member_token, r.cost_rank) for r in hc] == [("t1", 1)]


def test_redshift_sql_is_transactional_full_refresh():
    t = next(x for x in redshift.TABLES if x.table == "fact_claim_line")
    sql = redshift.build_load_sql(t, "s3://bucket/curated", "arn:aws:iam::123:role/rs")
    assert sql[0] == "BEGIN;" and sql[-1] == "END;"
    assert "FORMAT AS PARQUET" in sql[2] and "s3://bucket/curated/fact_claim_line/" in sql[2]
    # Full refresh: lines of claims voided since the last load must disappear too.
    assert sql[3] == "DELETE FROM claims_mart.fact_claim_line;"
    assert sql[4].startswith("INSERT INTO claims_mart.fact_claim_line (claim_id, claim_family_id")


def test_redshift_key_based_upsert_sql():
    t = redshift.TableLoad("dim_provider", ("npi",), ("npi", "provider_name"))
    sql = redshift.build_load_sql(t, "s3://b/curated", "arn:r")
    assert sql[3] == (
        "DELETE FROM claims_mart.dim_provider USING stage_dim_provider "
        "WHERE claims_mart.dim_provider.npi = stage_dim_provider.npi;"
    )


def test_ddl_matches_load_column_order(repo_root):
    """COPY from Parquet maps by position, so the DDL must list columns in the same order."""
    ddl = (repo_root / "sql" / "redshift" / "ddl.sql").read_text()
    for t in redshift.TABLES:
        body = re.search(rf"claims_mart\.{t.table} \((.*?)\n\)", ddl, re.S).group(1)
        cols = [
            ln.split()[0] for ln in body.strip().splitlines() if ln.strip() and not ln.strip().startswith("PRIMARY")
        ]
        assert tuple(cols) == t.columns, t.table
