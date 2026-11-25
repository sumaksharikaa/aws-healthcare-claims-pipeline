"""End-to-end: generated raw files -> cleansed -> curated, on local storage."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from claims_pipeline import redshift
from claims_pipeline.config import load_config
from claims_pipeline.pipeline import run
from generate_healthcare_data import generate

DATES = ["2026-11-01", "2026-11-02", "2026-11-03"]
STAGES = ["raw_to_cleansed", "cleansed_to_curated"]


@pytest.fixture(scope="module")
def lake(spark, repo_root, tmp_path_factory):
    root = tmp_path_factory.mktemp("claims")
    generate(root / "raw", date(2026, 11, 1), days=3, seed=5, members=300, claims=500)
    cfg = load_config(repo_root / "conf" / "pipeline.yaml", root_override=str(root))
    results = run(cfg, DATES, STAGES, spark=spark)
    return cfg, results


def _read(spark, cfg, zone, name):
    return spark.read.parquet(cfg.path(zone, name))


def test_all_steps_succeed_and_defects_are_quarantined(lake):
    _, results = lake
    assert {r.status for r in results} <= {"SUCCEEDED", "SKIPPED_NO_DATA"}
    failures = {}
    for r in results:
        for rule in __import__("json").loads(r.dq_failures):
            failures.setdefault(r.entity, set()).add(rule)
    assert {"dx_icd10_format", "member_known", "npi_check_digit", "service_not_in_future"} <= failures["claims"]
    assert "paid_not_negative" in failures["claim_lines"]
    assert "ndc_format" in failures["pharmacy"]
    assert sum(r.duplicates_removed for r in results if r.entity == "claims") > 0


def test_no_direct_identifiers_outside_vault(spark, lake):
    cfg, _ = lake
    banned = {"member_id", "ssn", "first_name", "last_name", "address_line1", "phone", "date_of_birth", "zip_code"}
    for zone, names in {
        "cleansed": ["members", "eligibility", "claims", "pharmacy"],
        "curated": [t.table for t in redshift.TABLES],
        "quarantine": ["members", "claims"],
    }.items():
        for n in names:
            leaked = banned & set(_read(spark, cfg, zone, n).columns)
            assert not leaked, f"{zone}/{n} leaks {leaked}"
    assert "ssn" in _read(spark, cfg, "phi_vault", "members").columns


def test_voids_excluded_and_one_version_per_claim(spark, lake):
    cfg, _ = lake
    fact = _read(spark, cfg, "curated", "fact_claim_line")
    assert fact.filter("claim_status = 'VOIDED'").count() == 0
    assert fact.groupBy("claim_family_id").agg(F.countDistinct("claim_id").alias("n")).filter("n > 1").count() == 0
    assert fact.groupBy("claim_id", "line_number").count().filter("count > 1").count() == 0
    assert fact.filter("member_sk IS NULL").count() == 0


def test_member_month_reconciles_to_facts(spark, lake):
    cfg, _ = lake
    fact_paid = _read(spark, cfg, "curated", "fact_claim_line").agg(F.sum("paid_amount")).first()[0]
    rx_paid = _read(spark, cfg, "curated", "fact_pharmacy").agg(F.sum("paid_amount")).first()[0]
    mm = _read(spark, cfg, "curated", "agg_member_month").agg(F.sum("medical_paid"), F.sum("rx_paid")).first()
    assert mm[0] == fact_paid and mm[1] == rx_paid


def test_curated_columns_match_redshift(spark, lake):
    cfg, _ = lake
    for t in redshift.TABLES:
        cols = [c for c in _read(spark, cfg, "curated", t.table).columns if c != "part_month"]
        assert tuple(cols) == t.columns, t.table


def test_rerun_is_idempotent(spark, lake):
    cfg, _ = lake
    names = ["claims", "claim_lines", "pharmacy", "members"]
    before = {n: _read(spark, cfg, "cleansed", n).count() for n in names}
    fact_before = _read(spark, cfg, "curated", "fact_claim_line").count()
    run(cfg, ["2026-11-02"], STAGES, spark=spark)
    after = {n: _read(spark, cfg, "cleansed", n).count() for n in names}
    assert before == after
    assert _read(spark, cfg, "curated", "fact_claim_line").count() == fact_before
