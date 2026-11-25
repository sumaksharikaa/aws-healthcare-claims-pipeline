import pytest

from claims_pipeline import phi
from claims_pipeline.validation import invalid_icd10_count, npi_is_valid
from generate_healthcare_data import make_npi, npi_check_digit


def test_npi_check_digit_matches_cms_example():
    # CMS published example: 123456789 -> check digit 3
    assert npi_check_digit("123456789") == "3"


def test_npi_validation_expression(spark):
    import random

    good = [make_npi(random.Random(i)) for i in range(25)] + ["1234567893"]
    bad = ["1234567890", "123456789", "3234567893", "12345678AB", None]
    df = spark.createDataFrame([(n,) for n in good + bad], "npi string")
    out = {r.npi: r.ok for r in df.select("npi", npi_is_valid("npi").alias("ok")).collect()}
    assert all(out[n] for n in good)
    assert not any(out[n] for n in bad)


def test_icd10_format(spark):
    df = spark.createDataFrame(
        [(["E11.9", "I10", "S93.401A"],), (["ZZZ.999", "I10"],), (["U07.1"],), ([],)], "dx array<string>"
    )
    counts = [r[0] for r in df.select(invalid_icd10_count("dx")).collect()]
    assert counts == [0, 1, 1, 0]


def test_tokenize_is_deterministic_and_keyed(spark):
    df = spark.createDataFrame([("M1",), ("M1",), ("M2",), (None,)], "member_id string")
    a = [r[0] for r in df.select(phi.tokenize("member_id", "k1")).collect()]
    b = [r[0] for r in df.select(phi.tokenize("member_id", "k2")).collect()]
    assert a[0] == a[1] and a[0] != a[2] and a[3] is None
    assert a[0] != b[0], "a different key must give different tokens"
    assert len(a[0]) == 64


def test_split_members_separates_identifiers(spark, cfg):
    df = (
        spark.createDataFrame(
            [
                (
                    "M1",
                    "Ann",
                    "Lee",
                    "900-12-3456",
                    "1 Oak St",
                    "555-111-2222",
                    "1980-06-15",
                    "F",
                    "06103",
                    "CT",
                    "Hartford",
                    "PPO-GOLD",
                    "2026-11-01 00:00:00",
                )
            ],
            "member_id string, first_name string, last_name string, ssn string, address_line1 string, phone string, "
            "date_of_birth string, gender string, zip_code string, state string, city string, plan_id string, "
            "updated_at string",
        )
        .selectExpr("*", "cast(date_of_birth as date) as dob", "cast(updated_at as timestamp) as ts")
        .drop("date_of_birth", "updated_at")
        .withColumnRenamed("dob", "date_of_birth")
        .withColumnRenamed("ts", "updated_at")
    )

    deid, vault = phi.split_members(df, cfg, "test-key")
    row = deid.collect()[0]
    assert row.zip3 == "061" and row.birth_year == 1980 and row.age_band == "35-49"
    for col in ("member_id", "ssn", "first_name", "last_name", "phone", "address_line1", "date_of_birth", "zip_code"):
        assert col not in deid.columns
    assert {"member_id", "ssn", "member_token"} <= set(vault.columns)
    assert vault.collect()[0].member_token == row.member_token


def test_guardrail_blocks_identifiers(spark, cfg):
    df = spark.createDataFrame([("x", "900-00-0000")], "member_token string, ssn string")
    with pytest.raises(ValueError, match="ssn"):
        phi.assert_no_direct_identifiers(df, cfg)


def test_age_bands(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(5,), (18,), (49,), (64,), (65,), (101,)], "age int")
    out = [r[0] for r in df.select(phi.age_band(F.col("age"), [0, 18, 35, 50, 65, 200])).collect()]
    assert out == ["0-17", "18-34", "35-49", "50-64", "65+", "65+"]


def test_normalize_batch_date():
    from claims_pipeline.config import normalize_batch_date

    assert normalize_batch_date("2026-11-03") == "2026-11-03"
    assert normalize_batch_date("2026-11-04T06:30:00Z") == "2026-11-03"
