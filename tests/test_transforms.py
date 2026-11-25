from datetime import date, datetime

from claims_pipeline import transforms

HDR = "claim_id string, original_claim_id string, frequency_code string, received_ts timestamp, total_billed int"


def _status(df):
    return {r.claim_family_id: (r.claim_id, r.claim_status, r.version_count) for r in df.collect()}


def test_claim_versions_replacement_and_void(spark):
    rows = [
        ("A", None, "1", datetime(2026, 11, 1, 9), 100),
        ("A2", "A", "7", datetime(2026, 11, 2, 9), 120),  # replacement wins
        ("B", None, "1", datetime(2026, 11, 1, 9), 50),
        ("B8", "B", "8", datetime(2026, 11, 3, 9), 0),  # void wins
        ("C", None, "1", datetime(2026, 11, 1, 9), 70),  # untouched
        ("D", None, "1", datetime(2026, 11, 1, 9), 70),
        ("D7", "D", "7", datetime(2026, 11, 2, 9), 75),
        ("D8", "D", "8", datetime(2026, 11, 2, 9), 0),  # same timestamp: void beats replacement
    ]
    out = _status(transforms.resolve_claim_versions(spark.createDataFrame(rows, HDR)))
    assert out == {
        "A": ("A2", "ADJUSTED", 2),
        "B": ("B8", "VOIDED", 2),
        "C": ("C", "ORIGINAL", 1),
        "D": ("D8", "VOIDED", 3),
    }


def test_eligibility_range_join(spark):
    claims = spark.createDataFrame(
        [
            ("c1", "m1", date(2026, 10, 5)),
            ("c2", "m1", date(2026, 12, 1)),
            ("c3", "m2", date(2026, 10, 5)),
            ("c4", "m3", date(2026, 10, 5)),
        ],
        "claim_id string, member_token string, service_from_date date",
    )
    elig = spark.createDataFrame(
        [
            ("m1", date(2026, 1, 1), date(2026, 10, 31)),
            ("m2", date(2026, 1, 1), None),
            ("m2", date(2025, 1, 1), None),
        ],  # overlapping spans must not duplicate claims
        "member_token string, coverage_start date, coverage_end date",
    )
    out = transforms.flag_eligibility(claims, elig)
    assert out.count() == 4
    assert {r.claim_id: r.is_eligible for r in out.collect()} == {"c1": True, "c2": False, "c3": True, "c4": False}


def test_conform_claims_derives_validation_columns(spark):
    raw = spark.createDataFrame(
        [
            (
                " CLM1 ",
                None,
                "1",
                "M1",
                "1234567893",
                "PROFESSIONAL",
                "11",
                "2026-10-01",
                "2026-10-01",
                "2026-11-01 10:00:00",
                "250.50",
                [" e11.9 ", "ZZZ"],
            )
        ],
        transforms.RAW_SCHEMAS["claims"],
    )
    row = transforms.conform_claims(raw).collect()[0]
    assert row.claim_id == "CLM1"
    assert row.primary_dx == "E11.9"
    assert row.billing_npi_valid is True
    assert row.invalid_dx_count == 1
    assert row.service_from_date == date(2026, 10, 1)


def test_latest_per_key_keeps_null_keys(spark):
    df = spark.createDataFrame([("k", 1), ("k", 3), (None, 1), (None, 2)], "id string, v int")
    out = sorted((r.id or "", r.v) for r in transforms.latest_per_key(df, ["id"], "v").collect())
    assert out == [("", 1), ("", 2), ("k", 3)]
