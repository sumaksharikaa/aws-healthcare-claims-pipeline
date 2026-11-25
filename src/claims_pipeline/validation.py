"""
Healthcare code validation written as native Spark expressions, so it runs in
the JVM with no Python UDF serialization cost.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# ICD-10-CM structure: letter (not U), digit, alphanumeric, optional dot + 1-4 alphanumerics.
ICD10_PATTERN = r"^[A-TV-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$"


def npi_is_valid(col: str) -> Column:
    """NPI check-digit validation (Luhn over the '80840' health-industry prefix).

    The 15 digits are read right to left and every second digit is doubled
    (subtracting 9 when the result is above 9). The NPI is valid when the sum
    is divisible by 10. Built with higher-order functions (transform/aggregate).
    """
    c = F.col(col)
    digits = F.transform(F.split(F.reverse(F.concat(F.lit("80840"), c)), ""), lambda x: x.cast("int"))
    luhn_sum = F.aggregate(
        F.sequence(F.lit(0), F.lit(14)),
        F.lit(0),
        lambda acc, i: (
            acc
            + F.when(i % 2 == 1, F.when(digits[i] * 2 > 9, digits[i] * 2 - 9).otherwise(digits[i] * 2)).otherwise(
                digits[i]
            )
        ),
    )
    return F.coalesce(c.rlike(r"^[12][0-9]{9}$") & (luhn_sum % 10 == 0), F.lit(False))


def invalid_icd10_count(col: str) -> Column:
    return F.size(F.filter(F.col(col), lambda dx: ~dx.rlike(ICD10_PATTERN)))
