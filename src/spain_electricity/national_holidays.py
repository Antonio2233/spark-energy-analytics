"""Spain's national public holidays.

Only the holidays common to the whole country are included. Regional holidays and holidays
moved to Monday by a region when they fall on a Sunday are out of scope for a national
demand model.
"""

from collections.abc import Iterable
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, DateType, StructField, StructType

FIXED_HOLIDAYS = ((1, 1), (1, 6), (5, 1), (8, 15), (10, 12), (11, 1), (12, 6), (12, 8), (12, 25))


def easter_sunday(year: int) -> date:
    """Return Easter Sunday of a Gregorian year (anonymous Gregorian algorithm)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    weekday_offset = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_offset) // 451
    month = (h + weekday_offset - 7 * m + 114) // 31
    day = (h + weekday_offset - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def good_friday(year: int) -> date:
    """Return Good Friday, two days before Easter Sunday."""
    return easter_sunday(year) - timedelta(days=2)


def national_holidays(year: int) -> list[date]:
    """Return the national holidays of ``year`` in date order."""
    return sorted([date(year, month, day) for month, day in FIXED_HOLIDAYS] + [good_friday(year)])


def holidays_frame(spark: SparkSession, years: Iterable[int]) -> DataFrame:
    """Return one row per national holiday with ``local_date`` and ``is_holiday`` = True."""
    schema = StructType([StructField("local_date", DateType()), StructField("is_holiday", BooleanType())])
    rows = [(day, True) for year in years for day in national_holidays(year)]
    return spark.createDataFrame(rows, schema)
