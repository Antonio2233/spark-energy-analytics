from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from spain_electricity.day_types import daily_profiles, fit_day_types


def local_day(spark, day, missing=None):
    zone = ZoneInfo("Europe/Madrid")
    start = datetime.combine(day, time(), zone).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time(), zone).astimezone(UTC)
    rows = []
    while start < end:
        local = start.astimezone(zone)
        if local.hour != missing:
            rows.append((start.replace(tzinfo=None), day, local.hour, 100.0 + local.hour))
        start += timedelta(hours=1)
    return spark.createDataFrame(
        rows, "hour_utc timestamp, local_date date, local_hour int, demand_mw double"
    )


@pytest.mark.parametrize("day", [date(2025, 1, 15), date(2025, 3, 30), date(2025, 10, 26)])
def test_profiles_normalize_complete_days_including_dst(spark, day):
    rows = daily_profiles(local_day(spark, day)).collect()
    assert len(rows) == 1
    values = rows[0].profile.toArray()
    assert len(values) == 24
    assert sum(values) / 24 == pytest.approx(1)
    assert values[2] == pytest.approx((values[1] + values[3]) / 2)


def test_missing_regular_hour_is_not_interpolated(spark):
    assert daily_profiles(local_day(spark, date(2025, 1, 15), missing=2)).count() == 0
    assert daily_profiles(local_day(spark, date(2025, 4, 28))).count() == 0


def test_kmeans_finds_distinct_shapes(spark):
    from pyspark.ml.linalg import Vectors

    rows = [
        (
            date(2024, 1, index + 1),
            Vectors.dense([1.0 + (0.5 if (hour < 12) == (index < 5) else 0) for hour in range(24)]),
        )
        for index in range(10)
    ]
    profiles = spark.createDataFrame(rows, ["local_date", "profile"])
    model, scores = fit_day_types(profiles, (2,))
    assert scores["2"] > 0.9
    assert sorted(
        row["count"] for row in model.transform(profiles).groupBy("day_type").count().collect()
    ) == [5, 5]
