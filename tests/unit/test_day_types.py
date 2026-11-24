from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from pyspark.ml import PipelineModel

from spain_electricity.day_types import daily_profiles, fit_day_types, name_day_types


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
    # Like every other model, day types are saved as a Spark ML Pipeline.
    assert isinstance(model, PipelineModel)
    assert scores["2"] > 0.9
    assert sorted(
        row["count"] for row in model.transform(profiles).groupBy("day_type").count().collect()
    ) == [5, 5]


def summary_row(day_type, days_off, temperature):
    return {"day_type": day_type, "weekend_or_holiday_share": days_off, "mean_temperature_c": temperature}


def test_day_types_are_named_from_days_off_and_temperature():
    summary = [summary_row(0, 0.13, 23.6), summary_row(1, 0.0, 13.2), summary_row(2, 0.97, 16.8)]
    assert name_day_types(summary) == {0: "Warm working day", 1: "Cool working day", 2: "Weekend or holiday"}


def test_day_type_names_stay_unique_with_many_groups():
    summary = [summary_row(index, 0.0, temperature) for index, temperature in enumerate((10, 25, 15, 20))]
    names = name_day_types(summary)
    assert names[1] == "Warm working day" and names[0] == "Cool working day"
    assert len(set(names.values())) == 4
    assert all("working day" in name for name in names.values())
