from datetime import date, datetime, timedelta

from spain_electricity.features import add_flags, add_lag_24h


def test_flags_cover_legal_boundaries_and_blackout(spark):
    days = [date(2022, 6, 14), date(2022, 6, 15), date(2023, 12, 31), date(2024, 1, 1), date(2025, 4, 28)]
    rows = add_flags(spark.createDataFrame([(day,) for day in days], "local_date date")).collect()
    assert [row.iberian_exception for row in rows] == [False, True, True, False, False]
    assert [row.is_anomaly for row in rows] == [False, False, False, False, True]


def test_lag_uses_exact_elapsed_hours_and_preserves_gaps(spark):
    first = datetime(2025, 3, 29, 12)
    frame = spark.createDataFrame(
        [(first, 1.0), (first + timedelta(hours=24), 2.0), (first + timedelta(hours=25), 3.0)],
        "hour_utc timestamp, value double",
    )
    rows = add_lag_24h(frame, "value").orderBy("hour_utc").collect()
    assert [row.value_24h_ago for row in rows] == [None, 1.0, None]
