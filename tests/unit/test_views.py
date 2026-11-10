from datetime import UTC, date, datetime

import pandas as pd
import pytest

from spain_electricity import views
from spain_electricity.features import GENERATION_COLUMNS


def ts(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M")


def write_hourly(spark, paths, rows, holidays=()):
    columns = ["hour_utc", "local_date", "is_weekend", "demand_mw", "temperature_c", "renewable_share"]
    frame = spark.createDataFrame(rows, columns)
    frame = frame.withColumn("is_holiday", frame.local_date.isin(list(holidays)))
    for column in GENERATION_COLUMNS:
        if column not in frame.columns:
            frame = frame.withColumn(column, frame.demand_mw / 10)
    for column in ("actual_demand_mw", "forecast_demand_mw", "spot_price_eur_mwh", "pvpc_eur_mwh"):
        frame = frame.withColumn(column, frame.demand_mw)
    frame.write.parquet(str(paths.processed / "hourly_features"))


HOURLY_ROWS = [
    (ts("2025-01-15 10:00"), date(2025, 1, 15), False, 30000.0, 5.0, 0.4),
    (ts("2025-01-15 11:00"), date(2025, 1, 15), False, 32000.0, 7.0, 0.5),
    (ts("2025-01-18 11:00"), date(2025, 1, 18), True, 25000.0, 12.0, 0.6),
]


def test_status_on_empty_dir(spark, paths):
    status = views.data_status(spark, paths)
    assert list(status.dataset) == list(views.DATASETS)
    assert (status.rows == 0).all()


def test_status_counts_existing_dataset(spark, paths):
    write_hourly(spark, paths, HOURLY_ROWS)
    status = views.data_status(spark, paths).set_index("dataset")
    assert status.loc["hourly_features", "rows"] == 3
    assert str(status.loc["hourly_features", "first"]) == "2025-01-15 10:00:00"


def test_status_time_columns_are_datetimes(spark, paths):
    write_hourly(spark, paths, HOURLY_ROWS)
    spark.createDataFrame(
        [(date(2025, 1, 2), 52.6)], "delivery_date date, gas_price_eur_mwh double"
    ).write.parquet(str(paths.processed / "gas_price"))
    status = views.data_status(spark, paths)
    assert pd.api.types.is_datetime64_any_dtype(status["first"])
    assert pd.api.types.is_datetime64_any_dtype(status["last"])
    assert status.set_index("dataset").loc["gas_price", "first"] == pd.Timestamp("2025-01-02")


def test_hourly_history_filters_dates(spark, paths):
    assert list(views.hourly_history(spark, paths, date(2025, 1, 1), date(2025, 1, 31)).columns) == list(
        views.HISTORY_COLUMNS
    )
    write_hourly(spark, paths, HOURLY_ROWS)
    history = views.hourly_history(spark, paths, date(2025, 1, 15), date(2025, 1, 15))
    assert len(history) == 2
    assert "actual_demand_mw" in history.columns


def test_mix_history_is_long(spark, paths):
    write_hourly(spark, paths, HOURLY_ROWS)
    mix = views.mix_history(spark, paths, date(2025, 1, 15), date(2025, 1, 15))
    assert set(mix.columns) == {"hour_utc", "technology", "mw"}
    assert set(mix.technology) == set(views.MIX_TECHNOLOGIES)
    assert len(mix) == 2 * len(views.MIX_TECHNOLOGIES)


def test_daily_demand_vs_temperature(spark, paths):
    write_hourly(spark, paths, HOURLY_ROWS)
    daily = views.daily_demand_vs_temperature(spark, paths).set_index("local_date")
    assert daily.loc[date(2025, 1, 15), "demand_mw"] == 31000.0
    assert daily.loc[date(2025, 1, 15), "temperature_c"] == 6.0
    assert bool(daily.loc[date(2025, 1, 18), "is_weekend"]) is True


def test_daily_demand_vs_temperature_reports_holidays(spark, paths):
    write_hourly(spark, paths, HOURLY_ROWS, holidays=[date(2025, 1, 15)])
    daily = views.daily_demand_vs_temperature(spark, paths).set_index("local_date")
    assert bool(daily.loc[date(2025, 1, 15), "is_holiday"]) is True
    assert bool(daily.loc[date(2025, 1, 18), "is_holiday"]) is False


def test_correlation_diagonal(spark, paths):
    assert views.correlation_matrix(spark, paths, ["demand_mw", "temperature_c"]).empty
    write_hourly(spark, paths, HOURLY_ROWS)
    matrix = views.correlation_matrix(spark, paths, ["demand_mw", "temperature_c"])
    assert list(matrix.columns) == ["demand_mw", "temperature_c"]
    assert matrix.loc["demand_mw", "demand_mw"] == pytest.approx(1.0)
    assert matrix.loc["demand_mw", "temperature_c"] < 0


def test_live_windows_keeps_latest_batch(spark, paths):
    target = paths.stream_output / "windows"
    schema = "window_start timestamp, window_end timestamp, avg_demand_mw double, max_demand_mw double, "
    schema += "avg_renewable_share double, n_readings long"
    for batch_id, average in ((0, 10.0), (1, 12.0)):
        row = [(ts("2025-01-15 10:00"), ts("2025-01-15 10:15"), average, average, 0.5, 3)]
        spark.createDataFrame(row, schema).write.parquet(str(target / f"batch_id={batch_id}"))
    windows = views.live_windows(spark, paths)
    assert len(windows) == 1
    assert windows.avg_demand_mw.iloc[0] == 12.0


def test_live_readings_last_hours(spark, paths):
    assert views.live_readings(spark, paths).empty
    rows = [
        (ts("2025-01-15 00:00"), 1.0, 0.1),
        (ts("2025-01-15 09:00"), 2.0, 0.2),
        (ts("2025-01-15 10:00"), 3.0, 0.3),
    ]
    spark.createDataFrame(rows, ["ts_utc", "demand_mw", "renewable_share"]).write.parquet(
        str(paths.stream_output / "readings")
    )
    readings = views.live_readings(spark, paths, last_hours=6)
    assert list(readings.demand_mw) == [2.0, 3.0]


def test_minutes_since():
    now = datetime(2026, 11, 10, 18, 0, tzinfo=UTC)
    assert views.minutes_since(pd.Timestamp("2026-11-10 17:50", tz="UTC"), now) == 10
    assert views.minutes_since(pd.Timestamp("2026-11-10 17:50"), now) == 10
    assert views.minutes_since(None, now) is None
    assert views.minutes_since(pd.NaT, now) is None
