from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from spain_electricity import features
from spain_electricity.national_holidays import holidays_frame


def ts(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M")


def test_hourly_mean_is_mean_not_sum(spark):
    rows = [
        (ts(f"2025-01-15 10:{minute:02d}"), float(index + 1)) for index, minute in enumerate(range(0, 60, 5))
    ]
    df = spark.createDataFrame(rows, ["ts_utc", "demand_mw"])
    result = features.hourly_mean(df, "ts_utc", ["demand_mw"]).collect()
    assert len(result) == 1
    assert result[0].demand_mw == 6.5
    assert result[0].hour_utc == ts("2025-01-15 10:00")


SHARE_COLUMNS = ["wind_mw", "solar_mw", "solar_pv_mw", "solar_thermal_mw", "hydro_mw", "demand_mw"]


def test_renewable_share(spark):
    df = spark.createDataFrame([(100.0, 50.0, 40.0, 10.0, 50.0, 400.0)], SHARE_COLUMNS)
    assert features.add_renewable_share(df).first().renewable_share == 0.5


def test_renewable_share_counts_solar_as_pv_plus_thermal(spark):
    # From December 2025 REE's total solar field (sol) no longer equals PV + thermal (n = 616,032
    # readings, 2021-2026): the share must keep one definition across the whole history.
    df = spark.createDataFrame([(100.0, 0.0, 40.0, 10.0, 50.0, 400.0)], SHARE_COLUMNS)
    assert features.add_renewable_share(df).first().renewable_share == 0.5


def test_renewable_share_with_zero_demand_is_null(spark):
    df = spark.createDataFrame([(100.0, 50.0, 40.0, 10.0, 50.0, 0.0)], SHARE_COLUMNS)
    assert features.add_renewable_share(df).first().renewable_share is None


def test_national_weather_weighted(spark):
    rows = [
        ("madrid", ts("2025-01-15 10:00"), 10.0, 0.0, 5.0),
        ("barcelona", ts("2025-01-15 10:00"), 20.0, 0.0, 5.0),
    ]
    weather = spark.createDataFrame(
        rows, ["city", "ts_utc", "temperature_c", "shortwave_radiation_wm2", "wind_speed_100m_kmh"]
    )
    result = features.national_weather(weather).first()
    assert result.temperature_c == pytest.approx(13.333, abs=0.01)
    assert result.wind_speed_100m_kmh == pytest.approx(5.0)


def test_national_weather_ignores_missing_values(spark):
    rows = [
        ("madrid", ts("2025-01-15 10:00"), 10.0, 0.0, 5.0),
        ("barcelona", ts("2025-01-15 10:00"), None, 0.0, 5.0),
    ]
    weather = spark.createDataFrame(
        rows,
        "city string, ts_utc timestamp, temperature_c double, shortwave_radiation_wm2 double, "
        "wind_speed_100m_kmh double",
    )
    assert features.national_weather(weather).first().temperature_c == pytest.approx(10.0)


def test_calendar_uses_madrid_time(spark):
    df = spark.createDataFrame([(ts("2025-07-01 22:00"),)], ["hour_utc"])
    row = features.add_calendar(df, holidays_frame(spark, [2025])).first()
    assert (row.local_date, row.local_hour, row.day_of_week, row.month) == (date(2025, 7, 2), 0, 3, 7)
    assert row.is_weekend is False and row.is_holiday is False


def test_calendar_marks_holiday_and_weekend(spark):
    df = spark.createDataFrame([(ts("2025-12-08 10:00"),), (ts("2025-12-13 10:00"),)], ["hour_utc"])
    rows = features.add_calendar(df, holidays_frame(spark, [2025])).orderBy("hour_utc").collect()
    assert rows[0].is_holiday is True and rows[0].is_weekend is False
    assert rows[1].is_holiday is False and rows[1].is_weekend is True


def write(spark, rows, schema, path):
    spark.createDataFrame(rows, schema).write.parquet(str(path))


def test_build_hourly_features(spark, paths):
    processed = paths.processed
    generation_columns = ", ".join(f"{name} double" for name in features.GENERATION_COLUMNS)
    readings = [
        (ts("2025-01-15 10:00"), *[100.0] * len(features.GENERATION_COLUMNS)),
        (ts("2025-01-15 10:05"), *[300.0] * len(features.GENERATION_COLUMNS)),
    ]
    write(spark, readings, f"ts_utc timestamp, {generation_columns}", processed / "generation")
    write(
        spark,
        [(ts("2025-01-15 10:00"), 1.0, 190.0, 1.0)],
        "ts_utc timestamp, actual_demand_mw double, forecast_demand_mw double, scheduled_demand_mw double",
        processed / "demand_forecast",
    )
    prices = [(ts(f"2025-01-15 10:{minute:02d}"), "spot", 100.0 + minute) for minute in (0, 15, 30, 45)]
    prices.append((ts("2025-01-15 10:00"), "pvpc", 150.0))
    write(spark, prices, "ts_utc timestamp, series string, price_eur_mwh double", processed / "market_price")
    write(
        spark,
        [("madrid", ts("2025-01-15 10:00"), 8.0, 100.0, 10.0)],
        "city string, ts_utc timestamp, temperature_c double, shortwave_radiation_wm2 double, "
        "wind_speed_100m_kmh double",
        processed / "weather",
    )
    write(
        spark,
        [(date(2025, 1, 15), 45.0)],
        "delivery_date date, gas_price_eur_mwh double",
        processed / "gas_price",
    )

    row = features.build_hourly_features(spark, paths).first()
    assert row.hour_utc == ts("2025-01-15 10:00")
    assert row.demand_mw == 200.0
    assert row.renewable_share == pytest.approx(4 * 200.0 / 200.0)  # wind, PV, thermal and hydro
    assert row.forecast_demand_mw == 190.0
    assert row.actual_demand_mw == 1.0
    assert row.spot_price_eur_mwh == pytest.approx(122.5)
    assert row.pvpc_eur_mwh == 150.0
    assert row.temperature_c == 8.0
    assert row.gas_price_eur_mwh == 45.0
    assert (row.local_date, row.local_hour) == (date(2025, 1, 15), 11)


def test_build_hourly_features_tolerates_missing_sources(spark, paths):
    generation_columns = ", ".join(f"{name} double" for name in features.GENERATION_COLUMNS)
    readings = [(ts("2025-01-15 10:00"), *[100.0] * len(features.GENERATION_COLUMNS))]
    write(spark, readings, f"ts_utc timestamp, {generation_columns}", paths.processed / "generation")
    row = features.build_hourly_features(spark, paths).first()
    assert row.demand_mw == 100.0
    assert row.spot_price_eur_mwh is None and row.temperature_c is None and row.gas_price_eur_mwh is None
    assert row.actual_demand_mw is None and row.forecast_demand_mw is None


def test_write_hourly_features(spark, paths):
    generation_columns = ", ".join(f"{name} double" for name in features.GENERATION_COLUMNS)
    readings = [(ts("2025-01-15 10:00"), *[100.0] * len(features.GENERATION_COLUMNS))]
    write(spark, readings, f"ts_utc timestamp, {generation_columns}", paths.processed / "generation")
    assert features.write_hourly_features(spark, paths) == 1
    written = spark.read.parquet(str(paths.processed / "hourly_features"))
    assert {"year", "month", "hour_utc"} <= set(written.columns)
    assert written.select(F.max("demand_mw")).first()[0] == 100.0
