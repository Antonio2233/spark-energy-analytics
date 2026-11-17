from datetime import UTC, date, datetime, timedelta

import pytest
from helpers import FakeResponse, FakeSession

from spain_electricity import capture, desk, views
from spain_electricity.features import GENERATION_COLUMNS


def seed_desk(spark, paths, mode="replay"):
    rows = [
        (datetime(2026, 10, 11, 8), 30000.0),
        (datetime(2026, 10, 11, 8, 5), 31000.0),
        (datetime(2026, 10, 11, 9), 32000.0),
        (datetime(2026, 10, 11, 9, 5), 33000.0),
    ]
    from pyspark.sql import functions as F

    readings = spark.createDataFrame(rows, "ts_utc timestamp, demand_mw double")
    for column in GENERATION_COLUMNS:
        if column != "demand_mw":
            readings = readings.withColumn(column, F.lit(2000.0))
    readings.withColumn("renewable_share", F.lit(0.4)).write.parquet(str(paths.stream_output / "readings"))
    landing = paths.landing / "ree_generation"
    landing.mkdir(parents=True)
    name = "ree_generation_2026-10-11.jsonp" if mode == "replay" else "20261011T090500Z.jsonp"
    (landing / name).write_text("cb({});")
    spark.createDataFrame(
        [
            (datetime(2026, 10, 11, 8), "spot", 50.0),
            (datetime(2026, 10, 11, 9), "spot", 80.0),
            (datetime(2026, 10, 11, 9), "pvpc", 100.0),
            (datetime(2026, 10, 11, 10), "spot", -5.0),
            (datetime(2026, 11, 16, 9), "spot", 999.0),
        ],
        "ts_utc timestamp, series string, price_eur_mwh double",
    ).write.parquet(str(paths.processed / "market_price"))
    spark.createDataFrame(
        [(date(2026, 10, 10), 35.0), (date(2026, 10, 11), 40.0), (date(2026, 11, 16), 100.0)],
        "delivery_date date, gas_price_eur_mwh double",
    ).write.parquet(str(paths.processed / "gas_price"))


def test_snapshot_aligns_delivery_periods_and_uses_exact_hour_deltas(spark, paths):
    seed_desk(spark, paths)
    result = desk.snapshot(spark, paths, efficiency=0.5)
    assert result.mode == "replay" and len(result.timeline) == 4
    assert result.summary["spot_price_eur_mwh"] == 80
    assert result.summary["gas_price_eur_mwh"] == 40
    assert result.summary["fuel_cost_eur_mwh"] == 80
    assert result.summary["fuel_spread_eur_mwh"] == 0
    assert result.summary["demand_mw_change_1h"] == 2000
    assert result.summary["residual_demand_mw"] == 19800
    assert result.summary["combined_cycle_share"] == pytest.approx(2000 / 33000)
    assert result.prices.spot_price_eur_mwh.tolist() == [50, 80, -5]
    assert result.mix.generation_fraction.sum() == pytest.approx(1)
    assert result.cities.empty
    with pytest.raises(ValueError):
        desk.snapshot(spark, paths, efficiency=0)


def test_live_context_never_relabels_replay_weather_or_prices(spark, paths):
    seed_desk(spark, paths, "live")
    result = desk.snapshot(spark, paths)
    assert result.mode == "live"
    assert result.timeline.spot_price_eur_mwh.isna().all()
    assert result.timeline.expected_demand_mw.isna().all()
    assert result.prices.empty
    assert result.summary["gas_price_eur_mwh"] == 40  # Same-day static gas reference is allowed.


def test_market_capture_throttles_and_survives_failed_refresh(paths):
    now = datetime(2026, 11, 16, 23, 30, tzinfo=UTC)
    price = b'{"included":[]}'
    forecast = b'cb({"valoresPrevistaProgramada":[{"ts":"2026-11-17 01:00","pre":30000}]});'
    session = FakeSession(lambda url: FakeResponse(200, price if "apidatos" in url else forecast))
    files = capture.capture_market_once(paths, now, session=session)
    assert len(files) == 2 and all("2026-11-17" in str(file) for file in files)
    assert all("2026-11-17" in url for url in session.urls)
    assert capture.capture_market_once(paths, now + timedelta(minutes=5), session=session) == []
    assert len(session.urls) == 2
    failed = FakeSession(lambda url: FakeResponse(503, b"unavailable"))
    assert capture.capture_market_once(paths, now + timedelta(minutes=16), session=failed) == []
    assert files[0].read_bytes() == price


def test_analysis_omits_absent_or_constant_sources(spark, paths):
    rows = [(float(hour), float(hour * 2), None, 1.0) for hour in range(30)]
    spark.createDataFrame(
        rows, "demand_mw double, temperature_c double, gas_price_eur_mwh double, wind_speed_100m_kmh double"
    ).write.parquet(str(paths.processed / "hourly_features"))
    available = views.available_correlation_columns(
        spark, paths, ["demand_mw", "temperature_c", "gas_price_eur_mwh", "wind_speed_100m_kmh"]
    )
    assert available == ["demand_mw", "temperature_c"]


def test_historical_day_never_reads_later_live_or_replay_events(spark, paths):
    import json

    from pyspark.sql import functions as F

    seed_desk(spark, paths)
    original = spark.read.parquet(str(paths.stream_output / "readings")).drop("renewable_share")
    future = original.limit(1).withColumn("ts_utc", F.to_timestamp(F.lit("2026-11-16 09:05:00")))
    original.unionByName(future).write.parquet(str(paths.processed / "generation"))
    model_root = paths.models / "demand"
    model_root.mkdir(parents=True)
    (model_root / "metrics.json").write_text(json.dumps({"alert_threshold_mw": 1000.0}))
    spark.createDataFrame(
        [(datetime(2026, 10, 11, 9), 30000.0)], "hour_utc timestamp, expected_demand_mw double"
    ).write.parquet(str(model_root / "predictions"))
    result = desk.snapshot(spark, paths.with_stream("live"), historical_day=date(2026, 10, 11))
    assert result.mode == "historical" and result.asof.date() == date(2026, 10, 11)
    assert len(result.timeline) == 4 and result.summary["expected_demand_mw"] == 30000
    assert result.summary["spot_price_eur_mwh"] == 80
    assert result.monitoring.hour_utc.max().date() == date(2026, 10, 11)
    assert not result.monitoring.demand_alert.any()  # Partial hours never alert.


def test_in_progress_model_batch_is_not_visible_to_the_dashboard(spark, paths):
    from spain_electricity.storage import completed_batches

    target = paths.stream_output / "hourly" / "batch_id=1"
    target.mkdir(parents=True)
    (target / "part-in-progress.parquet").write_text("not committed")
    assert completed_batches(target.parent) == []
    assert views.live_predictions(spark, paths).empty
