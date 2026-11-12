"""Regression tests for the findings of the phase 1 whole-branch review."""

import os
import shutil
import time
from datetime import UTC, datetime

import pandas as pd

from spain_electricity import pipeline, views
from spain_electricity.features import build_hourly_features


def test_run_ingest_skips_source_folder_without_files(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "ree_generation", paths.raw / "ree_generation")
    (paths.raw / "redata_demand" / "2025").mkdir(parents=True)
    (paths.raw / "mibgas").mkdir(parents=True)
    counts = pipeline.run_ingest(spark, paths)
    assert counts == {"generation": 1152, "hourly_features": 96}


def test_status_treats_dataset_without_parquet_as_missing(spark, paths):
    folder = paths.processed / "demand_forecast"
    folder.mkdir(parents=True)
    (folder / "_SUCCESS").write_bytes(b"")
    status = views.data_status(spark, paths).set_index("dataset")
    assert status.loc["demand_forecast", "rows"] == 0


def test_hourly_build_ignores_dataset_without_parquet(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "ree_generation", paths.raw / "ree_generation")
    pipeline.run_ingest(spark, paths)
    empty = paths.processed / "weather"
    empty.mkdir(parents=True, exist_ok=True)
    (empty / "_SUCCESS").write_bytes(b"")
    assert build_hourly_features(spark, paths).count() == 96


def test_status_times_are_utc_on_a_local_machine(spark, paths):
    spark.sql("SELECT timestamp'2025-01-15 10:00:00' AS ts_utc, 1.0 AS demand_mw").write.parquet(
        str(paths.processed / "generation")
    )
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Madrid"
    time.tzset()
    try:
        status = views.data_status(spark, paths).set_index("dataset")
    finally:
        os.environ["TZ"] = previous or "UTC"
        time.tzset()
    assert status.loc["generation", "first"] == pd.Timestamp("2025-01-15 10:00:00")


def land(paths, name, mtime):
    folder = paths.landing / "ree_generation"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(b"x")
    os.utime(path, (mtime, mtime))


READINGS = pd.DataFrame(
    {"ts_utc": [pd.Timestamp("2026-11-10 10:45")], "demand_mw": [1.0], "renewable_share": [0.5]}
)
NOW = datetime(2026, 11, 10, 17, 58, tzinfo=UTC)


def test_stream_mode(paths):
    assert views.stream_mode(paths) == "none"
    land(paths, "ree_generation_2025-01-01.jsonp", 1_700_000_000)
    assert views.stream_mode(paths) == "replay"
    land(paths, "20261110T175500Z.jsonp", 1_700_000_100)
    assert views.stream_mode(paths) == "live"


def test_stale_warning_when_live_feed_stalls(paths):
    land(paths, "20261110T175500Z.jsonp", 1_700_000_100)
    message = views.stale_warning(paths, READINGS, NOW)
    assert message is not None and "433 minutes" in message


def test_no_stale_warning_when_live_feed_is_fresh(paths):
    land(paths, "20261110T175500Z.jsonp", 1_700_000_100)
    fresh = READINGS.assign(ts_utc=[pd.Timestamp("2026-11-10 17:50")])
    assert views.stale_warning(paths, fresh, NOW) is None


def test_no_stale_warning_while_replaying(paths):
    land(paths, "ree_generation_2025-01-01.jsonp", 1_700_000_000)
    old = READINGS.assign(ts_utc=[pd.Timestamp("2025-01-01 10:00")])
    assert views.stale_warning(paths, old, NOW) is None


def test_replay_notice_says_the_data_is_old_and_how_to_go_live(paths):
    land(paths, "ree_generation_2025-01-03.jsonp", 1_700_000_000)
    replayed = READINGS.assign(ts_utc=[pd.Timestamp("2025-01-04 02:00")])
    message = views.replay_notice(paths, replayed)
    assert message is not None
    assert "04 Jan 2025" in message
    assert "capture_realtime.py" in message and "run_stream.py" in message


def test_no_replay_notice_when_capturing_live(paths):
    land(paths, "20261110T175500Z.jsonp", 1_700_000_100)
    assert views.replay_notice(paths, READINGS) is None
