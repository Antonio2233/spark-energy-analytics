import json
import shutil
from datetime import date, datetime, timedelta

import pytest
from pyspark.ml import PipelineModel
from streamlit.testing.v1 import AppTest

from spain_electricity import models, streaming, views
from spain_electricity.config import CITIES
from spain_electricity.features import add_calendar
from spain_electricity.national_holidays import holidays_frame

TINY_GRIDS = {
    "linear": {"regParam": [0.1]},
    "random_forest": {"numTrees": [8], "maxDepth": [4]},
    "gbt": {"maxIter": [5], "maxDepth": [3]},
}


@pytest.mark.integration
def test_train_save_monitor_and_render(spark, paths, fixtures_dir):
    rows = []
    for year, days in [(2024, 7), (2025, 3), (2026, 3)]:
        for offset in range(days * 24):
            stamp = datetime(year, 1, 8) + timedelta(hours=offset)
            temperature = 5.0 + offset % 24 / 3
            demand = 20000.0 + 100 * (offset % 24) + 200 * temperature
            # Missing baseline hours must be absent from BOTH evaluation scores.
            baseline = demand + 100 if offset != 0 else None
            rows.append((stamp, demand, temperature, baseline))
    hourly = spark.createDataFrame(
        rows, "hour_utc timestamp, demand_mw double, temperature_c double, peninsular_forecast_mw double"
    )
    hourly = add_calendar(hourly, holidays_frame(spark, [2024, 2025, 2026]))
    hourly.write.parquet(str(paths.processed / "hourly_features"))
    result = models.train_demand(spark, paths, grids=TINY_GRIDS)
    # Evaluation hours need REE's forecast and the same hour the day before: each block's first day
    # has no previous day, so 72 hours per evaluation block become 48.
    assert result["split_hours"] == {"train": 168, "validation": 48, "test": 48}
    assert result["evaluation"]["test"]["ree"]["mae"] == pytest.approx(100)
    # The synthetic demand repeats every day, so yesterday's value is exact.
    assert result["evaluation"]["test"]["same_hour_yesterday"]["mae"] == pytest.approx(0)
    assert "bias" in result["evaluation"]["test"]["model"]
    assert [candidate["model"] for candidate in result["candidates"]] == ["linear", "random_forest", "gbt"]
    # The winner is refit on every valid hour of 2021-2025 before the single 2026 evaluation.
    assert result["refit"]["hours"] == 168 + 72
    assert set(result["alert_rate"]) == {"validation", "test"}
    assert result["audit"]["duplicate_hours"] == 0
    assert result["alert_threshold_mw"] > 0
    assert models.load_metadata(paths)["selected"] == result["selected"]
    model = PipelineModel.load(str(paths.models / "demand" / "pipeline"))
    hours = spark.createDataFrame(
        [
            (datetime(2026, 1, 9, 10), 90000.0, 10.0, 12),
            (datetime(2026, 1, 9, 11), 90000.0, 10.0, 11),
            (datetime(2026, 1, 9, 12), 90000.0, None, 12),
        ],
        "hour_utc timestamp, avg_demand_mw double, temperature_c double, n_readings long",
    )
    predicted = models.predict_hours(hours, model, result["alert_threshold_mw"]).orderBy("hour_utc").collect()
    assert [row.prediction_status for row in predicted] == ["complete", "provisional", "missing_weather"]
    assert [row.demand_alert for row in predicted] == [True, False, False]
    assert predicted[2].expected_demand_mw is None

    landing = paths.landing / "ree_generation"
    landing.mkdir(parents=True)
    shutil.copy(fixtures_dir / "ree_generation/2025/01/ree_generation_2025-01-15.jsonp", landing)
    weather = [
        (city, datetime(2025, 1, 14) + timedelta(hours=offset), 10.0, 0.0, 5.0)
        for city in CITIES
        for offset in range(96)
    ]
    spark.createDataFrame(
        weather,
        "city string, ts_utc timestamp, temperature_c double, "
        "shortwave_radiation_wm2 double, wind_speed_100m_kmh double",
    ).write.parquet(str(paths.processed / "weather"))
    queries = streaming.start_stream(spark, paths, available_now=True)
    try:
        assert len(queries) == 3
        for query in queries:
            assert query.awaitTermination(120)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
    monitored = views.live_predictions(spark, paths)
    assert not monitored.empty and monitored.expected_demand_mw.notna().all()
    assert "complete" in set(monitored.prediction_status)

    # Missing and repeated live captures must not fall back to historical weather.
    (landing / "20260109T100000Z.jsonp").write_text("cb({});")
    assert streaming.monitoring_weather(spark, paths) is None
    folder = paths.landing / "open_meteo_current"
    folder.mkdir()
    payload = [
        {
            "current": {
                "time": "2026-01-09T10:00",
                "temperature_2m": 12.0,
                "shortwave_radiation": 1.0,
                "wind_speed_100m": 5.0,
            }
        }
        for _ in CITIES
    ]
    for minute in (0, 5):
        (folder / f"20260109T10{minute:02d}00Z.json").write_text(json.dumps(payload))
    current = streaming.monitoring_weather(spark, paths).collect()
    assert len(current) == 1 and current[0].temperature_c == pytest.approx(12)
    assert current[0].hour_utc.date() == date(2026, 1, 9)

    # Render the complete Models panel using the saved pipeline results.
    # AppTest picks its data root from the same environment as the real app.
    import os

    previous = os.environ.get("SPAIN_ELECTRICITY_DATA_DIR")
    os.environ["SPAIN_ELECTRICITY_DATA_DIR"] = str(paths.root)
    try:
        # The full app needs all standard hourly display columns; use a tiny isolated harness here.
        app = AppTest.from_string(
            "from spain_electricity.model_ui import render_models\n"
            "from spain_electricity.config import DataPaths\n"
            "from spain_electricity.spark_session import get_spark\n"
            "from spain_electricity.charts import LIGHT\n"
            "render_models(get_spark(), DataPaths.from_env(), LIGHT)\n",
            default_timeout=120,
        ).run()
        assert not app.exception
    finally:
        if previous is None:
            os.environ.pop("SPAIN_ELECTRICITY_DATA_DIR", None)
        else:
            os.environ["SPAIN_ELECTRICITY_DATA_DIR"] = previous
