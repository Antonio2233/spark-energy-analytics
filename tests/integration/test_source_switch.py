from datetime import datetime

import pytest
from pyspark.sql import functions as F
from streamlit.testing.v1 import AppTest

from spain_electricity import runtime_ui
from spain_electricity.config import REPO_ROOT
from spain_electricity.features import GENERATION_COLUMNS, write_hourly_features
from spain_electricity.live_runtime import LiveStatus


@pytest.mark.integration
def test_one_click_switches_source_and_switches_back_without_mixing(spark, paths, monkeypatch):
    frame = spark.createDataFrame(
        [(datetime(2026, 10, 11, 9), 30000.0)], "ts_utc timestamp, demand_mw double"
    )
    for column in GENERATION_COLUMNS:
        if column != "demand_mw":
            frame = frame.withColumn(column, F.lit(2000.0))
    frame.write.parquet(str(paths.processed / "generation"))
    write_hourly_features(spark, paths)
    live = (
        frame.withColumn("ts_utc", F.to_timestamp(F.lit("2026-11-16 09:00:00")))
        .withColumn("demand_mw", F.lit(40000.0))
        .withColumn("renewable_share", F.lit(0.4))
    )
    live.write.parquet(str(paths.with_stream("live").stream_output / "readings"))
    requests = []

    class Service:
        def request(self, client_id, enabled, **kwargs):
            requests.append(enabled)
            return LiveStatus("running" if enabled else "paused", enabled, None, None, None)

    monkeypatch.setattr(runtime_ui, "live_service", lambda *args: Service())
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(paths.root))
    app = AppTest.from_file(str(REPO_ROOT / "app/streamlit_app.py"), default_timeout=180).run()
    assert not app.exception
    assert {metric.label: metric.value for metric in app.metric}["DEMAND"] == "30.00 GW"
    app.radio(key="desk_source").set_value("Real time").run()
    assert not app.exception
    assert {metric.label: metric.value for metric in app.metric}["DEMAND"] == "40.00 GW"
    assert True in requests
    app.radio(key="desk_source").set_value("Historical").run()
    assert not app.exception
    assert {metric.label: metric.value for metric in app.metric}["DEMAND"] == "30.00 GW"
    assert requests[-1] is False
