from datetime import datetime

import pytest
from streamlit.testing.v1 import AppTest

from spain_electricity.config import REPO_ROOT


@pytest.mark.integration
def test_desk_renders_streamed_generation_and_unavailable_markets(spark, paths, monkeypatch):
    from pyspark.sql import functions as F

    from spain_electricity.features import GENERATION_COLUMNS

    readings = spark.createDataFrame(
        [(datetime(2026, 10, 11, 9), 33000.0)], "ts_utc timestamp, demand_mw double"
    )
    for column in GENERATION_COLUMNS:
        if column != "demand_mw":
            readings = readings.withColumn(column, F.lit(2000.0))
    readings.withColumn("renewable_share", F.lit(0.4)).write.parquet(str(paths.stream_output / "readings"))
    landing = paths.landing / "ree_generation"
    landing.mkdir(parents=True)
    (landing / "ree_generation_2026-10-11.jsonp").write_text("cb({});")
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(paths.root))
    app = AppTest.from_file(str(REPO_ROOT / "app/streamlit_app.py"), default_timeout=180).run()
    assert not app.exception
    values = {metric.label: metric.value for metric in app.metric}
    assert values["DEMAND"] == "33.00 GW"
    assert values["RENEWABLES / LOAD"] == "40.0%"
    assert values["DAY-AHEAD SPOT"] == "—"
    assert any("no day-ahead price curve" in info.value.lower() for info in app.info)
    app.selectbox[0].set_value(6).run()
    assert not app.exception
