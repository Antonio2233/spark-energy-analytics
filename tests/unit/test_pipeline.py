from datetime import date

import pytest

from spain_electricity import pipeline


def test_download_jobs_per_source(paths):
    start, end = date(2025, 1, 1), date(2025, 1, 31)
    counts = {source: len(pipeline.download_jobs([source], start, end, paths)) for source in pipeline.SOURCES}
    assert counts == {
        "ree_generation": 31,
        "redata_demand": 1,
        "redata_price": 1,
        "open_meteo": 5,
        "mibgas": 1,
    }


def test_download_jobs_rejects_unknown_source(paths):
    with pytest.raises(ValueError, match="unknown source"):
        pipeline.download_jobs(["weather_station"], date(2025, 1, 1), date(2025, 1, 2), paths)


def test_run_ingest_on_empty_data(spark, paths):
    assert pipeline.run_ingest(spark, paths) == {}
