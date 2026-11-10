from datetime import date

import pytest
from helpers import copy_raw_fixtures
from streamlit.testing.v1 import AppTest

from spain_electricity.config import REPO_ROOT
from spain_electricity.pipeline import run_ingest

APP = str(REPO_ROOT / "app" / "streamlit_app.py")


@pytest.mark.integration
def test_app_runs_on_empty_data(spark, monkeypatch, tmp_path):
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(tmp_path))
    app = AppTest.from_file(APP, default_timeout=180).run()
    assert not app.exception
    assert any("No data yet" in info.value for info in app.info)


@pytest.mark.integration
def test_app_runs_with_ingested_fixtures(spark, monkeypatch, paths, fixtures_dir):
    copy_raw_fixtures(fixtures_dir, paths)
    run_ingest(spark, paths)
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(paths.root))
    app = AppTest.from_file(APP, default_timeout=180).run()
    assert not app.exception
    assert not any("No data yet" in info.value for info in app.info)
    assert len(app.tabs) == 4


@pytest.mark.integration
def test_history_tolerates_a_single_selected_date(spark, monkeypatch, paths, fixtures_dir):
    copy_raw_fixtures(fixtures_dir, paths)
    run_ingest(spark, paths)
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(paths.root))
    app = AppTest.from_file(APP, default_timeout=180).run()
    app.date_input[0].set_value((date(2025, 10, 26),)).run()
    assert not app.exception
