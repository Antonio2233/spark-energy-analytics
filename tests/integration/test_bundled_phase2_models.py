import json

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from streamlit.testing.v1 import AppTest

from spain_electricity.config import REPO_ROOT
from spain_electricity.models import estimate_column
from spain_electricity.regression_models import SPECS

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("spec", SPECS)
@pytest.mark.parametrize("version", ["main", "update"])
def test_bundled_model_reproduces_predictions_without_processed_data(spark, spec, version):
    folder = REPO_ROOT / "data" / "models" / spec.name
    report = json.loads((folder / "metrics.json").read_text(encoding="utf-8"))
    if version == "update":
        folder, expected_hours = folder / "update", report["update"]["evaluation"]["hours"]
        saved = spark.read.parquet(str(folder / "predictions"))
    else:
        # Validation predictions come from the 2021-2024 fit; only test ones come from the saved model.
        expected_hours = report["evaluation"]["test"]["hours"]
        saved = spark.read.parquet(str(folder / "predictions")).where(F.col("split") == "test")
    model = PipelineModel.load(str(folder / "pipeline"))
    scored = model.transform(saved)
    result = scored.agg(
        F.count("*").alias("hours"),
        F.max(F.abs(F.col("prediction") - F.col(estimate_column(spec.label)))).alias("error"),
    ).first()
    assert result.hours == expected_hours
    assert result.error < 1e-9


def test_bundled_clusters_match_saved_assignments(spark):
    folder = REPO_ROOT / "data" / "models" / "day_types"
    report = json.loads((folder / "metrics.json").read_text(encoding="utf-8"))
    model = PipelineModel.load(str(folder / "pipeline"))
    assignments = spark.read.parquet(str(folder / "assignments"))
    assert model.stages[-1].getK() == report["chosen_k"]
    assert assignments.count() == report["assigned_days"]
    assert len({row["name"] for row in report["summary"]}) == report["chosen_k"]
    for row in report["summary"]:
        assert len(row["mean_profile"]) == 24
        assert sum(row["mean_profile"]) / 24 == pytest.approx(1)
        assert assignments.where(F.col("day_type") == row["day_type"]).count() == row["days"]


def test_models_panel_renders_all_bundled_results(spark):
    app = AppTest.from_string(
        "from spain_electricity.model_ui import render_models\n"
        "from spain_electricity.config import DataPaths, REPO_ROOT\n"
        "from spain_electricity.spark_session import get_spark\n"
        "from spain_electricity.charts import LIGHT\n"
        "render_models(get_spark(), DataPaths(REPO_ROOT / 'data'), LIGHT)\n",
        default_timeout=180,
    ).run()
    assert not app.exception
    headings = {item.value for item in app.subheader}
    assert {
        "Hourly peninsular demand",
        "Electricity price",
        "Renewable share of demand",
        "Daily demand types with KMeans",
    } <= headings
    month = next(item for item in app.selectbox if item.key == "price_main_month")
    if len(month.options) > 1:
        month.set_value(month.options[-1]).run()
        assert not app.exception
    version = next(item for item in app.radio if item.key == "model_version")
    version.set_value("update").run()
    assert not app.exception
