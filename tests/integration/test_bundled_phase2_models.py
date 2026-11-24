import json

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeansModel
from pyspark.sql import functions as F
from streamlit.testing.v1 import AppTest

from spain_electricity.config import REPO_ROOT
from spain_electricity.regression_models import SPECS

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("spec", SPECS)
def test_bundled_model_reproduces_predictions_without_processed_data(spark, spec):
    folder = REPO_ROOT / "data" / "models" / spec.name
    saved = spark.read.parquet(str(folder / "predictions")).withColumnRenamed(
        "prediction", "saved_prediction"
    )
    model = PipelineModel.load(str(folder / "pipeline"))
    scored = model.transform(saved)
    result = scored.agg(
        F.count("*").alias("hours"),
        F.max(F.abs(F.col("prediction") - F.col("saved_prediction"))).alias("error"),
    ).first()
    report = json.loads((folder / "metrics.json").read_text(encoding="utf-8"))
    assert result.hours == report["test"]["hours"]
    assert result.error < 1e-9


def test_bundled_clusters_match_saved_assignments(spark):
    folder = REPO_ROOT / "data" / "models" / "day_types"
    report = json.loads((folder / "metrics.json").read_text(encoding="utf-8"))
    model = KMeansModel.load(str(folder / "pipeline"))
    assignments = spark.read.parquet(str(folder / "assignments"))
    assert model.getK() == report["chosen_k"]
    assert assignments.count() == report["assigned_days"]
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
    assert {"Electricity price", "Renewable share of demand", "Daily demand types with KMeans"} <= headings
    month = next(item for item in app.selectbox if item.key == "price_month")
    if len(month.options) > 1:
        month.set_value(month.options[-1]).run()
        assert not app.exception
