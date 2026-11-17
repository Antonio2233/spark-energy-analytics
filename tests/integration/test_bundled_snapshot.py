"""Verify that the saved model reproduces bundled predictions without historical data."""

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from spain_electricity.config import REPO_ROOT
from spain_electricity.models import add_model_features

pytestmark = pytest.mark.integration


def test_bundled_model_reproduces_saved_predictions_without_fitting(spark):
    root = REPO_ROOT / "data"
    model = PipelineModel.load(str(root / "models/demand/pipeline"))
    saved = spark.read.parquet(str(root / "models/demand/predictions"))
    saved = saved.withColumn("day_of_week", (F.dayofweek("local_date") + 5) % 7 + 1)
    actual = model.transform(add_model_features(saved))
    result = actual.agg(
        F.count("*").alias("hours"),
        F.max(F.abs(F.col("prediction") - F.col("expected_demand_mw"))).alias("max_error"),
    ).first()
    assert result.hours > 0
    assert result.max_error is not None and result.max_error < 1e-6
