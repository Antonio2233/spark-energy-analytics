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
    # Only test predictions come from the saved model: validation ones are from the model fitted on
    # 2021-2024, before the winner was refit on 2021-2025.
    saved = spark.read.parquet(str(root / "models/demand/predictions")).where(F.col("split") == "test")
    saved = saved.withColumn("day_of_week", (F.dayofweek("local_date") + 5) % 7 + 1)
    actual = model.transform(add_model_features(saved))
    result = actual.agg(
        F.count("*").alias("hours"),
        F.max(F.abs(F.col("prediction") - F.col("expected_demand_mw"))).alias("max_error"),
    ).first()
    assert result.hours > 0
    assert result.max_error is not None and result.max_error < 1e-6


def test_bundled_update_reproduces_its_november_predictions(spark):
    root = REPO_ROOT / "data" / "models" / "demand" / "update"
    model = PipelineModel.load(str(root / "pipeline"))
    saved = spark.read.parquet(str(root / "predictions"))
    saved = saved.withColumn("day_of_week", (F.dayofweek("local_date") + 5) % 7 + 1)
    actual = model.transform(add_model_features(saved))
    result = actual.agg(
        F.count("*").alias("hours"),
        F.max(F.abs(F.col("prediction") - F.col("expected_demand_mw"))).alias("max_error"),
        F.min("local_date").alias("first"),
    ).first()
    assert result.hours > 0
    assert result.max_error is not None and result.max_error < 1e-6
    assert result.first.isoformat() >= "2026-11-01"
