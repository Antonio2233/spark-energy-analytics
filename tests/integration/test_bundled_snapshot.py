"""Verify that a checkout contains intact data and a model that needs no new fitting."""

import hashlib
import json

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from spain_electricity.config import REPO_ROOT
from spain_electricity.models import add_model_features, valid_hours

pytestmark = pytest.mark.integration


def test_bundled_snapshot_hashes_match():
    root = REPO_ROOT / "data"
    manifest = json.loads((root / "snapshot_manifest.json").read_text(encoding="utf-8"))
    assert manifest["artifacts"]
    for item in manifest["artifacts"]:
        path = root / item["path"]
        assert path.resolve().is_relative_to(root.resolve())
        assert path.stat().st_size == item["bytes"], item["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"], item["path"]


def test_bundled_model_reproduces_saved_predictions_without_fitting(spark):
    root = REPO_ROOT / "data"
    model = PipelineModel.load(str(root / "models/demand/pipeline"))
    hourly = spark.read.parquet(str(root / "processed/hourly_features"))
    sample = valid_hours(hourly).where(F.year("local_date") == 2026).orderBy("hour_utc").limit(24)
    actual = model.transform(add_model_features(sample)).select("hour_utc", "prediction")
    expected = spark.read.parquet(str(root / "models/demand/predictions")).select(
        "hour_utc", "expected_demand_mw"
    )
    compared = (
        actual.join(expected, "hour_utc")
        .agg(
            F.count("*").alias("hours"),
            F.max(F.abs(F.col("prediction") - F.col("expected_demand_mw"))).alias("max_error"),
        )
        .first()
    )
    assert compared.hours == 24
    assert compared.max_error is not None and compared.max_error < 1e-6
