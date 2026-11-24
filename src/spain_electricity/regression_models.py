"""Small chronological Spark ML experiments for price and renewable-share estimation."""

import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import DataPaths
from spain_electricity.features import add_phase2_features
from spain_electricity.models import SPLITS


@dataclass(frozen=True)
class ModelSpec:
    """A target, its explanatory inputs and its exact 24-hour persistence baseline."""

    name: str
    label: str
    numeric: tuple[str, ...]
    categorical: tuple[str, ...]
    flags: tuple[str, ...]
    baseline: str
    unit: str


PRICE = ModelSpec(
    "price",
    "spot_price_eur_mwh",
    ("gas_price_eur_mwh", "renewable_share", "demand_mw"),
    ("local_hour",),
    ("iberian_exception",),
    "spot_price_eur_mwh_24h_ago",
    "EUR/MWh",
)
RENEWABLES = ModelSpec(
    "renewable_share",
    "renewable_share",
    ("wind_speed_100m_kmh", "shortwave_radiation_wm2"),
    ("local_hour", "month"),
    (),
    "renewable_share_24h_ago",
    "fraction of demand",
)
SPECS = (PRICE, RENEWABLES)


def complete_rows(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Require finite values, including valid negative prices and shares above one."""
    for column in columns:
        value = F.col(column).cast("double")
        df = df.where(value.isNotNull() & ~F.isnan(value) & (F.abs(value) < float("inf")))
    return df


def build_pipeline(spec: ModelSpec, family: str) -> Pipeline:
    """Fit encoding and scaling inside the training-only pipeline."""
    indices = [f"{name}_index" for name in spec.categorical]
    encoded = [f"{name}_encoded" for name in spec.categorical]
    flag_columns = [f"{name}_number" for name in spec.flags]
    from pyspark.ml.feature import SQLTransformer

    statement = (
        "SELECT *, "
        + ", ".join(f"CAST({name} AS DOUBLE) AS {name}_number" for name in spec.flags)
        + " FROM __THIS__"
    )
    stages = [SQLTransformer(statement=statement)] if spec.flags else []
    stages += [
        StringIndexer(inputCol=name, outputCol=index, handleInvalid="keep")
        for name, index in zip(spec.categorical, indices, strict=True)
    ]
    stages += [
        OneHotEncoder(inputCols=indices, outputCols=encoded, handleInvalid="keep"),
        VectorAssembler(inputCols=[*spec.numeric, *flag_columns, *encoded], outputCol="features_raw"),
    ]
    if family == "linear":
        stages += [
            StandardScaler(inputCol="features_raw", outputCol="features", withMean=False),
            LinearRegression(labelCol=spec.label, maxIter=60),
        ]
    elif family == "random_forest":
        stages += [
            RandomForestRegressor(
                labelCol=spec.label,
                featuresCol="features_raw",
                numTrees=24,
                maxDepth=6,
                minInstancesPerNode=2,
                seed=42,
            )
        ]
    else:
        raise ValueError(f"Unknown model family: {family}")
    return Pipeline(stages=stages)


def parameter_grid(pipeline: Pipeline, family: str) -> list:
    """Keep the search small, with every point evaluated on the temporal validation set."""
    estimator = pipeline.getStages()[-1]
    if family == "linear":
        return ParamGridBuilder().addGrid(estimator.regParam, [0.1, 10.0]).build()
    return ParamGridBuilder().addGrid(estimator.maxDepth, [4, 7]).build()


def evaluate(df: DataFrame, label: str, prediction: str) -> dict:
    """Measure regression quality on a preselected paired set of hours."""
    values = {
        metric: float(
            RegressionEvaluator(labelCol=label, predictionCol=prediction, metricName=metric).evaluate(df)
        )
        for metric in ("mae", "rmse", "r2")
    }
    return {key: value if math.isfinite(value) else None for key, value in values.items()}


def feature_weights(model: PipelineModel, transformed: DataFrame, vector: str) -> list[dict]:
    """Name every expanded feature using assembler metadata, including one-hot categories."""
    estimator = model.stages[-1]
    values = estimator.coefficients if hasattr(estimator, "coefficients") else estimator.featureImportances
    attributes = transformed.schema[vector].metadata["ml_attr"]["attrs"]
    names = {item["idx"]: item["name"] for group in attributes.values() for item in group}
    return [
        {"feature": names.get(index, f"feature_{index}"), "weight": float(value)}
        for index, value in enumerate(values)
    ]


def train(spark: SparkSession, paths: DataPaths, spec: ModelSpec) -> dict:
    """Choose on 2025, refit on train plus validation, and evaluate once on paired 2026 hours."""
    started = time.perf_counter()
    hourly = add_phase2_features(spark.read.parquet(str(paths.processed / "hourly_features")))
    if hourly.groupBy("hour_utc").count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate hourly keys; fix ingestion before training")
    required = (spec.label, *spec.numeric, *spec.categorical, *spec.flags)
    valid = complete_rows(hourly.where(~F.col("is_anomaly")), required)
    frames = {
        name: valid.where(F.year("local_date").between(first, last)) for name, (first, last) in SPLITS.items()
    }
    # Training may use every complete row; comparisons need both outputs on identical hours.
    for split in ("validation", "test"):
        frames[split] = complete_rows(frames[split], (spec.baseline,))
    frames = {key: value.cache() for key, value in frames.items()}
    try:
        counts = {key: value.count() for key, value in frames.items()}
        if counts["train"] < 24 or min(counts["validation"], counts["test"]) < 24:
            raise ValueError(
                f"Insufficient complete paired hours for {spec.name}: {counts}. Prepare market history first."
            )
        candidates, selected, best_mae = [], None, float("inf")
        for family in ("linear", "random_forest"):
            pipeline = build_pipeline(spec, family)
            for params in parameter_grid(pipeline, family):
                fitted = pipeline.fit(frames["train"], params)
                metrics = evaluate(fitted.transform(frames["validation"]), spec.label, "prediction")
                record = {
                    "model": family,
                    "parameters": {key.name: value for key, value in params.items()},
                    **metrics,
                }
                candidates.append(record)
                if metrics["mae"] is not None and metrics["mae"] < best_mae:
                    best_mae, selected = metrics["mae"], record
        if selected is None:
            raise ValueError("No finite validation candidate")
        pipeline = build_pipeline(spec, selected["model"])
        estimator = pipeline.getStages()[-1]
        params = {estimator.getParam(key): value for key, value in selected["parameters"].items()}
        final = pipeline.fit(frames["train"].unionByName(frames["validation"]), params)
        predictions = final.transform(frames["test"]).cache()
        try:
            test = {
                "model": evaluate(predictions, spec.label, "prediction"),
                "baseline": evaluate(predictions, spec.label, spec.baseline),
                "hours": counts["test"],
            }
            weights = feature_weights(final, predictions, "features_raw")
            target = paths.models / spec.name
            final.write().overwrite().save(str(target / "pipeline"))
            saved_columns = list(
                dict.fromkeys(
                    [
                        "hour_utc",
                        "local_date",
                        spec.label,
                        spec.baseline,
                        *spec.numeric,
                        *spec.categorical,
                        *spec.flags,
                        "prediction",
                    ]
                )
            )
            predictions.select(*saved_columns).write.mode("overwrite").parquet(str(target / "predictions"))
        finally:
            predictions.unpersist()
        report = {
            "label": spec.label,
            "baseline": spec.baseline,
            "unit": spec.unit,
            "selected": {key: selected[key] for key in ("model", "parameters")},
            "candidates": candidates,
            "validation_baseline": evaluate(frames["validation"], spec.label, spec.baseline),
            "test": test,
            "features": weights,
            "rows": counts,
            "weight_kind": "standardized coefficient" if selected["model"] == "linear" else "tree importance",
            "trained_at": datetime.now(UTC).isoformat(),
            "training_seconds": round(time.perf_counter() - started, 2),
            "interpretation": (
                "Contemporaneous explanatory estimate, not a day-ahead forecast. "
                "Baseline is exact UTC t-24h persistence. Results cover available complete hours."
            ),
        }
        (target / "metrics.json").write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
        return report
    finally:
        for frame in frames.values():
            frame.unpersist()
