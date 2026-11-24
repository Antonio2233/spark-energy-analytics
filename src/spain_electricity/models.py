"""Small demand pipelines, chronological evaluation and validation-calibrated monitoring."""

import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import ANOMALY_DATES, DataPaths
from spain_electricity.features import HOLIDAY_YEARS, add_calendar
from spain_electricity.national_holidays import holidays_frame

BASELINE = "peninsular_forecast_mw"
TARGET = "demand_mw"
CATEGORICAL = ("local_hour", "day_of_week", "month", "working_hour")
NUMERIC = (
    "temperature_c",
    "heating_degrees",
    "cooling_degrees",
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    "holiday_number",
    "weekend_number",
)
SPLITS = {"train": (2021, 2024), "validation": (2025, 2025), "test": (2026, 2026)}


def add_model_features(df: DataFrame) -> DataFrame:
    """Derive model features; the saved demand pipeline was fitted with 18/22 °C bases.

    These bases must remain unchanged when serving that model, despite the original 16/20 plan.
    """
    working = ~F.col("is_weekend") & ~F.col("is_holiday")
    return (
        df.withColumn("heating_degrees", F.greatest(F.lit(18.0) - F.col("temperature_c"), F.lit(0.0)))
        .withColumn("cooling_degrees", F.greatest(F.col("temperature_c") - F.lit(22.0), F.lit(0.0)))
        .withColumn("hour_sin", F.sin(F.col("local_hour") * (2 * math.pi / 24)))
        .withColumn("hour_cos", F.cos(F.col("local_hour") * (2 * math.pi / 24)))
        .withColumn("month_sin", F.sin((F.col("month") - 1) * (2 * math.pi / 12)))
        .withColumn("month_cos", F.cos((F.col("month") - 1) * (2 * math.pi / 12)))
        .withColumn("holiday_number", F.col("is_holiday").cast("double"))
        .withColumn("weekend_number", F.col("is_weekend").cast("double"))
        .withColumn("working_hour", F.col("local_hour") + F.when(working, 0).otherwise(24))
    )


def valid_hours(df: DataFrame) -> DataFrame:
    """Exclude missing/non-finite features, nonpositive demand and the documented blackout."""
    return df.where(
        (F.col(TARGET) > 0)
        & (F.abs(F.col(TARGET)) < float("inf"))
        & F.col("temperature_c").isNotNull()
        & ~F.isnan("temperature_c")
        & (F.abs(F.col("temperature_c")) < float("inf"))
        & ~F.col("local_date").cast("string").isin(list(ANOMALY_DATES))
    )


def temporal_split(df: DataFrame) -> dict[str, DataFrame]:
    """Split by local year, never by randomly shuffled rows."""
    return {
        name: df.where(F.year("local_date").between(first, last)) for name, (first, last) in SPLITS.items()
    }


def paired_rows(df: DataFrame) -> DataFrame:
    """Keep exactly the same hours for model and peninsular baseline evaluation."""
    return df.where((F.col(BASELINE) > 0) & (F.abs(F.col(BASELINE)) < float("inf")))


def build_pipeline(kind: str, strength: float = 0.1, depth: int = 6) -> Pipeline:
    """Build a training-only preprocessing pipeline and a bounded-size estimator."""
    indices = [f"{column}_index" for column in CATEGORICAL]
    encoded = [f"{column}_encoded" for column in CATEGORICAL]
    stages = [
        StringIndexer(inputCol=column, outputCol=index, handleInvalid="keep")
        for column, index in zip(CATEGORICAL, indices, strict=True)
    ]
    stages.extend(
        [
            OneHotEncoder(inputCols=indices, outputCols=encoded, handleInvalid="keep"),
            VectorAssembler(inputCols=[*NUMERIC, *encoded], outputCol="unscaled_features"),
            StandardScaler(inputCol="unscaled_features", outputCol="features", withMean=False),
        ]
    )
    if kind == "linear":
        estimator = LinearRegression(labelCol=TARGET, regParam=strength, maxIter=50)
    elif kind == "random_forest":
        estimator = RandomForestRegressor(
            labelCol=TARGET,
            numTrees=24,
            maxDepth=depth,
            seed=42,
            minInstancesPerNode=4,
            featureSubsetStrategy="onethird",
        )
    else:
        raise ValueError(f"Unknown model: {kind}")
    return Pipeline(stages=[*stages, estimator])


def regression_metrics(df: DataFrame, prediction: str) -> dict[str, float]:
    """Evaluate a fixed paired frame with Spark's regression evaluators."""
    return {
        metric: float(
            RegressionEvaluator(labelCol=TARGET, predictionCol=prediction, metricName=metric).evaluate(df)
        )
        for metric in ("mae", "rmse", "r2")
    }


def audit_hourly(df: DataFrame) -> dict:
    """Measure actual coverage, missingness and duplicate hours before fitting anything."""
    columns = [TARGET, "temperature_c", BASELINE]
    summary = (
        df.groupBy(F.year("local_date").alias("year"))
        .agg(
            F.count("*").alias("hours"),
            F.countDistinct("local_date").alias("days"),
            F.min("hour_utc").alias("first"),
            F.max("hour_utc").alias("last"),
            *[
                F.sum(F.when(F.col(column).isNull() | F.isnan(column), 1).otherwise(0)).alias(
                    f"missing_{column}"
                )
                for column in columns
            ],
        )
        .orderBy("year")
        .collect()
    )
    duplicates = df.groupBy("hour_utc").count().where("count > 1").count()
    return {"by_year": [row.asDict() for row in summary], "duplicate_hours": duplicates}


def metadata_path(paths: DataPaths) -> Path:
    """Return the completion marker and measured results of demand training."""
    return paths.models / "demand" / "metrics.json"


def load_metadata(paths: DataPaths) -> dict | None:
    """Read saved metrics, or return None before the first successful training."""
    path = metadata_path(paths)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def train_demand(spark: SparkSession, paths: DataPaths) -> dict:
    """Select four small candidates on 2025 MAE, then evaluate the winner once on 2026."""
    started = time.perf_counter()
    hourly = spark.read.parquet(str(paths.processed / "hourly_features"))
    audit = audit_hourly(hourly)
    if audit["duplicate_hours"]:
        raise ValueError("Duplicate hourly keys: fix ingestion before training")
    frames = temporal_split(add_model_features(valid_hours(hourly)))
    frames["validation"] = paired_rows(frames["validation"])
    frames["test"] = paired_rows(frames["test"])
    frames = {name: frame.cache() for name, frame in frames.items()}
    try:
        counts = {name: frame.count() for name, frame in frames.items()}
        if counts["train"] < 100 or min(counts["validation"], counts["test"]) < 24:
            raise ValueError(
                f"Insufficient complete hours per temporal split: {counts}; prepare the sample first"
            )
        specifications = [
            ("linear", {"strength": 0.1}),
            ("linear", {"strength": 10.0}),
            ("random_forest", {"depth": 5}),
            ("random_forest", {"depth": 8}),
        ]
        candidates, best, best_mae = [], None, float("inf")
        for kind, parameters in specifications:
            fitted = build_pipeline(kind, **parameters).fit(frames["train"])
            scored = fitted.transform(frames["validation"]).cache()
            try:
                metrics = regression_metrics(scored, "prediction")
            finally:
                scored.unpersist()
            candidates.append({"model": kind, "parameters": parameters, **metrics})
            if metrics["mae"] < best_mae:
                best, best_mae = fitted, metrics["mae"]
                selected = {"model": kind, "parameters": parameters}
        if best is None:
            raise ValueError("No candidate produced finite validation metrics")
        validation = best.transform(frames["validation"])
        errors = validation.select(F.abs(F.col(TARGET) - F.col("prediction")).alias("error"))
        threshold = max(1.0, errors.approxQuantile("error", [0.99], 0.0)[0])
        scores, predictions = {}, []
        for split in ("validation", "test"):
            scored = best.transform(frames[split]).cache()
            try:
                scores[split] = {
                    "model": regression_metrics(scored, "prediction"),
                    "ree": regression_metrics(scored, BASELINE),
                    "hours": counts[split],
                }
                predictions.append(
                    scored.select(
                        "hour_utc",
                        "local_date",
                        "local_hour",
                        "month",
                        "is_weekend",
                        "is_holiday",
                        TARGET,
                        "temperature_c",
                        BASELINE,
                        F.col("prediction").alias("expected_demand_mw"),
                        (F.col(TARGET) - F.col("prediction")).alias("demand_error_mw"),
                        F.lit(split).alias("split"),
                    )
                )
            finally:
                scored.unpersist()
        target = paths.models / "demand"
        best.write().overwrite().save(str(target / "pipeline"))
        predictions[0].unionByName(predictions[1]).write.mode("overwrite").parquet(
            str(target / "predictions")
        )
        sample_path = paths.root / "demand_sample.json"
        sample = json.loads(sample_path.read_text(encoding="utf-8")) if sample_path.exists() else None
        from spain_electricity.regression_models import feature_weights

        weights = feature_weights(best, best.transform(frames["validation"]), "unscaled_features")
        result = {
            "features": weights,
            "trained_at": datetime.now(UTC).isoformat(),
            "selected": selected,
            "candidates": candidates,
            "evaluation": scores,
            "split_hours": counts,
            "audit": audit,
            "sample": sample,
            "alert_threshold_mw": threshold,
            "alert_calibration": "99th percentile of absolute validation residuals; complete hours only",
            "interpretation": "Contemporaneous weather/calendar estimate, not a day-ahead forecast. "
            "Sampled dates do not establish performance over complete years.",
            "training_seconds": round(time.perf_counter() - started, 2),
        }
        target.mkdir(parents=True, exist_ok=True)
        partial = metadata_path(paths).with_suffix(".json.part")
        partial.write_text(json.dumps(result, indent=2, default=str, allow_nan=False), encoding="utf-8")
        partial.replace(metadata_path(paths))
        return result
    finally:
        for frame in frames.values():
            frame.unpersist()


def predict_hours(hours: DataFrame, model: PipelineModel, threshold: float) -> DataFrame:
    """Keep missing-weather hours visible and only alert after twelve unique readings."""
    calendar = add_calendar(hours, holidays_frame(hours.sparkSession, HOLIDAY_YEARS))
    valid = calendar.where(
        F.col("temperature_c").isNotNull()
        & ~F.isnan("temperature_c")
        & (F.abs(F.col("temperature_c")) < float("inf"))
    )
    predictions = model.transform(add_model_features(valid)).select(
        "hour_utc", F.col("prediction").alias("expected_demand_mw")
    )
    return (
        hours.join(predictions, "hour_utc", "left")
        .withColumn("demand_error_mw", F.col("avg_demand_mw") - F.col("expected_demand_mw"))
        .withColumn("alert_threshold_mw", F.lit(threshold))
        .withColumn(
            "prediction_status",
            F.when(F.col("expected_demand_mw").isNull(), "missing_weather")
            .when(F.col("n_readings") < 12, "provisional")
            .otherwise("complete"),
        )
        .withColumn(
            "demand_alert",
            F.coalesce(
                (F.col("prediction_status") == "complete") & (F.abs("demand_error_mw") > threshold),
                F.lit(False),
            ),
        )
    )
