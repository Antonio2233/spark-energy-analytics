"""Demand pipelines tuned by hand on 2025, refit on 2021-2025 and tested once on 2026 against baselines.

The winner is also refit on every valid hour before ``UPDATE_CUTOFF`` and scored on the hours after it,
to measure what the 2026 data adds to the model refit on 2021-2025.
"""

import json
import math
import shutil
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import ANOMALY_DATES, DataPaths
from spain_electricity.features import HOLIDAY_YEARS, add_calendar, add_lag_24h
from spain_electricity.national_holidays import holidays_frame
from spain_electricity.sampling import sampled_history

TARGET = "demand_mw"
BASELINE = "peninsular_forecast_mw"
# Same hour one day (24 h) earlier: the naive forecast any model should beat.
NAIVE = f"{TARGET}_24h_ago"
BASELINES = {"ree": BASELINE, "same_hour_yesterday": NAIVE}
# Hyperparameter grids, looped by hand: Spark's CrossValidator and TrainValidationSplit split rows at random.
GRIDS = {
    "linear": {"regParam": [0.1, 1.0, 10.0], "elasticNetParam": [0.0, 0.5]},
    "random_forest": {"numTrees": [60], "maxDepth": [8, 12]},
    "gbt": {"maxIter": [60, 120], "maxDepth": [4, 6]},
}
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
# The update refits the configuration selected on 2025 on every valid hour before this date, 2026
# included, and is scored from this date on: November 2026, the month after the last complete one.
UPDATE_CUTOFF = date(2026, 11, 1)
# Minimum paired hours to score the update; with fewer it is skipped and its folder removed.
MIN_UPDATE_HOURS = 24


def estimate_column(label: str, previous: bool = False) -> str:
    """Name the saved estimate of ``label``; ``previous`` names the 2021-2025 model's on update hours."""
    return f"expected_{label}_through_2025" if previous else f"expected_{label}"


def update_split(df: DataFrame, cutoff: date) -> tuple[DataFrame, DataFrame]:
    """Split by local date into the update's training hours (before ``cutoff``) and its evaluation hours."""
    return df.where(F.col("local_date") < F.lit(cutoff)), df.where(F.col("local_date") >= F.lit(cutoff))


def update_period(training_hours: int, evaluation: DataFrame, cutoff: date) -> dict:
    """Describe the update's training end and its measured evaluation dates."""
    last = evaluation.agg(F.max("local_date")).first()[0]
    return {
        "train_until": (cutoff - timedelta(days=1)).isoformat(),
        "training_hours": training_hours,
        "evaluated_from": cutoff.isoformat(),
        "evaluated_until": last.isoformat(),
    }


def clear_update(folder: Path) -> None:
    """Remove a previous update so a run without update hours leaves no stale artifacts."""
    if folder.exists():
        shutil.rmtree(folder)


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


def paired_rows(df: DataFrame, baselines: Sequence[str] = (BASELINE,)) -> DataFrame:
    """Keep exactly the hours where every baseline exists, so model and baselines share their rows."""
    condition = F.lit(True)
    for column in baselines:
        condition = condition & (F.col(column) > 0) & (F.abs(F.col(column)) < float("inf"))
    return df.where(condition)


def add_naive_baseline(df: DataFrame) -> DataFrame:
    """Add the demand 24 hours earlier as ``NAIVE``; null when that hour is missing or excluded."""
    return add_lag_24h(df, TARGET)


def build_pipeline(kind: str) -> Pipeline:
    """Build training-only preprocessing plus one estimator family with default parameters."""
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
        estimator = LinearRegression(labelCol=TARGET, maxIter=100)
    elif kind == "random_forest":
        estimator = RandomForestRegressor(
            labelCol=TARGET, seed=42, minInstancesPerNode=4, featureSubsetStrategy="onethird"
        )
    elif kind == "gbt":
        estimator = GBTRegressor(labelCol=TARGET, seed=42, minInstancesPerNode=4)
    else:
        raise ValueError(f"Unknown model: {kind}")
    return Pipeline(stages=[*stages, estimator])


def candidate_grid(grids: dict[str, dict[str, list]] | None = None) -> list[tuple[str, Pipeline, list[dict]]]:
    """Return, per family, its pipeline and the parameter maps built with ``ParamGridBuilder``."""
    candidates = []
    for kind, values in (grids or GRIDS).items():
        pipeline = build_pipeline(kind)
        estimator = pipeline.getStages()[-1]
        builder = ParamGridBuilder()
        for name, options in values.items():
            builder.addGrid(estimator.getParam(name), options)
        candidates.append((kind, pipeline, builder.build()))
    return candidates


def regression_metrics(df: DataFrame, prediction: str) -> dict[str, float]:
    """Evaluate a fixed paired frame: Spark's MAE, RMSE and R², plus the mean error (bias)."""
    metrics = {
        metric: float(
            RegressionEvaluator(labelCol=TARGET, predictionCol=prediction, metricName=metric).evaluate(df)
        )
        for metric in ("mae", "rmse", "r2")
    }
    metrics["bias"] = float(df.select(F.avg(F.col(TARGET) - F.col(prediction))).first()[0])
    return metrics


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


def _alert_rate(scored: DataFrame, threshold: float) -> float:
    """Share of hours whose absolute model error exceeds the alert threshold."""
    return float(
        scored.select(F.avg((F.abs(F.col(TARGET) - F.col("prediction")) > threshold).cast("double"))).first()[
            0
        ]
    )


def _predictions(scored: DataFrame, split: str, *extra: str) -> DataFrame:
    """Select the saved evaluation columns of a scored split."""
    return scored.select(
        "hour_utc",
        "local_date",
        "local_hour",
        "month",
        "is_weekend",
        "is_holiday",
        TARGET,
        "temperature_c",
        BASELINE,
        NAIVE,
        F.col("prediction").alias(estimate_column(TARGET)),
        (F.col(TARGET) - F.col("prediction")).alias("demand_error_mw"),
        *extra,
        F.lit(split).alias("split"),
    )


def _train_update(
    pipeline: Pipeline,
    params: dict,
    final: PipelineModel,
    frames: dict[str, DataFrame],
    threshold: float,
    target: Path,
    cutoff: date,
) -> dict | None:
    """Refit the winner through the day before ``cutoff`` and score it beside the 2021-2025 model."""
    hours = frames["update"].count()
    if hours < MIN_UPDATE_HOURS:
        clear_update(target / "update")
        return None
    updated = pipeline.fit(frames["update_train"], params=params)
    previous = final.transform(frames["update"]).select(
        "hour_utc", F.col("prediction").alias(estimate_column(TARGET, previous=True))
    )
    scored = updated.transform(frames["update"]).join(previous, "hour_utc").cache()
    try:
        through_2025 = estimate_column(TARGET, previous=True)
        result = {
            **update_period(frames["update_train"].count(), frames["update"], cutoff),
            "evaluation": {
                "model": regression_metrics(scored, "prediction"),
                "model_through_2025": regression_metrics(scored, through_2025),
                **{name: regression_metrics(scored, column) for name, column in BASELINES.items()},
                "hours": hours,
            },
            "alert_rate": {
                "model": _alert_rate(scored, threshold),
                "model_through_2025": _alert_rate(
                    scored.withColumn("prediction", F.col(through_2025)), threshold
                ),
            },
        }
        result["features"] = feature_weights(updated, scored, "unscaled_features")
        updated.write().overwrite().save(str(target / "update" / "pipeline"))
        _predictions(scored, "update", through_2025).write.mode("overwrite").parquet(
            str(target / "update" / "predictions")
        )
        return result
    finally:
        scored.unpersist()


def train_demand(
    spark: SparkSession,
    paths: DataPaths,
    grids: dict[str, dict[str, list]] | None = None,
    update_cutoff: date = UPDATE_CUTOFF,
) -> dict:
    """Tune every family by hand on 2025, refit the winner on 2021-2025 and evaluate it once on 2026.

    Validation and test hours are paired: each one has REE's peninsular forecast and the same hour the
    day before, so the model and both baselines are scored on identical rows. The alert threshold comes
    from the 2025 residuals of the model fitted on 2021-2024, before the refit sees 2025. The same winner
    is then refit on every valid hour before ``update_cutoff`` and scored on the paired hours after it.
    """
    started = time.perf_counter()
    hourly = spark.read.parquet(str(paths.processed / "hourly_features"))
    audit = audit_hourly(hourly)
    if audit["duplicate_hours"]:
        raise ValueError("Duplicate hourly keys: fix ingestion before training")
    featured = add_naive_baseline(add_model_features(valid_hours(hourly)))
    frames = temporal_split(featured)
    frames["refit"] = frames["train"].unionByName(frames["validation"])
    for split in ("validation", "test"):
        frames[f"{split}_paired"] = paired_rows(frames[split], tuple(BASELINES.values()))
    frames["update_train"], update_hours = update_split(featured, update_cutoff)
    frames["update"] = paired_rows(update_hours, tuple(BASELINES.values()))
    frames = {name: frame.cache() for name, frame in frames.items()}
    try:
        counts = {
            "train": frames["train"].count(),
            "validation": frames["validation_paired"].count(),
            "test": frames["test_paired"].count(),
        }
        if counts["train"] < 100 or min(counts["validation"], counts["test"]) < 24:
            raise ValueError(
                f"Insufficient complete hours per temporal split: {counts}; ingest the history first"
            )
        candidates, best, best_mae = [], None, float("inf")
        for kind, pipeline, param_maps in candidate_grid(grids):
            for params in param_maps:
                fitted = pipeline.fit(frames["train"], params=params)
                scored = fitted.transform(frames["validation_paired"]).cache()
                try:
                    metrics = regression_metrics(scored, "prediction")
                finally:
                    scored.unpersist()
                parameters = {param.name: value for param, value in params.items()}
                candidates.append({"model": kind, "parameters": parameters, **metrics})
                if metrics["mae"] < best_mae:
                    best, best_mae = (pipeline, params, fitted), metrics["mae"]
                    selected = {"model": kind, "parameters": parameters}
        if best is None:
            raise ValueError("No candidate produced finite validation metrics")
        pipeline, params, fitted = best
        validation = fitted.transform(frames["validation_paired"]).cache()
        errors = validation.select(F.abs(F.col(TARGET) - F.col("prediction")).alias("error"))
        threshold = max(1.0, errors.approxQuantile("error", [0.99], 0.0)[0])
        final = pipeline.fit(frames["refit"], params=params)
        test = final.transform(frames["test_paired"]).cache()
        scores, alert_rate = {}, {}
        try:
            for split, scored in (("validation", validation), ("test", test)):
                scores[split] = {
                    "model": regression_metrics(scored, "prediction"),
                    **{name: regression_metrics(scored, column) for name, column in BASELINES.items()},
                    "hours": counts[split],
                }
                alert_rate[split] = _alert_rate(scored, threshold)
            target = paths.models / "demand"
            final.write().overwrite().save(str(target / "pipeline"))
            update = _train_update(pipeline, params, final, frames, threshold, target, update_cutoff)
            _predictions(validation, "validation").unionByName(_predictions(test, "test")).write.mode(
                "overwrite"
            ).parquet(str(target / "predictions"))
        finally:
            validation.unpersist()
            test.unpersist()
        sample = sampled_history(paths, hourly)
        # Weights of the saved model, the winner refit on 2021-2025.
        weights = feature_weights(final, final.transform(frames["validation_paired"]), "unscaled_features")
        scope = (
            "Sampled dates do not establish performance over complete years."
            if sample
            else "Fitted on every complete hour of 2021-2024, selected on 2025, refit on 2021-2025 and "
            "evaluated once on 2026."
        )
        result = {
            "label": TARGET,
            "unit": "MW",
            "baselines": BASELINES,
            "features": weights,
            "weight_kind": "standardized coefficient" if selected["model"] == "linear" else "tree importance",
            "trained_at": datetime.now(UTC).isoformat(),
            "selected": selected,
            "candidates": candidates,
            "evaluation": scores,
            "split_hours": counts,
            "refit": {"years": "2021-2025", "hours": frames["refit"].count()},
            "audit": audit,
            "sample": sample,
            "alert_threshold_mw": threshold,
            "alert_calibration": "99th percentile of absolute 2025 residuals, model fitted on 2021-2024",
            "alert_rate": alert_rate,
            "update": update,
            "interpretation": f"Contemporaneous weather/calendar estimate, not a day-ahead forecast. {scope}",
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
