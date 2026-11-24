"""Price and renewable-share pipelines, searched, refit, evaluated and updated like the demand model."""

import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, SQLTransformer, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import ANOMALY_DATES, DataPaths
from spain_electricity.features import add_phase2_features
from spain_electricity.models import (
    GRIDS,
    MIN_UPDATE_HOURS,
    SPLITS,
    UPDATE_CUTOFF,
    clear_update,
    estimate_column,
    feature_weights,
    update_period,
    update_split,
)


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
    # Products of a numeric input and a flag, so the flag can change that input's effect.
    interactions: tuple[tuple[str, str], ...] = ()

    @property
    def baselines(self) -> dict[str, str]:
        """Name the persistence baseline the way the demand report names its naive one."""
        return {"same_hour_yesterday": self.baseline}


PRICE = ModelSpec(
    "price",
    "spot_price_eur_mwh",
    ("gas_price_eur_mwh", "renewable_share", "demand_mw"),
    ("local_hour",),
    ("iberian_exception",),
    "spot_price_eur_mwh_24h_ago",
    "EUR/MWh",
    (("gas_price_eur_mwh", "iberian_exception"),),
)
RENEWABLES = ModelSpec(
    "renewable_share",
    "renewable_share",
    ("wind_speed_100m_kmh", "shortwave_radiation_wm2", "demand_mw"),
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


def paired(df: DataFrame, spec: ModelSpec) -> DataFrame:
    """Keep hours with a finite baseline whose previous day is not an anomaly, as the demand model does."""
    excluded = [date.fromisoformat(day) for day in ANOMALY_DATES]
    return complete_rows(df, (spec.baseline,)).where(~F.date_sub("local_date", 1).isin(excluded))


def build_pipeline(spec: ModelSpec, family: str) -> Pipeline:
    """Fit encoding and scaling inside the training-only pipeline, with default estimator parameters."""
    indices = [f"{name}_index" for name in spec.categorical]
    encoded = [f"{name}_encoded" for name in spec.categorical]
    flag_columns = [f"{name}_number" for name in spec.flags]
    interaction_columns = [f"{value}_x_{flag}" for value, flag in spec.interactions]
    derived = [f"CAST({name} AS DOUBLE) AS {name}_number" for name in spec.flags] + [
        f"{value} * CAST({flag} AS DOUBLE) AS {value}_x_{flag}" for value, flag in spec.interactions
    ]
    stages = (
        [SQLTransformer(statement="SELECT *, " + ", ".join(derived) + " FROM __THIS__")] if derived else []
    )
    stages += [
        StringIndexer(inputCol=name, outputCol=index, handleInvalid="keep")
        for name, index in zip(spec.categorical, indices, strict=True)
    ]
    stages += [
        OneHotEncoder(inputCols=indices, outputCols=encoded, handleInvalid="keep"),
        VectorAssembler(
            inputCols=[*spec.numeric, *flag_columns, *interaction_columns, *encoded], outputCol="features_raw"
        ),
    ]
    if family == "linear":
        stages += [
            StandardScaler(inputCol="features_raw", outputCol="features", withMean=False),
            LinearRegression(labelCol=spec.label, maxIter=100),
        ]
    elif family == "random_forest":
        stages += [
            RandomForestRegressor(
                labelCol=spec.label, featuresCol="features_raw", minInstancesPerNode=2, seed=42
            )
        ]
    elif family == "gbt":
        stages += [
            GBTRegressor(labelCol=spec.label, featuresCol="features_raw", minInstancesPerNode=2, seed=42)
        ]
    else:
        raise ValueError(f"Unknown model family: {family}")
    return Pipeline(stages=stages)


def candidate_grid(spec: ModelSpec, grids: dict[str, dict[str, list]] | None = None) -> list[tuple]:
    """Return, per family, its pipeline and the ``ParamGridBuilder`` maps to loop by hand (demand's grid)."""
    candidates = []
    for family, values in (grids or GRIDS).items():
        pipeline = build_pipeline(spec, family)
        estimator = pipeline.getStages()[-1]
        builder = ParamGridBuilder()
        for name, options in values.items():
            builder.addGrid(estimator.getParam(name), options)
        candidates.append((family, pipeline, builder.build()))
    return candidates


def evaluate(df: DataFrame, label: str, prediction: str) -> dict:
    """Measure MAE, RMSE, R² and bias (actual minus estimate) on a preselected paired set of hours."""
    values = {
        metric: float(
            RegressionEvaluator(labelCol=label, predictionCol=prediction, metricName=metric).evaluate(df)
        )
        for metric in ("mae", "rmse", "r2")
    }
    values["bias"] = float(df.select(F.avg(F.col(label) - F.col(prediction))).first()[0])
    return {key: value if math.isfinite(value) else None for key, value in values.items()}


def _predictions(scored: DataFrame, spec: ModelSpec, split: str, *extra: str) -> DataFrame:
    """Keep the inputs, target, baseline and estimate, so the saved model can be checked offline."""
    inputs = dict.fromkeys(
        ["hour_utc", "local_date", spec.label, spec.baseline, *spec.numeric, *spec.categorical, *spec.flags]
    )
    return scored.select(
        *inputs,
        F.col("prediction").alias(estimate_column(spec.label)),
        *extra,
        F.lit(split).alias("split"),
    )


def _scores(scored: DataFrame, spec: ModelSpec, hours: int) -> dict:
    """Score the model and the persistence baseline on the same paired hours."""
    return {
        "model": evaluate(scored, spec.label, "prediction"),
        "same_hour_yesterday": evaluate(scored, spec.label, spec.baseline),
        "hours": hours,
    }


def _train_update(
    spec: ModelSpec,
    pipeline: Pipeline,
    params: dict,
    final: PipelineModel,
    frames: dict[str, DataFrame],
    target: Path,
    cutoff: date,
) -> dict | None:
    """Refit the selected configuration through the day before ``cutoff`` and score it beside ``final``."""
    hours = frames["update"].count()
    if hours < MIN_UPDATE_HOURS:
        clear_update(target / "update")
        return None
    updated = pipeline.fit(frames["update_train"], params)
    through_2025 = estimate_column(spec.label, previous=True)
    previous = final.transform(frames["update"]).select("hour_utc", F.col("prediction").alias(through_2025))
    scored = updated.transform(frames["update"]).join(previous, "hour_utc").cache()
    try:
        scores = _scores(scored, spec, hours)
        result = {
            **update_period(frames["update_train"].count(), frames["update"], cutoff),
            "evaluation": {
                "model": scores["model"],
                "model_through_2025": evaluate(scored, spec.label, through_2025),
                "same_hour_yesterday": scores["same_hour_yesterday"],
                "hours": hours,
            },
            "features": feature_weights(updated, scored, "features_raw"),
        }
        updated.write().overwrite().save(str(target / "update" / "pipeline"))
        _predictions(scored, spec, "update", through_2025).write.mode("overwrite").parquet(
            str(target / "update" / "predictions")
        )
        return result
    finally:
        scored.unpersist()


def train(
    spark: SparkSession,
    paths: DataPaths,
    spec: ModelSpec,
    grids: dict[str, dict[str, list]] | None = None,
    update_cutoff: date = UPDATE_CUTOFF,
) -> dict:
    """Choose on 2025, refit on every valid 2021-2025 hour, evaluate once on paired 2026 hours, then update.

    The update refits the same configuration on every valid hour before ``update_cutoff`` and scores it,
    beside the 2021-2025 model, on the paired hours from that date.
    """
    started = time.perf_counter()
    hourly = add_phase2_features(spark.read.parquet(str(paths.processed / "hourly_features")))
    if hourly.groupBy("hour_utc").count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate hourly keys; fix ingestion before training")
    required = (spec.label, *spec.numeric, *spec.categorical, *spec.flags)
    valid = complete_rows(hourly.where(~F.col("is_anomaly")), required)
    years = {
        name: valid.where(F.year("local_date").between(first, last)) for name, (first, last) in SPLITS.items()
    }
    update_train, update_hours = update_split(valid, update_cutoff)
    # Training and the refits use every complete row; comparisons need model and baseline on identical hours.
    frames = {
        "train": years["train"],
        "refit": years["train"].unionByName(years["validation"]),
        "validation": paired(years["validation"], spec),
        "test": paired(years["test"], spec),
        "update_train": update_train,
        "update": paired(update_hours, spec),
    }
    frames = {key: value.cache() for key, value in frames.items()}
    try:
        counts = {key: frames[key].count() for key in ("train", "validation", "test")}
        if counts["train"] < 24 or min(counts["validation"], counts["test"]) < 24:
            raise ValueError(
                f"Insufficient complete paired hours for {spec.name}: {counts}. Prepare market history first."
            )
        candidates, best, best_mae = [], None, float("inf")
        for family, pipeline, param_maps in candidate_grid(spec, grids):
            for params in param_maps:
                fitted = pipeline.fit(frames["train"], params)
                metrics = evaluate(fitted.transform(frames["validation"]), spec.label, "prediction")
                record = {
                    "model": family,
                    "parameters": {key.name: value for key, value in params.items()},
                    **metrics,
                }
                candidates.append(record)
                if metrics["mae"] is not None and metrics["mae"] < best_mae:
                    best_mae, best = metrics["mae"], (record, pipeline, params, fitted)
        if best is None:
            raise ValueError("No finite validation candidate")
        selected, pipeline, params, fitted = best
        final = pipeline.fit(frames["refit"], params)
        target = paths.models / spec.name
        # Validation estimates come from the 2021-2024 fit that was selected; test ones from the refit.
        validation = fitted.transform(frames["validation"]).cache()
        test = final.transform(frames["test"]).cache()
        try:
            evaluation = {
                "validation": _scores(validation, spec, counts["validation"]),
                "test": _scores(test, spec, counts["test"]),
            }
            weights = feature_weights(final, test, "features_raw")
            final.write().overwrite().save(str(target / "pipeline"))
            saved = _predictions(validation, spec, "validation").unionByName(_predictions(test, spec, "test"))
            saved.write.mode("overwrite").parquet(str(target / "predictions"))
        finally:
            validation.unpersist()
            test.unpersist()
        update = _train_update(spec, pipeline, params, final, frames, target, update_cutoff)
        report = {
            "label": spec.label,
            "unit": spec.unit,
            "baselines": spec.baselines,
            "selected": {key: selected[key] for key in ("model", "parameters")},
            "candidates": candidates,
            "evaluation": evaluation,
            "features": weights,
            "split_hours": counts,
            "refit": {"years": "2021-2025", "hours": frames["refit"].count()},
            "update": update,
            "weight_kind": "standardized coefficient" if selected["model"] == "linear" else "tree importance",
            "trained_at": datetime.now(UTC).isoformat(),
            "training_seconds": round(time.perf_counter() - started, 2),
            "interpretation": (
                "Contemporaneous explanatory estimate, not a day-ahead forecast. Baseline is exact UTC t-24h "
                "persistence. Selected on 2025, refit on 2021-2025 and evaluated once on 2026."
            ),
        }
        partial = target / "metrics.json.part"
        partial.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
        partial.replace(target / "metrics.json")
        return report
    finally:
        for frame in frames.values():
            frame.unpersist()
