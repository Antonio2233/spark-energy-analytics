"""KMeans day types from complete local-day demand profiles, with explicit DST handling."""

import json
import math

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import LOCAL_TIMEZONE, DataPaths
from spain_electricity.features import add_flags
from spain_electricity.models import SPLITS

# A group whose days are mostly weekends or national holidays is named a day off.
DAYS_OFF_SHARE = 0.5


def daily_profiles(hourly: DataFrame) -> DataFrame:
    """Normalize complete days to 24 local-hour values; interpolate only the DST spring gap."""
    valid = add_flags(hourly).where(~F.col("is_anomaly"))
    # Reject missing/nonfinite or nonpositive demand rather than manufacturing a daily curve.
    days = valid.groupBy("local_date").agg(
        F.count("*").alias("rows"),
        F.countDistinct("hour_utc").alias("hours"),
        F.min("demand_mw").alias("minimum"),
        F.sum(
            F.when(
                F.col("demand_mw").isNull() | F.isnan("demand_mw") | (F.abs("demand_mw") >= float("inf")), 1
            ).otherwise(0)
        ).alias("invalid"),
    )
    start = F.to_utc_timestamp(F.col("local_date").cast("timestamp"), LOCAL_TIMEZONE)
    end = F.to_utc_timestamp(F.date_add("local_date", 1).cast("timestamp"), LOCAL_TIMEZONE)
    days = days.withColumn("expected_hours", (end.cast("long") - start.cast("long")) / 3600)
    days = days.where(
        (F.col("hours") == F.col("expected_hours"))
        & (F.col("rows") == F.col("hours"))
        & (F.col("invalid") == 0)
        & (F.col("minimum") > 0)
    )
    # The repeated autumn hour is averaged before normalization, giving every wall-clock hour equal weight.
    values = (
        valid.join(days.select("local_date", "expected_hours"), "local_date")
        .groupBy("local_date", "expected_hours", "local_hour")
        .agg(F.avg("demand_mw").alias("mw"))
    )
    wide = (
        values.groupBy("local_date", "expected_hours").pivot("local_hour", list(range(24))).agg(F.first("mw"))
    )
    wide = wide.withColumn(
        "2",
        F.when(
            (F.col("expected_hours") == 23) & F.col("2").isNull(), (F.col("1") + F.col("3")) / 2
        ).otherwise(F.col("2")),
    )
    average = sum(F.col(str(hour)) for hour in range(24)) / 24
    profile = F.array(*[F.col(str(hour)) / average for hour in range(24)])
    return wide.dropna(subset=[str(hour) for hour in range(24)]).select(
        "local_date", array_to_vector(profile).alias("profile")
    )


def fit_day_types(
    profiles: DataFrame, k_values: tuple[int, ...] = (3, 4, 5, 6, 7, 8), seed: int = 42
) -> tuple[PipelineModel, dict]:
    """Select k by training-profile silhouette, skipping impossible or degenerate partitions."""
    n = profiles.count()
    scores, best, best_score = {}, None, float("-inf")
    for k in k_values:
        if not 2 <= k < n:
            continue
        kmeans = KMeans(k=k, seed=seed, featuresCol="profile", predictionCol="day_type", maxIter=30)
        model = Pipeline(stages=[kmeans]).fit(profiles)
        assignments = model.transform(profiles)
        clusters = assignments.select("day_type").distinct().count()
        if not 2 <= clusters < n:
            continue
        score = float(
            ClusteringEvaluator(featuresCol="profile", predictionCol="day_type").evaluate(assignments)
        )
        scores[str(k)] = score
        if score > best_score:
            best, best_score = model, score
    if best is None:
        raise ValueError("Need at least three complete days with distinct shapes for day types")
    return best, scores


def describe(assignments: DataFrame, hourly: DataFrame) -> DataFrame:
    """Describe clusters with equal weight per day, rather than overweighting 25-hour days."""
    daily = hourly.groupBy("local_date").agg(
        F.avg("temperature_c").alias("temperature_c"),
        F.avg("renewable_share").alias("renewable_share"),
        F.avg("demand_mw").alias("demand_mw"),
        F.max((F.col("is_weekend") | F.col("is_holiday")).cast("double")).alias("nonworking"),
    )
    joined = assignments.join(daily, "local_date")
    values = vector_to_array("profile")
    return joined.groupBy("day_type").agg(
        F.count("*").alias("days"),
        F.avg("temperature_c").alias("mean_temperature_c"),
        F.avg("renewable_share").alias("mean_renewable_share"),
        F.avg("demand_mw").alias("mean_demand_mw"),
        F.avg("nonworking").alias("weekend_or_holiday_share"),
        F.array(*[F.avg(values[hour]) for hour in range(24)]).alias("mean_profile"),
    )


def name_day_types(summary: list[dict]) -> dict[int, str]:
    """Name each group from its share of days off and its mean temperature: an inference, not a label.

    KMeans only sees the normalized shape. Groups that are mostly weekends or holidays are days off; the
    others are working days. Within each kind, the warmest group is "Warm" and the coolest "Cool" (a kind
    with one group gets no adjective; middle groups are "Mild", numbered when there are several).
    """
    names = {}
    for kind, is_day_off in (("working day", False), ("weekend or holiday", True)):
        rows = [row for row in summary if (row["weekend_or_holiday_share"] >= DAYS_OFF_SHARE) == is_day_off]
        # A group without temperature sorts as the coolest rather than failing.
        rows.sort(key=lambda row: row["mean_temperature_c"] or float("-inf"), reverse=True)
        middle = len(rows) - 2
        for position, row in enumerate(rows):
            if len(rows) == 1:
                name = kind.capitalize()
            elif position == 0:
                name = f"Warm {kind}"
            elif position == len(rows) - 1:
                name = f"Cool {kind}"
            else:
                name = f"Mild {kind}" + (f" {position}" if middle > 1 else "")
            names[row["day_type"]] = name
    return names


def train_day_types(
    spark: SparkSession, paths: DataPaths, k_values: tuple[int, ...] = (3, 4, 5, 6, 7, 8)
) -> dict:
    """Fit types on the training years only, then assign all complete days to those learned centers."""
    hourly = spark.read.parquet(str(paths.processed / "hourly_features"))
    profiles = daily_profiles(hourly).cache()
    try:
        first, last = SPLITS["train"]
        training = profiles.where(F.year("local_date").between(first, last))
        model, scores = fit_day_types(training, k_values)
        assignments = model.transform(profiles).cache()
        try:
            summary = describe(assignments, hourly).orderBy("day_type").toPandas().to_dict("records")
            for row in summary:
                row["mean_profile"] = row["mean_profile"].tolist()
                row["peak_local_hour"] = max(range(24), key=row["mean_profile"].__getitem__)
                for key, value in row.items():
                    if isinstance(value, float) and not math.isfinite(value):
                        row[key] = None
            names = name_day_types(summary)
            summary = [
                {"day_type": row["day_type"], "name": names[row["day_type"]], **row} for row in summary
            ]
            folder = paths.models / "day_types"
            model.write().overwrite().save(str(folder / "pipeline"))
            assignments.select("local_date", "day_type").write.mode("overwrite").parquet(
                str(folder / "assignments")
            )
            report = {
                "chosen_k": model.stages[-1].getK(),
                "silhouettes": scores,
                "training_days": training.count(),
                "assigned_days": assignments.count(),
                "summary": summary,
                "interpretation": (
                    f"Clusters describe normalized daily demand shapes, fitted on {first}-{last}. KMeans "
                    "numbers them arbitrarily; each name is inferred from the group's share of weekends "
                    "or holidays and its mean temperature. Incomplete days and the blackout are excluded."
                ),
            }
            partial = folder / "metrics.json.part"
            partial.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
            partial.replace(folder / "metrics.json")
            return report
        finally:
            assignments.unpersist()
    finally:
        profiles.unpersist()
