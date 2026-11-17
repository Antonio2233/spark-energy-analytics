"""Timestamp-aligned energy-desk views; all joins and indicators are computed in Spark."""

from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spain_electricity import models, views
from spain_electricity.capture import FORECAST_LANDING, PRICE_LANDING, WEATHER_LANDING
from spain_electricity.config import LOCAL_TIMEZONE, DataPaths
from spain_electricity.features import (
    WEATHER_COLUMNS,
    add_renewable_share,
    complete_weather,
    hourly_mean,
    national_weather,
)
from spain_electricity.sources import open_meteo, redata, ree_forecast
from spain_electricity.sources.ree_generation import RAW_TO_COLUMN
from spain_electricity.storage import completed_batches, has_parquet, latest_per_key

GENERATION = tuple(RAW_TO_COLUMN.values())
MIX = dict(views.MIX_TECHNOLOGIES)


@dataclass
class DeskSnapshot:
    """Bounded chart data and latest aligned values, never a mixture of live and replay contexts."""

    mode: str
    asof: pd.Timestamp | None
    timeline: pd.DataFrame
    hourly: pd.DataFrame
    mix: pd.DataFrame
    prices: pd.DataFrame
    cities: pd.DataFrame
    sources: pd.DataFrame
    summary: dict
    monitoring: pd.DataFrame = field(default_factory=pd.DataFrame)


def _optional(spark: SparkSession, path) -> DataFrame | None:
    return spark.read.parquet(str(path)) if has_parquet(path) else None


def price_context(spark: SparkSession, paths: DataPaths, mode: str) -> DataFrame | None:
    """Return hourly delivery prices; live mode only uses captured daily snapshots."""
    if mode == "live":
        folder = paths.landing / PRICE_LANDING
        if not folder.exists() or not any(folder.glob("*.json")):
            return None
        prices = redata.read_series(spark, folder)
        mapping = F.create_map(*[F.lit(item) for pair in redata.PRICE_SERIES.items() for item in pair])
        prices = (
            prices.where(F.col("series").isin(list(redata.PRICE_SERIES)))
            .select("ts_utc", mapping[F.col("series")].alias("series"), F.col("value").alias("price_eur_mwh"))
            .dropDuplicates(["ts_utc", "series"])
        )
    else:
        prices = _optional(spark, paths.processed / "market_price")
    if prices is None:
        return None
    return (
        prices.groupBy(F.date_trunc("hour", "ts_utc").alias("hour_utc"))
        .pivot("series", ["spot", "pvpc"])
        .agg(F.avg("price_eur_mwh"))
        .withColumnRenamed("spot", "spot_price_eur_mwh")
        .withColumnRenamed("pvpc", "pvpc_eur_mwh")
    )


def weather_context(spark: SparkSession, paths: DataPaths, mode: str) -> DataFrame | None:
    """Return weather observations from the selected mode, without silently falling back."""
    if mode == "live":
        folder = paths.landing / WEATHER_LANDING
        if not folder.exists() or not any(folder.glob("*.json")):
            return None
        return open_meteo.read_current(spark, folder)
    return _optional(spark, paths.processed / "weather")


def forecast_context(spark: SparkSession, paths: DataPaths, mode: str) -> DataFrame | None:
    """Return the peninsular baseline only, never REData's national demand forecast."""
    if mode == "live":
        folder = paths.landing / FORECAST_LANDING
        forecast = (
            ree_forecast.read_raw(spark, folder) if folder.exists() and any(folder.glob("*.jsonp")) else None
        )
    else:
        forecast = _optional(spark, paths.processed / "peninsular_forecast")
    return hourly_mean(forecast, "ts_utc", ["peninsular_forecast_mw"]) if forecast is not None else None


def _join_optional(frame: DataFrame, context: DataFrame | None, key: str, columns: list[str]) -> DataFrame:
    if context is not None:
        return frame.join(context.select(key, *columns), key, "left")
    for column in columns:
        frame = frame.withColumn(column, F.lit(None).cast("double"))
    return frame


def gas_context(spark: SparkSession, paths: DataPaths, mode: str) -> DataFrame | None:
    """Prefer a committed UI live reference; shared daily gas is still matched by delivery day."""
    if mode == "live":
        versions = sorted((paths.stream_root / "reference" / "gas").glob("asof=*"), reverse=True)
        for version in versions:
            if (version / "_SUCCESS").exists() and has_parquet(version):
                return spark.read.parquet(str(version))
    return _optional(spark, paths.processed / "gas_price")


def snapshot(
    spark: SparkSession,
    paths: DataPaths,
    last_hours: int = 24,
    efficiency: float = 0.55,
    historical_day: date | None = None,
) -> DeskSnapshot:
    """Align stream, weather, prices, daily gas and model by event time, with bounded UI results."""
    if last_hours not in (6, 12, 24, 48) or not 0 < efficiency <= 1:
        raise ValueError("Choose 6, 12, 24 or 48 hours and an efficiency between 0 and 1")
    mode = "historical" if historical_day is not None else views.stream_mode(paths)
    readings = _optional(
        spark, paths.processed / "generation" if historical_day else paths.stream_output / "readings"
    )
    empty = DeskSnapshot(mode, None, *[pd.DataFrame() for _ in range(6)], {})
    if readings is None:
        return empty
    selected = readings
    if historical_day is not None:
        selected = readings.where(
            F.to_date(F.from_utc_timestamp("ts_utc", LOCAL_TIMEZONE)) == F.lit(historical_day)
        )
    asof = selected.agg(F.max("ts_utc").alias("asof")).toPandas().iloc[0]["asof"]
    if pd.isna(asof):
        return empty
    asof = pd.Timestamp(asof)
    anchor = F.to_timestamp(F.lit(asof.strftime("%Y-%m-%d %H:%M:%S")))
    readings = readings.where(
        (F.col("ts_utc") > anchor - F.expr(f"INTERVAL {last_hours} HOURS")) & (F.col("ts_utc") <= anchor)
    )
    for column in GENERATION:
        if column not in readings.columns:
            readings = readings.withColumn(column, F.lit(None).cast("double"))
    if "renewable_share" not in readings.columns:
        readings = add_renewable_share(readings)
    timeline = readings.select("ts_utc", *GENERATION, "renewable_share").withColumn(
        "hour_utc", F.date_trunc("hour", "ts_utc")
    )
    prices = price_context(spark, paths, mode)
    timeline = _join_optional(timeline, prices, "hour_utc", ["spot_price_eur_mwh", "pvpc_eur_mwh"])
    timeline = _join_optional(
        timeline, forecast_context(spark, paths, mode), "hour_utc", ["peninsular_forecast_mw"]
    )
    weather = weather_context(spark, paths, mode)
    national = None
    if weather is not None:
        national = hourly_mean(national_weather(complete_weather(weather)), "ts_utc", WEATHER_COLUMNS)
    timeline = _join_optional(timeline, national, "hour_utc", list(WEATHER_COLUMNS))
    timeline = timeline.withColumn("local_date", F.to_date(F.from_utc_timestamp("ts_utc", LOCAL_TIMEZONE)))
    gas = gas_context(spark, paths, mode)
    if gas is not None:
        gas = gas.select(F.col("delivery_date").alias("local_date"), "gas_price_eur_mwh")
    timeline = _join_optional(timeline, gas, "local_date", ["gas_price_eur_mwh"])
    model_path = paths.stream_output / "hourly"
    estimates = None
    batches = completed_batches(model_path)
    if mode == "historical":
        estimates = _optional(spark, paths.models / "demand" / "predictions")
        if estimates is not None:
            metadata = models.load_metadata(paths)
            threshold = metadata["alert_threshold_mw"] if metadata else None
            estimates = estimates.select("hour_utc", "expected_demand_mw").withColumn(
                "alert_threshold_mw", F.lit(threshold).cast("double")
            )
    elif batches:
        estimates = latest_per_key(
            spark.read.option("basePath", str(model_path)).parquet(*batches), "hour_utc"
        )
    timeline = _join_optional(timeline, estimates, "hour_utc", ["expected_demand_mw", "alert_threshold_mw"])
    timeline = (
        timeline.withColumn("renewable_mw", F.col("demand_mw") * F.col("renewable_share"))
        .withColumn("residual_demand_mw", F.col("demand_mw") - F.col("renewable_mw"))
        .withColumn("combined_cycle_share", F.try_divide("combined_cycle_mw", "demand_mw"))
        .withColumn("fuel_cost_eur_mwh", F.col("gas_price_eur_mwh") / efficiency)
        .withColumn("fuel_spread_eur_mwh", F.col("spot_price_eur_mwh") - F.col("fuel_cost_eur_mwh"))
    )
    # Exact one-hour comparisons: gaps do not become fabricated hourly changes.
    previous_hour = Window.orderBy(F.col("ts_utc").cast("long")).rangeBetween(-3600, -3600)
    for column in ("demand_mw", "renewable_share", "spot_price_eur_mwh", "combined_cycle_share"):
        timeline = timeline.withColumn(
            f"{column}_change_1h", F.col(column) - F.first(column).over(previous_hour)
        )
    timeline = timeline.cache()
    try:
        latest = timeline.orderBy(F.desc("ts_utc")).limit(1).toPandas().iloc[0].to_dict()
        stats = (
            timeline.agg(
                F.max("demand_mw").alias("peak_demand_mw"),
                F.min("demand_mw").alias("min_demand_mw"),
                F.count("*").alias("readings"),
            )
            .toPandas()
            .iloc[0]
            .to_dict()
        )
        latest.update(stats)
        numeric = [
            *GENERATION,
            "renewable_share",
            "residual_demand_mw",
            "combined_cycle_share",
            "spot_price_eur_mwh",
            "pvpc_eur_mwh",
            "gas_price_eur_mwh",
            "fuel_cost_eur_mwh",
            "fuel_spread_eur_mwh",
            *WEATHER_COLUMNS,
            "peninsular_forecast_mw",
            "expected_demand_mw",
            "alert_threshold_mw",
        ]
        hourly = timeline.groupBy("hour_utc").agg(
            *[F.avg(column).alias(column) for column in numeric], F.count("*").alias("n_readings")
        )
        time_frame = timeline.orderBy("ts_utc").limit(576).toPandas()
        hour_frame = hourly.orderBy("hour_utc").limit(49).toPandas()
        monitoring = pd.DataFrame()
        if mode == "historical":
            monitor = (
                hourly.withColumn("avg_demand_mw", F.col("demand_mw"))
                .withColumn("demand_error_mw", F.col("demand_mw") - F.col("expected_demand_mw"))
                .withColumn(
                    "prediction_status",
                    F.when(F.col("expected_demand_mw").isNull(), "unavailable")
                    .when(F.col("n_readings") < 12, "provisional")
                    .otherwise("complete"),
                )
                .withColumn(
                    "demand_alert",
                    F.coalesce(
                        (F.col("prediction_status") == "complete")
                        & (F.abs("demand_error_mw") > F.col("alert_threshold_mw")),
                        F.lit(False),
                    ),
                )
            )
            monitoring = monitor.orderBy(F.desc("hour_utc")).limit(49).toPandas()
        pairs = ", ".join(f"'{key}', {column}" for key, column in MIX.items())
        mix = (
            timeline.orderBy(F.desc("ts_utc"))
            .limit(1)
            .selectExpr(f"stack({len(MIX)}, {pairs}) as (technology, mw)")
            .where("mw is not null and mw >= 0")
            .withColumn("generation_fraction", F.try_divide("mw", F.sum("mw").over(Window.partitionBy())))
            .toPandas()
        )
    finally:
        timeline.unpersist()
    day = asof.tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date()
    price_frame = pd.DataFrame()
    if prices is not None:
        price_frame = prices.where(F.to_date(F.from_utc_timestamp("hour_utc", LOCAL_TIMEZONE)) == F.lit(day))
        price_frame = price_frame.orderBy("hour_utc").limit(25).toPandas()
    city_frame = pd.DataFrame()
    if weather is not None:
        hour_anchor = F.date_trunc("hour", anchor)
        city_frame = (
            weather.where(F.date_trunc("hour", "ts_utc") == hour_anchor)
            .groupBy("city")
            .agg(*[F.avg(column).alias(column) for column in WEATHER_COLUMNS])
            .orderBy("city")
            .toPandas()
        )
    source_rows = []
    for name, column, delivery in [
        ("REE generation", "demand_mw", str(asof)),
        ("REE peninsular forecast", "peninsular_forecast_mw", str(asof.floor("h"))),
        ("Open-Meteo · five cities", "temperature_c", str(asof.floor("h"))),
        ("REE day-ahead prices", "spot_price_eur_mwh", str(asof.floor("h"))),
        ("MIBGAS day-ahead gas", "gas_price_eur_mwh", str(day)),
        ("Spark ML estimate", "expected_demand_mw", str(asof.floor("h"))),
    ]:
        present = pd.notna(latest.get(column))
        source_rows.append(
            {
                "layer": name,
                "status": "Matched" if present else "No matching period",
                "observation_or_delivery": delivery if present else "—",
            }
        )
    return DeskSnapshot(
        mode,
        asof,
        time_frame,
        hour_frame,
        mix,
        price_frame,
        city_frame,
        pd.DataFrame(source_rows),
        latest,
        monitoring,
    )
