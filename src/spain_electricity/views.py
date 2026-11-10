"""Data for the UI: every function aggregates in Spark and returns a small pandas DataFrame.

A missing dataset gives an empty frame with the expected columns, so a fresh clone can render.
"""

import re
from collections.abc import Sequence
from datetime import date, datetime

import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from spain_electricity.capture import REE_LANDING
from spain_electricity.config import DataPaths
from spain_electricity.storage import has_parquet

DATASETS = ("generation", "demand_forecast", "market_price", "weather", "gas_price", "hourly_features")
TIME_COLUMN = {"gas_price": "delivery_date", "hourly_features": "hour_utc"}
HISTORY_COLUMNS = (
    "hour_utc",
    "demand_mw",
    "actual_demand_mw",
    "forecast_demand_mw",
    "renewable_share",
    "spot_price_eur_mwh",
    "pvpc_eur_mwh",
    "temperature_c",
)
MIX_TECHNOLOGIES = {
    "wind": "wind_mw",
    "solar_pv": "solar_pv_mw",
    "solar_thermal": "solar_thermal_mw",
    "hydro": "hydro_mw",
    "nuclear": "nuclear_mw",
    "combined_cycle": "combined_cycle_mw",
    "coal": "coal_mw",
    "cogeneration_other": "cogeneration_other_mw",
}
LIVE_READING_COLUMNS = ("ts_utc", "demand_mw", "renewable_share")
LIVE_WINDOW_COLUMNS = ("window_start", "avg_demand_mw", "max_demand_mw", "avg_renewable_share", "n_readings")
STALE_MINUTES = 20
# Live captures (and their catch-up backfills) are named after their UTC capture time.
LIVE_CAPTURE_PREFIX = re.compile(r"^\d{8}T\d{6}Z")


def _hourly(spark: SparkSession, paths: DataPaths):
    path = paths.processed / "hourly_features"
    return spark.read.parquet(str(path)) if has_parquet(path) else None


def data_status(spark: SparkSession, paths: DataPaths) -> pd.DataFrame:
    """Return rows and time range of every processed dataset (0 rows when missing)."""
    records = []
    for dataset in DATASETS:
        path = paths.processed / dataset
        if not has_parquet(path):
            records.append({"dataset": dataset, "rows": 0, "first": None, "last": None})
            continue
        column = TIME_COLUMN.get(dataset, "ts_utc")
        # toPandas (Arrow) keeps the session's UTC wall time; Row objects would use the machine's zone.
        summary = (
            spark.read.parquet(str(path))
            .agg(F.count("*").alias("rows"), F.min(column).alias("first"), F.max(column).alias("last"))
            .toPandas()
            .iloc[0]
        )
        records.append(
            {
                "dataset": dataset,
                "rows": int(summary["rows"]),
                "first": summary["first"],
                "last": summary["last"],
            }
        )
    status = pd.DataFrame(records, columns=["dataset", "rows", "first", "last"])
    # Dates (gas delivery days) and timestamps share these columns; make them one datetime type.
    for column in ("first", "last"):
        status[column] = pd.to_datetime(status[column])
    return status


def hourly_history(spark: SparkSession, paths: DataPaths, start: date, end: date) -> pd.DataFrame:
    """Return the hourly demand, forecast, renewable share, prices and temperature between two local dates."""
    hourly = _hourly(spark, paths)
    if hourly is None:
        return pd.DataFrame(columns=list(HISTORY_COLUMNS))
    selected = hourly.where(F.col("local_date").between(start, end)).select(*HISTORY_COLUMNS)
    return selected.orderBy("hour_utc").toPandas()


def mix_history(spark: SparkSession, paths: DataPaths, start: date, end: date) -> pd.DataFrame:
    """Return the hourly generation mix in long format (``hour_utc``, ``technology``, ``mw``)."""
    hourly = _hourly(spark, paths)
    if hourly is None:
        return pd.DataFrame(columns=["hour_utc", "technology", "mw"])
    pairs = ", ".join(f"'{name}', {column}" for name, column in MIX_TECHNOLOGIES.items())
    long = hourly.where(F.col("local_date").between(start, end)).selectExpr(
        "hour_utc", f"stack({len(MIX_TECHNOLOGIES)}, {pairs}) AS (technology, mw)"
    )
    return long.orderBy("hour_utc", "technology").toPandas()


def daily_demand_vs_temperature(spark: SparkSession, paths: DataPaths) -> pd.DataFrame:
    """Return mean demand and mean national temperature per local day."""
    hourly = _hourly(spark, paths)
    if hourly is None:
        return pd.DataFrame(columns=["local_date", "demand_mw", "temperature_c", "is_weekend"])
    daily = hourly.groupBy("local_date").agg(
        F.avg("demand_mw").alias("demand_mw"),
        F.avg("temperature_c").alias("temperature_c"),
        F.max("is_weekend").alias("is_weekend"),
    )
    return daily.orderBy("local_date").toPandas()


def correlation_matrix(
    spark: SparkSession, paths: DataPaths, columns: Sequence[str], method: str = "pearson"
) -> pd.DataFrame:
    """Return the correlation matrix of ``columns`` over the hourly table (rows with nulls skipped)."""
    hourly = _hourly(spark, paths)
    if hourly is None:
        return pd.DataFrame()
    vectors = VectorAssembler(inputCols=list(columns), outputCol="features", handleInvalid="skip").transform(
        hourly.select(*columns)
    )
    if vectors.limit(2).count() < 2:
        return pd.DataFrame()
    matrix = Correlation.corr(vectors, "features", method).head()[0].toArray()
    return pd.DataFrame(matrix, index=list(columns), columns=list(columns))


def live_readings(spark: SparkSession, paths: DataPaths, last_hours: int = 6) -> pd.DataFrame:
    """Return the streamed 5-minute readings of the last ``last_hours`` before the newest one."""
    path = paths.stream_output / "readings"
    if not has_parquet(path):
        return pd.DataFrame(columns=list(LIVE_READING_COLUMNS))
    readings = spark.read.parquet(str(path)).select(*LIVE_READING_COLUMNS)
    newest = readings.agg(F.max("ts_utc")).first()[0]
    if newest is None:
        return pd.DataFrame(columns=list(LIVE_READING_COLUMNS))
    recent = readings.where(F.col("ts_utc") > F.lit(newest) - F.expr(f"INTERVAL {int(last_hours)} HOURS"))
    return recent.orderBy("ts_utc").toPandas()


def live_windows(spark: SparkSession, paths: DataPaths) -> pd.DataFrame:
    """Return the latest state of every streamed window, newest first."""
    path = paths.stream_output / "windows"
    if not path.exists() or not any(path.glob("batch_id=*")):
        return pd.DataFrame(columns=list(LIVE_WINDOW_COLUMNS))
    rank = F.row_number().over(Window.partitionBy("window_start").orderBy(F.desc("batch_id")))
    latest = spark.read.parquet(str(path)).withColumn("rank", rank).where("rank = 1")
    return latest.select(*LIVE_WINDOW_COLUMNS).orderBy(F.desc("window_start")).toPandas()


def minutes_since(moment: pd.Timestamp | None, now: datetime) -> float | None:
    """Return the minutes between ``moment`` (naive means UTC) and ``now``, or None."""
    if moment is None or pd.isna(moment):
        return None
    moment = pd.Timestamp(moment)
    if moment.tzinfo is None:
        moment = moment.tz_localize("UTC")
    return (pd.Timestamp(now) - moment).total_seconds() / 60


def stream_mode(paths: DataPaths) -> str:
    """Return ``live``, ``replay`` or ``none`` from the newest file in the REE landing folder."""
    folder = paths.landing / REE_LANDING
    files = list(folder.glob("*.jsonp")) if folder.exists() else []
    if not files:
        return "none"
    newest = max(files, key=lambda path: path.stat().st_mtime)
    return "live" if LIVE_CAPTURE_PREFIX.match(newest.name) else "replay"


def stale_warning(
    paths: DataPaths, readings: pd.DataFrame, now: datetime, threshold_minutes: float = STALE_MINUTES
) -> str | None:
    """Return a warning when live readings are older than ``threshold_minutes``, else None.

    The age is the newest reading's event time against ``now``: a stalled feed keeps answering with
    the same readings, so the capture still lands files but no newer reading ever arrives. Replayed
    history is old by design and never warns.
    """
    if stream_mode(paths) != "live" or readings.empty:
        return None
    age = minutes_since(readings["ts_utc"].max(), now)
    if age is None or age <= threshold_minutes:
        return None
    return (
        f"No new readings for {age:.0f} minutes: the REE real-time feed or the capture may be down. "
        "Restart scripts/capture_realtime.py; it catches up missed days on start."
    )
