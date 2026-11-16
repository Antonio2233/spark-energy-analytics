"""Structured Streaming over the landing folder: deduplicated 5-minute readings and 15-minute windows.

Captures repeat the whole day so far and replayed days overlap by about 6 hours, so readings are
deduplicated on ``ts_utc`` within a 2-hour watermark. Windows are written per micro-batch in update
mode; readers keep the row of the highest ``batch_id`` for each window.
"""

import logging

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from spain_electricity.capture import REE_LANDING, WEATHER_LANDING
from spain_electricity.config import CITIES, DataPaths
from spain_electricity.features import WEATHER_COLUMNS, add_renewable_share, hourly_mean, national_weather
from spain_electricity.models import load_metadata, predict_hours
from spain_electricity.sources.open_meteo import read_current
from spain_electricity.sources.ree_generation import parse_payload, with_utc
from spain_electricity.storage import has_parquet
from spain_electricity.views import stream_mode

logger = logging.getLogger(__name__)

WATERMARK = "2 hours"
# Files per micro-batch: enough to drain hours of live captures after a restart in a few batches.
MAX_FILES_PER_TRIGGER = 20


def read_generation_stream(
    spark: SparkSession, landing_dir, max_files_per_trigger: int = MAX_FILES_PER_TRIGGER
) -> DataFrame:
    """Return a streaming DataFrame of unique 5-minute readings with the renewable share."""
    source = landing_dir / REE_LANDING
    source.mkdir(parents=True, exist_ok=True)
    text = (
        spark.readStream.option("wholetext", True)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("pathGlobFilter", "*.jsonp")
        .text(str(source))
    )
    readings = with_utc(parse_payload(text)).withWatermark("ts_utc", WATERMARK)
    return add_renewable_share(readings.dropDuplicatesWithinWatermark(["ts_utc"]))


def window_metrics(readings: DataFrame, duration: str = "15 minutes") -> DataFrame:
    """Aggregate readings into fixed event-time windows."""
    return (
        readings.groupBy(F.window("ts_utc", duration).alias("window"))
        .agg(
            F.avg("demand_mw").alias("avg_demand_mw"),
            F.max("demand_mw").alias("max_demand_mw"),
            F.avg("renewable_share").alias("avg_renewable_share"),
            F.count("*").alias("n_readings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_demand_mw",
            "max_demand_mw",
            "avg_renewable_share",
            "n_readings",
        )
    )


def _windows_writer(target: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        batch.write.mode("overwrite").parquet(f"{target}/batch_id={batch_id}")

    return write


def monitoring_weather(spark: SparkSession, paths: DataPaths) -> DataFrame | None:
    """Use captured weather in live mode and historical weather during replay; never mix them."""
    if stream_mode(paths) == "live":
        folder = paths.landing / WEATHER_LANDING
        if not folder.exists() or not any(folder.glob("*.json")):
            return None
        weather = read_current(spark, folder)
    else:
        folder = paths.processed / "weather"
        if not has_parquet(folder):
            return None
        weather = spark.read.parquet(str(folder))
    # A national temperature needs all five cities at the same observation time.
    complete = (
        weather.where(F.col("temperature_c").isNotNull())
        .groupBy("ts_utc")
        .agg(F.countDistinct("city").alias("cities"))
        .where(F.col("cities") == len(CITIES))
        .select("ts_utc")
    )
    weather = weather.join(complete, "ts_utc", "inner")
    return hourly_mean(national_weather(weather), "ts_utc", WEATHER_COLUMNS).select(
        "hour_utc", "temperature_c"
    )


def _prediction_writer(paths: DataPaths, model: PipelineModel, threshold: float):
    def write(batch: DataFrame, batch_id: int) -> None:
        hours = batch.withColumnRenamed("window_start", "hour_utc")
        weather = monitoring_weather(batch.sparkSession, paths)
        if weather is None:
            hours = hours.withColumn("temperature_c", F.lit(None).cast("double"))
        else:
            hours = hours.join(weather, "hour_utc", "left")
        predicted = predict_hours(hours, model, threshold)
        predicted.write.mode("overwrite").parquet(
            str(paths.stream_output / "hourly" / f"batch_id={batch_id}")
        )

    return write


def start_stream(
    spark: SparkSession,
    paths: DataPaths,
    *,
    trigger_seconds: int = 30,
    available_now: bool = False,
    max_files_per_trigger: int = MAX_FILES_PER_TRIGGER,
    query_prefix: str = "",
) -> list[StreamingQuery]:
    """Start the ``readings`` and ``windows`` queries; each has its own checkpoint."""
    trigger = {"availableNow": True} if available_now else {"processingTime": f"{trigger_seconds} seconds"}
    readings = read_generation_stream(spark, paths.landing, max_files_per_trigger)
    readings_query = (
        readings.writeStream.queryName(f"{query_prefix}readings")
        .format("parquet")
        .option("path", str(paths.stream_output / "readings"))
        .option("checkpointLocation", str(paths.checkpoints / "readings"))
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )
    windows_query = (
        window_metrics(readings)
        .writeStream.queryName(f"{query_prefix}windows")
        .outputMode("update")
        .foreachBatch(_windows_writer(str(paths.stream_output / "windows")))
        .option("checkpointLocation", str(paths.checkpoints / "windows"))
        .trigger(**trigger)
        .start()
    )
    queries = [readings_query, windows_query]
    metadata = load_metadata(paths)
    if metadata is not None:
        try:
            model = PipelineModel.load(str(paths.models / "demand" / "pipeline"))
        except Exception:
            logger.exception("Demand model could not be loaded; continuing with readings and windows")
            return queries
        hourly_query = (
            window_metrics(readings, "1 hour")
            .writeStream.queryName(f"{query_prefix}hourly_demand")
            .outputMode("update")
            .foreachBatch(_prediction_writer(paths, model, metadata["alert_threshold_mw"]))
            .option("checkpointLocation", str(paths.checkpoints / "hourly_demand"))
            .trigger(**trigger)
            .start()
        )
        queries.append(hourly_query)
    else:
        logger.info("No demand model yet; run scripts/train_demand.py to enable hourly predictions")
    return queries
