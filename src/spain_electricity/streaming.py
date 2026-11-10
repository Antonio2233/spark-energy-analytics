"""Structured Streaming over the landing folder: deduplicated 5-minute readings and 15-minute windows.

Captures repeat the whole day so far and replayed days overlap by about 6 hours, so readings are
deduplicated on ``ts_utc`` within a 2-hour watermark. Windows are written per micro-batch in update
mode; readers keep the row of the highest ``batch_id`` for each window.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from spain_electricity.capture import REE_LANDING
from spain_electricity.config import DataPaths
from spain_electricity.features import add_renewable_share
from spain_electricity.sources.ree_generation import parse_payload, with_utc

WATERMARK = "2 hours"


def read_generation_stream(spark: SparkSession, landing_dir) -> DataFrame:
    """Return a streaming DataFrame of unique 5-minute readings with the renewable share."""
    source = landing_dir / REE_LANDING
    source.mkdir(parents=True, exist_ok=True)
    text = (
        spark.readStream.option("wholetext", True)
        .option("maxFilesPerTrigger", 1)
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


def start_stream(
    spark: SparkSession, paths: DataPaths, *, trigger_seconds: int = 30, available_now: bool = False
) -> list[StreamingQuery]:
    """Start the ``readings`` and ``windows`` queries; each has its own checkpoint."""
    trigger = {"availableNow": True} if available_now else {"processingTime": f"{trigger_seconds} seconds"}
    readings = read_generation_stream(spark, paths.landing)
    readings_query = (
        readings.writeStream.queryName("readings")
        .format("parquet")
        .option("path", str(paths.stream_output / "readings"))
        .option("checkpointLocation", str(paths.checkpoints / "readings"))
        .outputMode("append")
        .trigger(**trigger)
        .start()
    )
    windows_query = (
        window_metrics(readings)
        .writeStream.queryName("windows")
        .outputMode("update")
        .foreachBatch(_windows_writer(str(paths.stream_output / "windows")))
        .option("checkpointLocation", str(paths.checkpoints / "windows"))
        .trigger(**trigger)
        .start()
    )
    return [readings_query, windows_query]
