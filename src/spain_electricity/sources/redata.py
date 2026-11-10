"""REE REData API: actual, forecast and scheduled demand, and the market price.

Both widgets accept a whole month per request. Timestamps carry their UTC offset, so the
October repeated hour is unambiguous. The spot price is quarter-hourly in the responses; before
October 2025 the four quarters repeat the hourly price.
"""

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from spain_electricity.config import DataPaths
from spain_electricity.download import DownloadJob
from spain_electricity.periods import month_end, month_starts

BASE_URL = "https://apidatos.ree.es/es/datos"
DEMAND_WIDGET = "demanda/demanda-tiempo-real"
PRICE_WIDGET = "mercados/precios-mercados-tiempo-real"
DEMAND_SERIES = {
    "Real": "actual_demand_mw",
    "Prevista": "forecast_demand_mw",
    "Programada": "scheduled_demand_mw",
}
PRICE_SERIES = {"Precio mercado spot": "spot", "PVPC": "pvpc"}

PAYLOAD_SCHEMA = StructType(
    [
        StructField(
            "included",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType()),
                        StructField(
                            "attributes",
                            StructType(
                                [
                                    StructField("title", StringType()),
                                    StructField(
                                        "values",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField("value", DoubleType()),
                                                    StructField("percentage", DoubleType()),
                                                    StructField("datetime", StringType()),
                                                ]
                                            )
                                        ),
                                    ),
                                ]
                            ),
                        ),
                    ]
                )
            ),
        )
    ]
)


def _month_url(widget: str, month: date) -> str:
    first, last = date(month.year, month.month, 1), month_end(month)
    return (
        f"{BASE_URL}/{widget}?start_date={first.isoformat()}T00:00"
        f"&end_date={last.isoformat()}T23:59&time_trunc=hour"
    )


def build_demand_url(month: date) -> str:
    """Return the URL of one month of 5-minute actual, forecast and scheduled demand."""
    return _month_url(DEMAND_WIDGET, month)


def build_price_url(month: date) -> str:
    """Return the URL of one month of spot and PVPC prices."""
    return _month_url(PRICE_WIDGET, month)


def is_valid_payload(content: bytes) -> bool:
    """Return True for a data response, False for an ``errors`` body."""
    return b'"errors"' not in content and b'"included"' in content


def _jobs(folder: str, url_for, start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    return [
        DownloadJob(
            url_for(month),
            paths.raw / folder / f"{month:%Y}" / f"{folder}_{month:%Y-%m}.json",
            month_end(month),
            is_valid_payload,
        )
        for month in month_starts(start, end)
    ]


def demand_jobs(start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return one demand download job per month touched by the range."""
    return _jobs("redata_demand", build_demand_url, start, end, paths)


def price_jobs(start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return one price download job per month touched by the range."""
    return _jobs("redata_price", build_price_url, start, end, paths)


def read_series(spark: SparkSession, raw_root: Path) -> DataFrame:
    """Return every series value as ``series``, ``ts_utc`` and ``value``."""
    raw = (
        spark.read.schema(PAYLOAD_SCHEMA)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(str(raw_root))
    )
    series = raw.select(F.explode("included").alias("block")).select(
        F.col("block.attributes.title").alias("series"),
        F.explode("block.attributes.values").alias("point"),
    )
    return series.select(
        "series",
        F.to_timestamp("point.datetime", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ts_utc"),
        F.col("point.value").alias("value"),
    )


def _write(df: DataFrame, target: Path) -> None:
    (
        df.withColumn("year", F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(target))
    )


def ingest_demand(spark: SparkSession, paths: DataPaths) -> int:
    """Write ``processed/demand_forecast`` (one column per demand series); return its row count."""
    mapping = F.create_map(*[F.lit(item) for pair in DEMAND_SERIES.items() for item in pair])
    wide = (
        read_series(spark, paths.raw / "redata_demand")
        .where(F.col("series").isin(list(DEMAND_SERIES)))
        .withColumn("column", mapping[F.col("series")])
        .groupBy("ts_utc")
        .pivot("column", list(DEMAND_SERIES.values()))
        .agg(F.avg("value"))
    )
    target = paths.processed / "demand_forecast"
    _write(wide, target)
    return spark.read.parquet(str(target)).count()


def ingest_price(spark: SparkSession, paths: DataPaths) -> int:
    """Write ``processed/market_price`` (long: ``series`` spot or pvpc); return its row count."""
    mapping = F.create_map(*[F.lit(item) for pair in PRICE_SERIES.items() for item in pair])
    prices = (
        read_series(spark, paths.raw / "redata_price")
        .where(F.col("series").isin(list(PRICE_SERIES)))
        .select("ts_utc", mapping[F.col("series")].alias("series"), F.col("value").alias("price_eur_mwh"))
        .dropDuplicates(["ts_utc", "series"])
    )
    target = paths.processed / "market_price"
    _write(prices, target)
    return spark.read.parquet(str(target)).count()
