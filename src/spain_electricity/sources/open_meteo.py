"""Open-Meteo hourly weather per city: historical archive and current conditions.

Without a ``timezone`` parameter the API answers in GMT, which is what the pipeline wants.
"""

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from spain_electricity.config import CITIES, City, DataPaths
from spain_electricity.download import DownloadJob
from spain_electricity.periods import month_end, month_starts

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = ("temperature_2m", "shortwave_radiation", "wind_speed_100m")
VARIABLE_TO_COLUMN = {
    "temperature_2m": "temperature_c",
    "shortwave_radiation": "shortwave_radiation_wm2",
    "wind_speed_100m": "wind_speed_100m_kmh",
}
# The reanalysis archive lags a few days behind real time; refresh files until a week has passed.
ARCHIVE_SETTLE_DAYS = 7

ARCHIVE_SCHEMA = StructType(
    [
        StructField(
            "hourly",
            StructType(
                [StructField("time", ArrayType(StringType()))]
                + [StructField(variable, ArrayType(DoubleType())) for variable in HOURLY_VARIABLES]
            ),
        )
    ]
)


def build_archive_url(city: City, start: date, end: date) -> str:
    """Return the archive URL of one city's hourly weather between two dates."""
    return (
        f"{ARCHIVE_URL}?latitude={city.latitude}&longitude={city.longitude}"
        f"&start_date={start.isoformat()}&end_date={end.isoformat()}&hourly={','.join(HOURLY_VARIABLES)}"
    )


def build_current_url(cities: Iterable[City]) -> str:
    """Return the URL of the current conditions of several cities in one request."""
    cities = list(cities)
    latitudes = ",".join(str(city.latitude) for city in cities)
    longitudes = ",".join(str(city.longitude) for city in cities)
    return f"{FORECAST_URL}?latitude={latitudes}&longitude={longitudes}&current={','.join(HOURLY_VARIABLES)}"


def raw_path(paths: DataPaths, city: City, month: date) -> Path:
    """Return where one city's month of archive weather is stored."""
    return paths.raw / "open_meteo" / city.name / f"open_meteo_{city.name}_{month:%Y-%m}.json"


def jobs(start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return one archive download job per city and month, clipped to ``start``..``end``."""
    result = []
    for city in CITIES.values():
        for month in month_starts(start, end):
            first, last = max(month, start), min(month_end(month), end)
            result.append(
                DownloadJob(
                    build_archive_url(city, first, last),
                    raw_path(paths, city, month),
                    last,
                    lambda content: b'"hourly"' in content,
                    ARCHIVE_SETTLE_DAYS,
                )
            )
    return result


def read_archive(spark: SparkSession, raw_root: Path) -> DataFrame:
    """Return one row per city and hour with temperature, radiation and wind speed."""
    raw = (
        spark.read.schema(ARCHIVE_SCHEMA)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(str(raw_root))
        .withColumn(
            "city", F.regexp_extract(F.col("_metadata.file_name"), r"^open_meteo_(.+)_\d{4}-\d{2}\.json$", 1)
        )
    )
    hours = raw.select("city", "hourly", F.posexplode("hourly.time").alias("position", "time"))
    return hours.select(
        "city",
        F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm").alias("ts_utc"),
        *[
            F.element_at(F.col(f"hourly.{variable}"), F.col("position") + 1).alias(column)
            for variable, column in VARIABLE_TO_COLUMN.items()
        ],
    ).dropDuplicates(["city", "ts_utc"])


def ingest(spark: SparkSession, paths: DataPaths) -> int:
    """Write ``processed/weather`` partitioned by year and month; return its row count."""
    target = paths.processed / "weather"
    (
        read_archive(spark, paths.raw / "open_meteo")
        .withColumn("year", F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(target))
    )
    return spark.read.parquet(str(target)).count()
