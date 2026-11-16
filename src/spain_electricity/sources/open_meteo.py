"""Open-Meteo hourly weather per city: historical archive and current conditions.

Without a ``timezone`` parameter the API answers in GMT, which is what the pipeline wants.
"""

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
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
CURRENT_SCHEMA = StructType(
    [
        StructField(
            "current",
            StructType(
                [
                    StructField("time", StringType()),
                    *[StructField(variable, DoubleType()) for variable in HOURLY_VARIABLES],
                ]
            ),
        ),
    ]
)


def read_current(spark: SparkSession, folder: Path) -> DataFrame:
    """Read multi-city captures, keeping the newest capture of each city's observation."""
    text = spark.read.option("wholetext", True).option("pathGlobFilter", "*.json").text(str(folder))
    text = text.withColumn("capture_name", F.col("_metadata.file_name"))
    payload = F.from_json("value", ArrayType(CURRENT_SCHEMA))
    cities = F.array(*[F.lit(city) for city in CITIES])
    rows = text.select("capture_name", F.posexplode(payload).alias("position", "weather"))
    rows = rows.select(
        "capture_name",
        F.element_at(cities, F.col("position") + 1).alias("city"),
        F.to_timestamp("weather.current.time", "yyyy-MM-dd'T'HH:mm").alias("ts_utc"),
        *[F.col(f"weather.current.{raw}").alias(column) for raw, column in VARIABLE_TO_COLUMN.items()],
    )
    rank = F.row_number().over(Window.partitionBy("city", "ts_utc").orderBy(F.desc("capture_name")))
    return rows.withColumn("rank", rank).where("rank = 1").drop("rank", "capture_name")


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


def jobs(start: date, end: date, paths: DataPaths, today: date | None = None) -> list[DownloadJob]:
    """Return one archive download job per city and month touched by ``start``..``end``.

    Each file holds its whole month (clipped only to ``today``), so a narrow first range never
    leaves a month file partial; the settle rule refreshes the current month until it is over.
    """
    today = today or date.today()
    result = []
    for city in CITIES.values():
        for month in month_starts(start, end):
            first, last = month, min(month_end(month), today)
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
