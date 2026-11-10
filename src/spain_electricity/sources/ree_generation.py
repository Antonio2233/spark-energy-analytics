"""REE peninsular demand and generation mix every 5 minutes (``demanda.ree.es``).

Each daily response is JSONP and covers about 30 hours (21:00 of the previous day to the early
hours of the next one). Labels are Madrid wall time; the repeated October hour is labeled
``2A`` (first pass, UTC+2) and ``2B`` (second pass, UTC+1), and the skipped March hour is absent.
"""

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from spain_electricity.config import LOCAL_TIMEZONE, DataPaths
from spain_electricity.download import DownloadJob

BASE_URL = "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/demandaGeneracionPeninsula"
RAW_FOLDER = "ree_generation"
PROCESSED_FOLDER = "generation"
JSONP_PATTERN = r"(?s)^\s*\w+\((.*)\)\s*;?\s*$"

RAW_TO_COLUMN: dict[str, str] = {
    "dem": "demand_mw",
    "eol": "wind_mw",
    "sol": "solar_mw",
    "solFot": "solar_pv_mw",
    "solTer": "solar_thermal_mw",
    "hid": "hydro_mw",
    "nuc": "nuclear_mw",
    "cc": "combined_cycle_mw",
    "car": "coal_mw",
    "cogenResto": "cogeneration_other_mw",
    "inter": "net_interchange_mw",
}

PAYLOAD_SCHEMA = StructType(
    [
        StructField(
            "valoresHorariosGeneracion",
            ArrayType(
                StructType(
                    [StructField("ts", StringType())] + [StructField(raw, DoubleType()) for raw in RAW_TO_COLUMN]
                )
            ),
        )
    ]
)


def build_url(day: date) -> str:
    """Return the URL of one day's 5-minute demand and generation."""
    return f"{BASE_URL}?callback=cb&curva=DEMANDAAU&fecha={day.isoformat()}"


def raw_path(paths: DataPaths, day: date) -> Path:
    """Return where one day's raw response is stored."""
    return paths.raw / RAW_FOLDER / f"{day:%Y}" / f"{day:%m}" / f"{RAW_FOLDER}_{day.isoformat()}.jsonp"


def is_valid_payload(content: bytes) -> bool:
    """Return True when the response looks like the expected JSONP."""
    return content.lstrip().startswith(b"cb(")


def jobs(start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return one download job per day between ``start`` and ``end`` (inclusive)."""
    days = (start + timedelta(days=offset) for offset in range((end - start).days + 1))
    return [DownloadJob(build_url(day), raw_path(paths, day), day, is_valid_payload) for day in days]


def parse_payload(text_df: DataFrame, text_col: str = "value") -> DataFrame:
    """Explode JSONP documents into one row per 5-minute reading (works on streaming frames)."""
    other_columns = [name for name in text_df.columns if name != text_col]
    payload = F.from_json(F.regexp_extract(F.col(text_col), JSONP_PATTERN, 1), PAYLOAD_SCHEMA)
    readings = text_df.select(*other_columns, F.explode(payload["valoresHorariosGeneracion"]).alias("reading"))
    return readings.select(
        *other_columns,
        F.col("reading.ts").alias("ts_label"),
        *[F.col(f"reading.{raw}").alias(column) for raw, column in RAW_TO_COLUMN.items()],
    )


def with_utc(df: DataFrame, label_col: str = "ts_label") -> DataFrame:
    """Replace the Madrid wall-time label with a UTC timestamp ``ts_utc``."""
    label = F.col(label_col)
    hour = F.substring(label, 12, 2)
    first_pass, second_pass = hour == F.lit("2A"), hour == F.lit("2B")
    wall_hour = F.when(first_pass | second_pass, F.lit("02")).otherwise(hour)
    wall = F.to_timestamp(
        F.concat(F.substring(label, 1, 10), F.lit(" "), wall_hour, F.lit(":"), F.substring(label, 15, 2)),
        "yyyy-MM-dd HH:mm",
    )
    ts_utc: Column = (
        F.when(first_pass, wall - F.expr("INTERVAL 2 HOURS"))
        .when(second_pass, wall - F.expr("INTERVAL 1 HOUR"))
        .otherwise(F.to_utc_timestamp(wall, LOCAL_TIMEZONE))
    )
    return df.withColumn("ts_utc", ts_utc).drop(label_col)


def read_raw(spark: SparkSession, raw_root: Path) -> DataFrame:
    """Read daily raw files, keeping each reading only from the file of its own local day."""
    text = (
        spark.read.option("wholetext", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.jsonp")
        .text(str(raw_root))
        .withColumn("file_day", F.regexp_extract(F.col("_metadata.file_name"), r"(\d{4}-\d{2}-\d{2})", 1))
    )
    readings = parse_payload(text).where(F.substring("ts_label", 1, 10) == F.col("file_day")).drop("file_day")
    return with_utc(readings).dropDuplicates(["ts_utc"])


def ingest(spark: SparkSession, paths: DataPaths) -> int:
    """Write ``processed/generation`` partitioned by year and month; return its row count."""
    target = paths.processed / PROCESSED_FOLDER
    (
        read_raw(spark, paths.raw / RAW_FOLDER)
        .withColumn("year", F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(target))
    )
    return spark.read.parquet(str(target)).count()
