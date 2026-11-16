"""REE's peninsular forecast: the baseline matching the generation feed's demand."""

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from spain_electricity.config import DataPaths
from spain_electricity.download import DownloadJob
from spain_electricity.sources.ree_generation import JSONP_PATTERN, with_utc

BASE_URL = "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/prevProgPeninsula"
SCHEMA = StructType(
    [
        StructField(
            "valoresPrevistaProgramada",
            ArrayType(
                StructType(
                    [
                        StructField("ts", StringType()),
                        StructField("pre", DoubleType()),
                    ]
                )
            ),
        ),
    ]
)


def build_url(day: date) -> str:
    """Return the same-day peninsular forecast URL."""
    return f"{BASE_URL}?callback=cb&curva=DEMANDAAU&fecha={day.isoformat()}"


def is_valid_payload(content: bytes) -> bool:
    """Reject empty or unrelated API responses."""
    return b'"valoresPrevistaProgramada"' in content and b'"pre"' in content


def jobs(start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return daily forecast downloads, resumable like the generation feed."""
    return [
        DownloadJob(
            build_url(day),
            paths.raw / "ree_forecast" / f"{day:%Y/%m}" / f"ree_forecast_{day}.jsonp",
            day,
            is_valid_payload,
        )
        for offset in range((end - start).days + 1)
        for day in [start + timedelta(days=offset)]
    ]


def read_raw(spark: SparkSession, raw_root: Path) -> DataFrame:
    """Read each local day only from its own file, including DST labels."""
    text = (
        spark.read.option("wholetext", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.jsonp")
        .text(str(raw_root))
        .withColumn("file_day", F.regexp_extract(F.col("_metadata.file_name"), r"(\d{4}-\d{2}-\d{2})", 1))
    )
    payload = F.from_json(F.regexp_extract("value", JSONP_PATTERN, 1), SCHEMA)
    readings = text.select("file_day", F.explode(payload["valoresPrevistaProgramada"]).alias("r"))
    readings = (
        readings.select(
            "file_day", F.col("r.ts").alias("ts_label"), F.col("r.pre").alias("peninsular_forecast_mw")
        )
        .where(F.substring("ts_label", 1, 10) == F.col("file_day"))
        .drop("file_day")
    )
    return with_utc(readings).dropDuplicates(["ts_utc"])


def ingest(spark: SparkSession, paths: DataPaths) -> int:
    """Write the 5-minute baseline without confusing it with REData's national forecast."""
    target = paths.processed / "peninsular_forecast"
    (
        read_raw(spark, paths.raw / "ree_forecast")
        .withColumn("year", F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(target))
    )
    return spark.read.parquet(str(target)).count()
