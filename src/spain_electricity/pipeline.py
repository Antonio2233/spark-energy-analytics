"""Batch orchestration used by the command-line scripts: which files to download and what to ingest."""

import logging
from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession

from spain_electricity.config import DataPaths
from spain_electricity.download import DownloadJob
from spain_electricity.features import write_hourly_features
from spain_electricity.sources import mibgas, open_meteo, redata, ree_generation
from spain_electricity.storage import has_files

logger = logging.getLogger(__name__)

SOURCES = ("ree_generation", "redata_demand", "redata_price", "open_meteo", "mibgas")


def download_jobs(sources: Sequence[str], start: date, end: date, paths: DataPaths) -> list[DownloadJob]:
    """Return the download jobs of the selected sources for ``start``..``end``."""
    builders = {
        "ree_generation": lambda: ree_generation.jobs(start, end, paths),
        "redata_demand": lambda: redata.demand_jobs(start, end, paths),
        "redata_price": lambda: redata.price_jobs(start, end, paths),
        "open_meteo": lambda: open_meteo.jobs(start, end, paths),
        "mibgas": lambda: mibgas.jobs(range(start.year, end.year + 1), paths),
    }
    unknown = [source for source in sources if source not in builders]
    if unknown:
        raise ValueError(f"unknown source(s): {', '.join(unknown)}; choose from {', '.join(SOURCES)}")
    return [job for source in sources for job in builders[source]()]


def run_ingest(spark: SparkSession, paths: DataPaths) -> dict[str, int]:
    """Ingest every source that has raw data, then the hourly table; return row counts by dataset."""
    steps = [
        ("generation", "ree_generation", "*.jsonp", lambda: ree_generation.ingest(spark, paths)),
        ("demand_forecast", "redata_demand", "*.json", lambda: redata.ingest_demand(spark, paths)),
        ("market_price", "redata_price", "*.json", lambda: redata.ingest_price(spark, paths)),
        ("weather", "open_meteo", "*.json", lambda: open_meteo.ingest(spark, paths)),
        ("gas_price", "mibgas", "*.xlsx", lambda: mibgas.ingest(spark, paths)),
    ]
    counts: dict[str, int] = {}
    for dataset, raw_folder, pattern, ingest in steps:
        if not has_files(paths.raw / raw_folder, pattern):
            logger.info("Skipping %s: no %s files in %s", dataset, pattern, paths.raw / raw_folder)
            continue
        counts[dataset] = ingest()
        logger.info("%s: %d rows", dataset, counts[dataset])
    if "generation" in counts:
        counts["hourly_features"] = write_hourly_features(spark, paths)
        logger.info("hourly_features: %d rows", counts["hourly_features"])
    return counts
