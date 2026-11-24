"""Add only the market months and annual gas references needed by the compact sample."""

import logging
from datetime import date

from spain_electricity.config import DataPaths
from spain_electricity.download import download_all
from spain_electricity.pipeline import run_ingest
from spain_electricity.sampling import sample_days
from spain_electricity.sources import mibgas, redata
from spain_electricity.spark_session import get_spark, stop_spark


def main() -> int:
    """Reuse existing demand/weather downloads; fetch at most 24 price months and six gas files."""
    logging.basicConfig(level=logging.INFO)
    paths = DataPaths.from_env()
    days = sample_days()
    months = sorted({date(day.year, day.month, 1) for day in days})
    jobs = [job for month in months for job in redata.price_jobs(month, month, paths)]
    jobs += mibgas.jobs(sorted({day.year for day in days}), paths)
    result = download_all(jobs)
    if result.failed:
        logging.error("Some market downloads failed: %s", result.failed)
        return 1
    try:
        logging.info("Ingested: %s", run_ingest(get_spark("phase2-preparation", "local[2]", 4), paths))
    finally:
        stop_spark()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
