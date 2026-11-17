"""Fetch one month's electricity prices and one year's gas file for the Energy Desk."""

import argparse
import logging
from datetime import date

from spain_electricity.config import DataPaths
from spain_electricity.download import download_all
from spain_electricity.pipeline import run_ingest
from spain_electricity.sources import mibgas, redata, ree_forecast
from spain_electricity.spark_session import get_spark, stop_spark


def main() -> int:
    """Enrich the existing sample without downloading another demand history or retraining."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default=date.today().strftime("%Y-%m"), help="delivery month, YYYY-MM")
    parser.add_argument(
        "--forecast-day", type=date.fromisoformat, help="optional extra peninsular forecast day"
    )
    args = parser.parse_args()
    month = date.fromisoformat(f"{args.month}-01")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    paths = DataPaths.from_env()
    jobs = [*redata.price_jobs(month, month, paths), *mibgas.jobs([month.year], paths)]
    if args.forecast_day:
        jobs.extend(ree_forecast.jobs(args.forecast_day, args.forecast_day, paths))
    report = download_all(jobs)
    if report.failed:
        logging.error("Market downloads failed: %s", report.failed)
        return 1
    try:
        logging.info("Ingested: %s", run_ingest(get_spark("desk-market", "local[2]", 4), paths))
    finally:
        stop_spark()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
