"""Ingest everything in data/raw into Parquet datasets and rebuild the hourly feature table.

Example:
    uv run python scripts/ingest.py
"""

import argparse
import logging
import sys

from spain_electricity.config import DataPaths
from spain_electricity.pipeline import run_ingest
from spain_electricity.spark_session import get_spark, stop_spark

logger = logging.getLogger("ingest")


def main(argv: list[str] | None = None) -> int:
    """Run every ingestion step and log the row count of each dataset."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("ingest")
    try:
        counts = run_ingest(spark, DataPaths.from_env())
    finally:
        stop_spark()
    if not counts:
        logger.warning("Nothing ingested: data/raw is empty. Run scripts/download_history.py first.")
        return 1
    for dataset, rows in counts.items():
        logger.info("%-16s %9d rows", dataset, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
