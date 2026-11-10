"""Download raw history for a date range into data/raw (resumable; settled files are skipped).

Example:
    uv run python scripts/download_history.py --start 2025-01-01 --end 2025-01-31
"""

import argparse
import logging
import sys
from datetime import date

from spain_electricity.config import DataPaths
from spain_electricity.download import download_all
from spain_electricity.pipeline import SOURCES, download_jobs

logger = logging.getLogger("download_history")


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, run the downloads and return 1 if any job failed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="first day (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="last day (YYYY-MM-DD)")
    parser.add_argument("--sources", nargs="+", default=list(SOURCES), choices=SOURCES)
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between requests")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    jobs = download_jobs(args.sources, args.start, args.end, DataPaths.from_env())
    logger.info("%d files to check for %s .. %s", len(jobs), args.start, args.end)
    report = download_all(jobs, pause_seconds=args.pause)
    logger.info("fetched=%d skipped=%d failed=%d", report.fetched, report.skipped, len(report.failed))
    for url in report.failed:
        logger.error("failed: %s", url)
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
