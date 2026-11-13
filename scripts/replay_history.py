"""Replay downloaded REE days into data/stream/landing as if they were arriving live.

Example:
    uv run python scripts/replay_history.py --start 2025-01-01 --end 2025-01-07 --delay 5
"""

import argparse
import logging
import sys
from datetime import date, timedelta

from spain_electricity.capture import REE_LANDING, replay_files
from spain_electricity.config import DataPaths
from spain_electricity.sources.ree_generation import raw_path

logger = logging.getLogger("replay_history")


def main(argv: list[str] | None = None) -> int:
    """Copy the raw day files of the range into the landing folder, one every ``--delay`` seconds."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="first day (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="last day (YYYY-MM-DD)")
    parser.add_argument("--delay", type=float, default=5.0, help="seconds between days")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    paths = DataPaths.from_env()
    days = [args.start + timedelta(days=offset) for offset in range((args.end - args.start).days + 1)]
    files = [raw_path(paths, day) for day in days if raw_path(paths, day).exists()]
    if not files:
        logger.error(
            "No raw REE files for %s .. %s: run scripts/download_history.py first", args.start, args.end
        )
        return 1
    replay_files(files, paths.landing / REE_LANDING, args.delay)
    return 0


if __name__ == "__main__":
    sys.exit(main())
