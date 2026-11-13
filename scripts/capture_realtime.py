"""Capture REE and Open-Meteo real-time data into data/stream/landing every few minutes.

On start it first catches up any days missed since the last capture.

Example:
    uv run python scripts/capture_realtime.py --interval 300
"""

import argparse
import logging

from spain_electricity.capture import run_capture
from spain_electricity.config import DataPaths


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and capture until interrupted with Ctrl+C."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--interval", type=int, default=300, help="seconds between captures")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run_capture(DataPaths.from_env(), args.interval)
    except KeyboardInterrupt:
        logging.getLogger("capture_realtime").info("Capture stopped by the user")


if __name__ == "__main__":
    main()
