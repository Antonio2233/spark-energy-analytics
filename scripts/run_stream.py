"""Run the Structured Streaming job over data/stream/landing.

Examples:
    uv run python scripts/run_stream.py                 # keeps running, a micro-batch every 30 s
    uv run python scripts/run_stream.py --available-now # processes what has landed and stops
"""

import argparse
import logging

from spain_electricity.config import DataPaths
from spain_electricity.spark_session import get_spark, stop_spark
from spain_electricity.streaming import MAX_FILES_PER_TRIGGER, start_stream

logger = logging.getLogger("run_stream")


def main(argv: list[str] | None = None) -> None:
    """Start the readings and windows queries and wait for them."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--trigger", type=int, default=30, help="seconds between micro-batches")
    parser.add_argument("--available-now", action="store_true", help="process pending files, then stop")
    parser.add_argument(
        "--max-files", type=int, default=MAX_FILES_PER_TRIGGER, help="landed files taken per micro-batch"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spark = get_spark("stream")
    queries = start_stream(
        spark,
        DataPaths.from_env(),
        trigger_seconds=args.trigger,
        available_now=args.available_now,
        max_files_per_trigger=args.max_files,
    )
    logger.info("Started queries: %s", ", ".join(query.name for query in queries))
    try:
        if args.available_now:
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping the streaming queries")
        for query in queries:
            query.stop()
    finally:
        stop_spark()


if __name__ == "__main__":
    main()
