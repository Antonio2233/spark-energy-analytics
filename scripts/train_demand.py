"""Train and compare small Spark demand models with chronological holdouts."""

import logging

from spain_electricity.config import DataPaths
from spain_electricity.models import train_demand
from spain_electricity.spark_session import get_spark, stop_spark


def main() -> None:
    """Train on the ingested data and report the measured results."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        result = train_demand(get_spark("demand-training", "local[2]", 4), DataPaths.from_env())
        logging.info(
            "Selected %s; held-out results: %s; %.1f seconds",
            result["selected"],
            result["evaluation"],
            result["training_seconds"],
        )
    finally:
        stop_spark()


if __name__ == "__main__":
    main()
