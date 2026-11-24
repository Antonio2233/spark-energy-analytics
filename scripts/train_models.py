"""Train the remaining Phase 2 models with small chronological searches."""

import argparse
import logging

from spain_electricity.config import DataPaths
from spain_electricity.day_types import train_day_types
from spain_electricity.models import train_demand
from spain_electricity.regression_models import SPECS, refresh_demand_weights, train
from spain_electricity.spark_session import get_spark, stop_spark


def main() -> None:
    """Train selected models; preserve the bundled demand model unless explicitly requested."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--models",
        nargs="+",
        choices=["demand", "price", "renewable_share", "day_types"],
        default=["price", "renewable_share", "day_types"],
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    paths = DataPaths.from_env()
    try:
        spark = get_spark("phase2-training", "local[2]", 4)
        refresh_demand_weights(spark, paths)
        for spec in SPECS:
            if spec.name in args.models:
                report = train(spark, paths, spec)
                logging.info(
                    "%s: selected=%s test=%s rows=%s",
                    spec.name,
                    report["selected"],
                    report["test"],
                    report["rows"],
                )
        if "demand" in args.models:
            train_demand(spark, paths)
        if "day_types" in args.models:
            logging.info("Day types: %s", train_day_types(spark, paths))
    finally:
        stop_spark()


if __name__ == "__main__":
    main()
