"""Download a small seasonal demand sample (96 days by default), then optionally ingest it."""

import argparse
import json
import logging
from collections import Counter

from spain_electricity.config import DataPaths
from spain_electricity.download import download_all
from spain_electricity.pipeline import run_ingest
from spain_electricity.sampling import manifest_path, sample_days, sample_jobs
from spain_electricity.spark_session import get_spark, stop_spark


def main() -> int:
    """Download only the bounded sample and record exactly what was requested."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-per-season", type=int, default=4, choices=range(1, 8))
    parser.add_argument("--pause", type=float, default=0.5, help="seconds between requests")
    parser.add_argument("--download-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    paths = DataPaths.from_env()
    jobs = sample_jobs(paths, args.days_per_season)
    logging.info("Checking %d files; settled downloads are reused", len(jobs))
    report = download_all(jobs, pause_seconds=args.pause)
    paths.root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "strategy": "Rotating four-season blocks, not complete years; sampled 2026 evaluation only.",
        "days_per_season": args.days_per_season,
        "requested_days": [day.isoformat() for day in sample_days(args.days_per_season)],
        "days_by_year": dict(Counter(day.year for day in sample_days(args.days_per_season))),
        "requested_files": len(jobs),
        "fetched": report.fetched,
        "skipped": report.skipped,
        "failed": report.failed,
    }
    manifest_path(paths).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if report.failed:
        logging.error("%d downloads failed; rerun before training", len(report.failed))
        return 1
    if not args.download_only:
        try:
            logging.info("Ingested: %s", run_ingest(get_spark("demand-sample", "local[2]", 4), paths))
        finally:
            stop_spark()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
