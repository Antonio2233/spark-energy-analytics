"""Small, deterministic seasonal downloads for the demand demonstration."""

import json
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spain_electricity.config import DataPaths
from spain_electricity.download import DownloadJob
from spain_electricity.sources import open_meteo, ree_forecast, ree_generation


def sample_days(days_per_season: int = 4, today: date | None = None) -> list[date]:
    """Select rotating weekdays in January, April, July and October of 2021-2026."""
    if not 1 <= days_per_season <= 7:
        raise ValueError("days_per_season must be between 1 and 7")
    today = today or date.today()
    return [
        day
        for year in range(2021, 2027)
        for season, month in enumerate((1, 4, 7, 10))
        for offset in range(days_per_season)
        for day in [date(year, month, 8) + timedelta(days=((year - 2021) * 3 + season * 2) % 7 + offset)]
        if day < today - timedelta(days=7)
    ]


def manifest_path(paths: DataPaths) -> Path:
    """Return where prepare_demand.py records the sampled days."""
    return paths.root / "demand_sample.json"


def sampled_history(paths: DataPaths, hourly: DataFrame) -> dict | None:
    """Return the sample manifest only if the hourly table holds no day outside the sample.

    The manifest stays behind after the full history is ingested, and must not then label a model
    trained on complete years as a sample. Each requested day also allows its two neighbours, which
    REE's 30-hour window spills into.
    """
    path = manifest_path(paths)
    if not path.exists():
        return None
    manifest = json.loads(path.read_text(encoding="utf-8"))
    allowed = {
        day + timedelta(days=offset)
        for day in map(date.fromisoformat, manifest["requested_days"])
        for offset in (-1, 0, 1)
    }
    outside = hourly.select("local_date").distinct().where(~F.col("local_date").isin(sorted(allowed)))
    return None if outside.limit(1).count() else manifest


def sample_jobs(paths: DataPaths, days_per_season: int = 4) -> list[DownloadJob]:
    """Fetch generation, evaluation baselines and the five-city weather; no gas or prices needed."""
    jobs = []
    for day in sample_days(days_per_season):
        jobs.extend(ree_generation.jobs(day, day, paths))
        if day.year >= 2025:
            jobs.extend(ree_forecast.jobs(day, day, paths))
        jobs.extend(open_meteo.jobs(day, day, paths))
    return list({job.destination: job for job in jobs}.values())
