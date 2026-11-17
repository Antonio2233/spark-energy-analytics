"""What counts as data being present in a folder (an empty folder or a bare _SUCCESS is not)."""

from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def latest_per_key(frame: DataFrame, key: str) -> DataFrame:
    """Select the row from the newest committed batch for each window or hour."""
    rank = F.row_number().over(Window.partitionBy(key).orderBy(F.desc("batch_id")))
    return frame.withColumn("_batch_rank", rank).where(F.col("_batch_rank") == 1).drop("_batch_rank")


def has_files(folder: Path, pattern: str) -> bool:
    """Return True when ``folder`` holds at least one file matching ``pattern`` (recursively)."""
    return folder.exists() and any(path.is_file() for path in folder.rglob(pattern))


def has_parquet(folder: Path) -> bool:
    """Return True when ``folder`` is a dataset with at least one Parquet part."""
    return has_files(folder, "*.parquet")


def completed_batches(folder: Path) -> list[str]:
    """Return committed foreachBatch folders, excluding an in-progress first write."""
    return [
        str(path)
        for path in sorted(folder.glob("batch_id=*"))
        if (path / "_SUCCESS").exists() and has_parquet(path)
    ]
