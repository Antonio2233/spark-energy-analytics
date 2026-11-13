"""What counts as data being present in a folder (an empty folder or a bare _SUCCESS is not)."""

from pathlib import Path


def has_files(folder: Path, pattern: str) -> bool:
    """Return True when ``folder`` holds at least one file matching ``pattern`` (recursively)."""
    return folder.exists() and any(path.is_file() for path in folder.rglob(pattern))


def has_parquet(folder: Path) -> bool:
    """Return True when ``folder`` is a dataset with at least one Parquet part."""
    return has_files(folder, "*.parquet")
