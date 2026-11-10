"""Shared pytest fixtures: one local SparkSession per test session, fixture paths and data paths."""

import os
import time
from pathlib import Path

# PySpark converts naive Python datetimes with the process time zone; run tests in UTC so that a
# naive datetime in a test means the same instant as the UTC Spark session displays.
os.environ["TZ"] = "UTC"
time.tzset()

import pytest  # noqa: E402

from spain_electricity.config import DataPaths  # noqa: E402
from spain_electricity.spark_session import get_spark  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """Return a small local SparkSession shared by every test."""
    session = get_spark("tests", "local[1]", 2)
    yield session
    session.stop()


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the folder holding trimmed real responses."""
    return FIXTURES_DIR


@pytest.fixture
def paths(tmp_path: Path) -> DataPaths:
    """Return data paths rooted in a temporary folder."""
    return DataPaths(tmp_path)
