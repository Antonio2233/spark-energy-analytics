"""Shared pytest fixtures: one local SparkSession per test session, fixture paths and data paths."""

from pathlib import Path

import pytest

from spain_electricity.config import DataPaths
from spain_electricity.spark_session import get_spark

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
