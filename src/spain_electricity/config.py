"""Project-wide constants and the data folder layout."""

import os
from dataclasses import dataclass
from pathlib import Path

LOCAL_TIMEZONE = "Europe/Madrid"
GAS_PRODUCT = "GDAES_D+1"
DATA_DIR_ENV = "SPAIN_ELECTRICITY_DATA_DIR"
REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class City:
    """A weather location and its weight in the national average."""

    name: str
    latitude: float
    longitude: float
    weight: float


# Weights are approximate municipal populations in millions (INE order of magnitude);
# they only need to rank the cities sensibly for a national temperature average.
CITIES: dict[str, City] = {
    "madrid": City("madrid", 40.4168, -3.7038, 3.4),
    "barcelona": City("barcelona", 41.3874, 2.1686, 1.7),
    "valencia": City("valencia", 39.4699, -0.3763, 0.8),
    "seville": City("seville", 37.3891, -5.9845, 0.69),
    "bilbao": City("bilbao", 43.2630, -2.9350, 0.35),
}


@dataclass(frozen=True)
class DataPaths:
    """Folder layout under the data root (never versioned)."""

    root: Path

    @property
    def raw(self) -> Path:
        """Downloads exactly as received."""
        return self.root / "raw"

    @property
    def processed(self) -> Path:
        """Parquet datasets written by Spark."""
        return self.root / "processed"

    @property
    def landing(self) -> Path:
        """Real-time files read by the streaming job."""
        return self.root / "stream" / "landing"

    @property
    def checkpoints(self) -> Path:
        """Structured Streaming checkpoints, one folder per query."""
        return self.root / "stream" / "checkpoints"

    @property
    def stream_output(self) -> Path:
        """Results written by the streaming job."""
        return self.root / "stream" / "output"

    @property
    def models(self) -> Path:
        """Saved Spark ML pipeline models."""
        return self.root / "models"

    @classmethod
    def from_env(cls) -> "DataPaths":
        """Build the layout from the environment, defaulting to ``<repo>/data``."""
        configured = os.environ.get(DATA_DIR_ENV)
        return cls(Path(configured) if configured else REPO_ROOT / "data")
