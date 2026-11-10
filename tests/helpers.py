"""Test-only builders shared by unit and integration tests."""

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook

from spain_electricity.config import DataPaths

MIBGAS_HEADER = [
    "Trading day",
    "Product",
    "Place of delivery",
    "Area",
    "First Day Delivery",
    "Last Day Delivery",
    "Reference Price\n[EUR/MWh]",
    "Auction Price\n[EUR/MWh]",
    "Last Price\n[EUR/MWh]",
    "Bid\n[EUR/MWh]",
    "Ask\n[EUR/MWh]",
    "Source",
    "Maximum Price\n[EUR/MWh]",
    "Minimum Price\n[EUR/MWh]",
    "Price difference between purchases and sales\n[%]",
    "Auction Volume Traded\xa0[MWh]",
    "OTC Volume Registered [MWh]",
    "Volume Traded\n[MWh]",
]


def mibgas_trade(product: str, area: str, delivery: datetime, price: float) -> list:
    """Return one row of the MIBGAS trading sheet."""
    day = datetime(2025, 12, 31)
    return [day, product, "PVB", area, delivery, delivery, price, None, price, None, None, "M",
            price, price, 0.7, 0.0, 0.0, 1000.0]  # fmt: skip


def build_mibgas_workbook(path: Path) -> Path:
    """Write a small MIBGAS-like workbook: two Spanish products, one blank row, one Portuguese product."""
    workbook = Workbook()
    workbook.active.title = "Disclaimer"
    sheet = workbook.create_sheet("Trading Data PVB&VTP")
    sheet.append(MIBGAS_HEADER + [None, None])
    sheet.append(mibgas_trade("GDAES_D+1", "ES", datetime(2026, 1, 1), 28.59))
    sheet.append(mibgas_trade("GDAES_D+2", "ES", datetime(2026, 1, 2), 28.70))
    sheet.append([None] * 20)
    sheet.append(mibgas_trade("GDAPT_D+1", "PT", datetime(2026, 1, 1), 28.75))
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    return path


def copy_raw_fixtures(fixtures_dir: Path, paths: DataPaths) -> None:
    """Copy every raw fixture into ``paths.raw`` with the raw layout, plus a MIBGAS workbook."""
    for source in ("ree_generation", "redata_demand", "redata_price", "open_meteo"):
        shutil.copytree(fixtures_dir / source, paths.raw / source)
    build_mibgas_workbook(paths.raw / "mibgas" / "MIBGAS_Data_2025.xlsx")


@dataclass
class FakeResponse:
    """A minimal stand-in for ``requests.Response``."""

    status_code: int
    content: bytes


class FakeSession:
    """Returns queued responses (or raises queued exceptions) and records requested URLs.

    ``responses`` is a list consumed in order, or a callable ``url -> response`` for routing by URL.
    """

    def __init__(self, responses):
        self.responses = responses if callable(responses) else list(responses)
        self.urls = []

    def get(self, url, timeout):
        self.urls.append(url)
        item = self.responses(url) if callable(self.responses) else self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item
