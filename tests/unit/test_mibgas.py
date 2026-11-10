from datetime import date, datetime

from openpyxl import Workbook

from spain_electricity.sources import mibgas

HEADER = [
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


def trade(product, area, delivery, price):
    day = datetime(2025, 12, 31)
    return [day, product, "PVB", area, delivery, delivery, price, None, price, None, None, "M",
            price, price, 0.7, 0.0, 0.0, 1000.0]  # fmt: skip


def build_workbook(path):
    workbook = Workbook()
    workbook.active.title = "Disclaimer"
    sheet = workbook.create_sheet("Trading Data PVB&VTP")
    sheet.append(HEADER + [None, None])
    sheet.append(trade("GDAES_D+1", "ES", datetime(2026, 1, 1), 28.59))
    sheet.append(trade("GDAES_D+2", "ES", datetime(2026, 1, 2), 28.70))
    sheet.append([None] * 20)
    sheet.append(trade("GDAPT_D+1", "PT", datetime(2026, 1, 1), 28.75))
    workbook.save(path)
    return path


def test_build_url_and_jobs(paths):
    assert (
        mibgas.build_url(2025)
        == "https://www.mibgas.es/es/file-access/MIBGAS_Data_2025.xlsx?path=AGNO_2025/XLS"
    )
    jobs = mibgas.jobs([2024, 2025], paths)
    assert [job.destination.name for job in jobs] == ["MIBGAS_Data_2024.xlsx", "MIBGAS_Data_2025.xlsx"]
    assert jobs[1].covers_until == date(2025, 12, 31)
    assert jobs[1].validator(b"PK\x03\x04rest")
    assert not jobs[1].validator(b"<html>")


def test_xlsx_to_csv(tmp_path):
    csv_path = tmp_path / "out" / "mibgas_2025.csv"
    assert mibgas.xlsx_to_csv(build_workbook(tmp_path / "book.xlsx"), csv_path) == 3
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert lines[0].split(",")[:5] == ["2025-12-31", "GDAES_D+1", "PVB", "ES", "2026-01-01"]
    assert lines[0].split(",")[7] == ""


def test_read_gas_price_filters_product(spark, tmp_path):
    mibgas.xlsx_to_csv(build_workbook(tmp_path / "book.xlsx"), tmp_path / "csv" / "mibgas_2025.csv")
    rows = mibgas.read_gas_price(spark, tmp_path / "csv").collect()
    assert [(row.delivery_date, row.gas_price_eur_mwh) for row in rows] == [(date(2026, 1, 1), 28.59)]


def test_ingest(spark, paths):
    (paths.raw / "mibgas").mkdir(parents=True)
    build_workbook(paths.raw / "mibgas" / "MIBGAS_Data_2025.xlsx")
    assert mibgas.ingest(spark, paths) == 1
    assert (paths.raw / "mibgas" / "csv" / "MIBGAS_Data_2025.csv").exists()
