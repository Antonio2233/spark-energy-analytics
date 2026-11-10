"""MIBGAS daily natural gas price (Iberian market), from the free yearly Excel files.

Spark cannot read XLSX, so each workbook's ``Trading Data PVB&VTP`` sheet is converted to a
header-less CSV first (format conversion only, no filtering); Spark does the rest. The price used
is the reference price of the Spanish day-ahead product, assigned to its first delivery day.
"""

import csv
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from spain_electricity.config import GAS_PRODUCT, DataPaths
from spain_electricity.download import DownloadJob

TRADING_SHEET = "Trading Data PVB&VTP"
CSV_COLUMNS = (
    "trading_day",
    "product",
    "place_of_delivery",
    "area",
    "first_day_delivery",
    "last_day_delivery",
    "reference_price_eur_mwh",
    "auction_price_eur_mwh",
    "last_price_eur_mwh",
    "bid_eur_mwh",
    "ask_eur_mwh",
    "source",
    "max_price_eur_mwh",
    "min_price_eur_mwh",
    "price_spread_pct",
    "auction_volume_mwh",
    "otc_volume_mwh",
    "volume_traded_mwh",
)
TEXT_COLUMNS = {"product", "place_of_delivery", "area", "source"}
DATE_COLUMNS = {"trading_day", "first_day_delivery", "last_day_delivery"}
CSV_SCHEMA = StructType(
    [
        StructField(
            name,
            StringType() if name in TEXT_COLUMNS else DateType() if name in DATE_COLUMNS else DoubleType(),
        )
        for name in CSV_COLUMNS
    ]
)


def build_url(year: int) -> str:
    """Return the URL of one year's MIBGAS workbook."""
    return f"https://www.mibgas.es/es/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS"


def jobs(years: Iterable[int], paths: DataPaths) -> list[DownloadJob]:
    """Return one download job per year."""
    return [
        DownloadJob(
            build_url(year),
            paths.raw / "mibgas" / f"MIBGAS_Data_{year}.xlsx",
            date(year, 12, 31),
            lambda content: content[:2] == b"PK",
        )
        for year in years
    ]


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value


def xlsx_to_csv(xlsx_path: Path, csv_path: Path, sheet: str = TRADING_SHEET, n_columns: int = 18) -> int:
    """Write the sheet's data rows (no header, first ``n_columns``) as CSV; return rows written."""
    workbook = load_workbook(xlsx_path, read_only=True, data_only=True)
    try:
        rows = workbook[sheet].iter_rows(min_row=2, max_col=n_columns, values_only=True)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            for row in rows:
                if all(value is None for value in row):
                    continue
                writer.writerow([_csv_value(value) for value in row])
                written += 1
    finally:
        workbook.close()
    return written


def read_gas_price(spark: SparkSession, csv_root: Path, product: str = GAS_PRODUCT) -> DataFrame:
    """Return the reference price of ``product`` per delivery day."""
    trades = spark.read.schema(CSV_SCHEMA).option("dateFormat", "yyyy-MM-dd").csv(str(csv_root))
    return (
        trades.where(F.col("product") == product)
        .select(
            F.col("first_day_delivery").alias("delivery_date"),
            F.col("reference_price_eur_mwh").alias("gas_price_eur_mwh"),
        )
        .dropDuplicates(["delivery_date"])
        .orderBy("delivery_date")
    )


def ingest(spark: SparkSession, paths: DataPaths) -> int:
    """Convert every raw workbook to CSV and write ``processed/gas_price``; return its row count."""
    raw_dir, csv_dir = paths.raw / "mibgas", paths.raw / "mibgas" / "csv"
    for workbook in sorted(raw_dir.glob("*.xlsx")):
        xlsx_to_csv(workbook, csv_dir / f"{workbook.stem}.csv")
    target = paths.processed / "gas_price"
    read_gas_price(spark, csv_dir).write.mode("overwrite").parquet(str(target))
    return spark.read.parquet(str(target)).count()
