import shutil
from datetime import date

from pyspark.sql import functions as F

from spain_electricity.sources import redata


def test_build_urls():
    assert redata.build_demand_url(date(2025, 1, 1)) == (
        "https://apidatos.ree.es/es/datos/demanda/demanda-tiempo-real"
        "?start_date=2025-01-01T00:00&end_date=2025-01-31T23:59&time_trunc=hour"
    )
    assert redata.build_price_url(date(2024, 2, 10)) == (
        "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real"
        "?start_date=2024-02-01T00:00&end_date=2024-02-29T23:59&time_trunc=hour"
    )


def test_jobs_one_per_month(paths):
    jobs = redata.demand_jobs(date(2024, 12, 15), date(2025, 2, 3), paths)
    assert [job.covers_until for job in jobs] == [date(2024, 12, 31), date(2025, 1, 31), date(2025, 2, 28)]
    assert jobs[1].destination == paths.raw / "redata_demand" / "2025" / "redata_demand_2025-01.json"
    price = redata.price_jobs(date(2025, 1, 1), date(2025, 1, 31), paths)
    assert price[0].destination == paths.raw / "redata_price" / "2025" / "redata_price_2025-01.json"


def test_validator_rejects_error_body(paths):
    validator = redata.demand_jobs(date(2025, 1, 1), date(2025, 1, 1), paths)[0].validator
    assert not validator(b'{"errors":[{"status":"400","title":"Error Interno"}]}')
    assert validator(b'{"data":{},"included":[]}')


def test_offsets_to_utc(spark, fixtures_dir):
    series = redata.read_series(spark, fixtures_dir / "redata_demand")
    counts = {row.series: row["count"] for row in series.groupBy("series").count().collect()}
    assert counts == {"Real": 300, "Prevista": 300, "Programada": 300}
    first = series.agg(F.date_format(F.min("ts_utc"), "yyyy-MM-dd HH:mm:ss").alias("t")).first().t
    assert first == "2025-10-25 22:00:00"


def test_demand_is_wide(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "redata_demand", paths.raw / "redata_demand")
    assert redata.ingest_demand(spark, paths) == 300
    written = spark.read.parquet(str(paths.processed / "demand_forecast"))
    assert {"ts_utc", "actual_demand_mw", "forecast_demand_mw", "scheduled_demand_mw"} <= set(written.columns)


def test_price_series(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "redata_price", paths.raw / "redata_price")
    assert redata.ingest_price(spark, paths) == 240
    written = spark.read.parquet(str(paths.processed / "market_price"))
    counts = {
        (row.year, row.series): row["count"]
        for row in written.groupBy(F.year("ts_utc").alias("year"), "series").count().collect()
    }
    assert counts == {(2025, "spot"): 96, (2025, "pvpc"): 24, (2026, "spot"): 96, (2026, "pvpc"): 24}
