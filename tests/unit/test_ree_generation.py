import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from spain_electricity.sources import ree_generation


def test_build_url():
    assert ree_generation.build_url(date(2025, 1, 15)) == (
        "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/demandaGeneracionPeninsula"
        "?callback=cb&curva=DEMANDAAU&fecha=2025-01-15"
    )


def test_jobs_layout(paths):
    jobs = ree_generation.jobs(date(2025, 1, 1), date(2025, 1, 3), paths)
    assert len(jobs) == 3
    assert jobs[1].destination == paths.raw / "ree_generation" / "2025" / "01" / "ree_generation_2025-01-02.jsonp"
    assert jobs[1].covers_until == date(2025, 1, 2)
    assert jobs[1].validator(b'cb({"valoresHorariosGeneracion":[]});')
    assert not jobs[1].validator(b"<html>error</html>")


@pytest.mark.parametrize(
    "label, expected",
    [
        ("2025-01-15 05:20", "2025-01-15 04:20:00"),
        ("2025-10-26 01:55", "2025-10-25 23:55:00"),
        ("2025-10-26 2A:15", "2025-10-26 00:15:00"),
        ("2025-10-26 2B:15", "2025-10-26 01:15:00"),
        ("2025-10-26 03:00", "2025-10-26 02:00:00"),
        ("2025-03-30 01:55", "2025-03-30 00:55:00"),
        ("2025-03-30 03:00", "2025-03-30 01:00:00"),
    ],
)
def test_with_utc(spark, label, expected):
    df = spark.createDataFrame([(label,)], ["ts_label"])
    row = ree_generation.with_utc(df).select(F.date_format("ts_utc", "yyyy-MM-dd HH:mm:ss").alias("t")).first()
    assert row.t == expected
    assert "ts_label" not in ree_generation.with_utc(df).columns


def test_parse_payload_strips_jsonp(spark, fixtures_dir):
    path = fixtures_dir / "ree_generation" / "2025" / "01" / "ree_generation_2025-01-15.jsonp"
    text = spark.read.option("wholetext", True).text(str(path))
    parsed = ree_generation.parse_payload(text)
    assert parsed.count() == 361
    assert {"ts_label", "demand_mw", "wind_mw", "solar_mw", "hydro_mw"} <= set(parsed.columns)
    first = parsed.orderBy("ts_label").first()
    assert first.ts_label == "2025-01-14 21:00"
    assert first.demand_mw > 0


def test_read_raw_owner_day_removes_overlap(spark, fixtures_dir):
    df = ree_generation.read_raw(spark, fixtures_dir / "ree_generation" / "2025" / "01")
    assert df.count() == 576
    assert df.select("ts_utc").distinct().count() == 576


@pytest.mark.parametrize("month, expected", [("10", 300), ("03", 276)])
def test_read_raw_dst_days(spark, fixtures_dir, month, expected):
    df = ree_generation.read_raw(spark, fixtures_dir / "ree_generation" / "2025" / month)
    assert df.count() == expected
    assert df.select("ts_utc").distinct().count() == expected


def test_read_raw_ignores_partial_files(spark, fixtures_dir, tmp_path):
    source = fixtures_dir / "ree_generation" / "2025" / "01" / "ree_generation_2025-01-15.jsonp"
    shutil.copy(source, tmp_path / "ree_generation_2025-01-15.jsonp")
    shutil.copy(source, tmp_path / "ree_generation_2025-01-16.jsonp.part")
    assert ree_generation.read_raw(spark, tmp_path).count() == 288


def test_ingest_writes_partitioned_parquet(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "ree_generation" / "2025" / "01", paths.raw / "ree_generation" / "2025" / "01")
    assert ree_generation.ingest(spark, paths) == 576
    written = spark.read.parquet(str(paths.processed / "generation"))
    assert {"year", "month", "ts_utc", "demand_mw"} <= set(written.columns)
    assert written.count() == 576
