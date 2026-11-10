import shutil
from datetime import date

from pyspark.sql import functions as F

from spain_electricity.config import CITIES
from spain_electricity.sources import open_meteo


def test_archive_url_has_no_timezone():
    url = open_meteo.build_archive_url(CITIES["madrid"], date(2025, 1, 1), date(2025, 1, 31))
    assert url == (
        "https://archive-api.open-meteo.com/v1/archive?latitude=40.4168&longitude=-3.7038"
        "&start_date=2025-01-01&end_date=2025-01-31"
        "&hourly=temperature_2m,shortwave_radiation,wind_speed_100m"
    )
    assert "timezone" not in url


def test_current_url_lists_every_city():
    url = open_meteo.build_current_url([CITIES["madrid"], CITIES["barcelona"]])
    assert "latitude=40.4168,41.3874" in url and "longitude=-3.7038,2.1686" in url
    assert url.endswith("current=temperature_2m,shortwave_radiation,wind_speed_100m")


def test_jobs_per_city_and_month(paths):
    jobs = open_meteo.jobs(date(2025, 1, 20), date(2025, 2, 10), paths)
    assert len(jobs) == len(CITIES) * 2
    madrid_january = next(job for job in jobs if "open_meteo_madrid_2025-01" in job.destination.name)
    assert (
        madrid_january.destination == paths.raw / "open_meteo" / "madrid" / "open_meteo_madrid_2025-01.json"
    )
    assert "start_date=2025-01-20&end_date=2025-01-31" in madrid_january.url
    assert madrid_january.settle_days == 7


def test_read_archive(spark, fixtures_dir):
    weather = open_meteo.read_archive(spark, fixtures_dir / "open_meteo")
    assert weather.count() == 96
    assert {row.city for row in weather.select("city").distinct().collect()} == {"madrid", "barcelona"}
    first = weather.agg(F.date_format(F.min("ts_utc"), "yyyy-MM-dd HH:mm").alias("t")).first().t
    assert first == "2025-01-15 00:00"
    madrid_midnight = weather.where("city = 'madrid'").orderBy("ts_utc").first()
    assert madrid_midnight.temperature_c == -1.2
    assert {"shortwave_radiation_wm2", "wind_speed_100m_kmh"} <= set(weather.columns)


def test_ingest(spark, paths, fixtures_dir):
    shutil.copytree(fixtures_dir / "open_meteo", paths.raw / "open_meteo")
    assert open_meteo.ingest(spark, paths) == 96
