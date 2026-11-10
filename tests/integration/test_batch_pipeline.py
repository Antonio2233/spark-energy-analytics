import pytest
from helpers import copy_raw_fixtures
from pyspark.sql import functions as F

from spain_electricity import catalog, pipeline, views


@pytest.mark.integration
def test_batch_pipeline_from_raw_fixtures(spark, paths, fixtures_dir):
    copy_raw_fixtures(fixtures_dir, paths)
    counts = pipeline.run_ingest(spark, paths)
    assert counts == {
        "generation": 1152,
        "demand_forecast": 300,
        "market_price": 240,
        "weather": 96,
        "gas_price": 1,
        "hourly_features": 96,
    }
    hourly = spark.read.parquet(str(paths.processed / "hourly_features"))
    assert hourly.select("hour_utc").distinct().count() == 96
    coverage = hourly.agg(
        F.count("forecast_demand_mw").alias("forecast"),
        F.count("spot_price_eur_mwh").alias("spot"),
        F.count("temperature_c").alias("temperature"),
        F.count("gas_price_eur_mwh").alias("gas"),
    ).first()
    assert (coverage.forecast, coverage.spot, coverage.temperature, coverage.gas) == (25, 24, 47, 0)


@pytest.mark.integration
def test_every_ingested_column_is_described(spark, paths, fixtures_dir):
    copy_raw_fixtures(fixtures_dir, paths)
    pipeline.run_ingest(spark, paths)
    for dataset in views.DATASETS:
        for column in views.dataset_attributes(spark, paths.processed / dataset)["column"]:
            assert catalog.attribute(column)[1], f"{dataset}.{column} has no description"
