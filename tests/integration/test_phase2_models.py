from datetime import datetime, timedelta

import pytest
from pyspark.ml import PipelineModel

from spain_electricity.day_types import train_day_types
from spain_electricity.regression_models import PRICE, RENEWABLES, train

pytestmark = pytest.mark.integration

TINY_GRIDS = {
    "linear": {"regParam": [0.1]},
    "random_forest": {"numTrees": [8], "maxDepth": [4]},
    "gbt": {"maxIter": [5], "maxDepth": [3]},
}


def history(spark):
    rows = []
    for year in range(2021, 2027):
        first = datetime(year, 1, 5)
        for index in range(96):
            moment = first + timedelta(hours=index)
            hour = (moment.hour + 1) % 24
            local_day = (moment + timedelta(hours=1)).date()
            wind = 5.0 + hour / 2
            solar = float(max(0, 12 - abs(hour - 12)))
            share = 0.1 + wind / 100 + solar / 100
            rows.append(
                (
                    moment,
                    local_day,
                    hour,
                    1,
                    30000.0 + hour * 100,
                    10.0,
                    False,
                    False,
                    30.0,
                    share,
                    40.0 + 20.0 * share + hour,
                    wind,
                    solar,
                )
            )
    return spark.createDataFrame(
        rows,
        "hour_utc timestamp, local_date date, local_hour int, month int, "
        "demand_mw double, temperature_c double, is_weekend boolean, is_holiday boolean, "
        "gas_price_eur_mwh double, "
        "renewable_share double, spot_price_eur_mwh double, wind_speed_100m_kmh double, "
        "shortwave_radiation_wm2 double",
    )


@pytest.mark.parametrize("spec", [PRICE, RENEWABLES])
def test_chronological_models_save_and_reload(spark, paths, spec, monkeypatch):
    history(spark).write.parquet(str(paths.processed / "hourly_features"))
    from pyspark.ml import Pipeline
    from pyspark.sql import functions as F

    fitted_years = []
    original_fit = Pipeline.fit

    def observed_fit(self, frame, *args, **kwargs):
        fitted_years.append(
            {row.year for row in frame.select(F.year("local_date").alias("year")).distinct().collect()}
        )
        return original_fit(self, frame, *args, **kwargs)

    monkeypatch.setattr(Pipeline, "fit", observed_fit)
    report = train(spark, paths, spec, grids=TINY_GRIDS)
    assert fitted_years[:3] == [{2021, 2022, 2023, 2024}] * 3
    assert fitted_years[3] == {2021, 2022, 2023, 2024, 2025}
    assert report["rows"] == {"train": 384, "validation": 72, "test": 72}
    # Like demand, the refit uses every valid 2025 hour, not only those with a baseline.
    assert report["refit"] == {"years": "2021-2025", "hours": 384 + 96}
    assert [item["model"] for item in report["candidates"]] == ["linear", "random_forest", "gbt"]
    assert "bias" in report["test"]["model"] and "bias" in report["validation_baseline"]
    assert not (paths.models / spec.name / "metrics.json.part").exists()
    assert report["selected"]["model"] == min(report["candidates"], key=lambda item: item["mae"])["model"]
    assert report["test"]["hours"] == report["rows"]["test"]
    assert report["features"] and all(item["feature"] for item in report["features"])
    assert PipelineModel.load(str(paths.models / spec.name / "pipeline"))


def test_day_types_save_training_only_selection_and_descriptors(spark, paths):
    history(spark).write.parquet(str(paths.processed / "hourly_features"))
    # Add two shaped variations so k has genuine, nondegenerate choices.
    from pyspark.sql import functions as F

    from spain_electricity.day_types import daily_profiles

    frame = history(spark).withColumn(
        "demand_mw",
        F.col("demand_mw")
        + F.when((F.dayofmonth("local_date") % 2 == 0) & (F.col("local_hour") > 12), 5000).otherwise(0),
    )
    temp = paths.processed / "hourly_features"
    frame.write.mode("overwrite").parquet(str(temp))
    assert daily_profiles(frame).count() > 3
    report = train_day_types(spark, paths, (2,))
    assert report["chosen_k"] == 2
    assert report["training_days"] < report["assigned_days"]
    assert sum(row["days"] for row in report["summary"]) == report["assigned_days"]
