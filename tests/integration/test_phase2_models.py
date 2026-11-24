from datetime import date, datetime, timedelta

import pytest
from pyspark.ml import PipelineModel

from spain_electricity.day_types import train_day_types
from spain_electricity.models import estimate_column
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
    report = train(spark, paths, spec, grids=TINY_GRIDS, update_cutoff=date(2026, 1, 7))
    assert fitted_years[:3] == [{2021, 2022, 2023, 2024}] * 3
    assert fitted_years[3] == {2021, 2022, 2023, 2024, 2025}
    # The update refits the same configuration on every valid hour before the cutoff, 2026 included.
    assert fitted_years[4] == {2021, 2022, 2023, 2024, 2025, 2026}
    assert report["split_hours"] == {"train": 384, "validation": 72, "test": 72}
    # Like demand, the refit uses every valid 2025 hour, not only those with a baseline.
    assert report["refit"] == {"years": "2021-2025", "hours": 384 + 96}
    assert [item["model"] for item in report["candidates"]] == ["linear", "random_forest", "gbt"]
    assert report["baselines"] == {"same_hour_yesterday": spec.baseline}
    for split in ("validation", "test"):
        assert set(report["evaluation"][split]) == {"model", "same_hour_yesterday", "hours"}
        assert "bias" in report["evaluation"][split]["model"]
    assert not (paths.models / spec.name / "metrics.json.part").exists()
    assert report["selected"]["model"] == min(report["candidates"], key=lambda item: item["mae"])["model"]
    assert report["evaluation"]["test"]["hours"] == report["split_hours"]["test"]
    assert report["features"] and all(item["feature"] for item in report["features"])
    assert PipelineModel.load(str(paths.models / spec.name / "pipeline"))
    saved = spark.read.parquet(str(paths.models / spec.name / "predictions"))
    assert {row.split for row in saved.select("split").distinct().collect()} == {"validation", "test"}
    assert estimate_column(spec.label) in saved.columns

    # 2021-2025 (5 x 96 hours) plus local 5-6 January 2026 (23 + 24 hours) train the update; it is
    # scored on local 7-9 January (24 + 24 + 1 hours), all of which have the t-24h baseline.
    update = report["update"]
    assert update["training_hours"] == 480 + 47
    assert update["train_until"] == "2026-01-06"
    assert (update["evaluated_from"], update["evaluated_until"]) == ("2026-01-07", "2026-01-09")
    assert set(update["evaluation"]) == {"model", "model_through_2025", "same_hour_yesterday", "hours"}
    assert update["evaluation"]["hours"] == 49
    assert update["features"]
    folder = paths.models / spec.name / "update"
    assert PipelineModel.load(str(folder / "pipeline"))
    updated = spark.read.parquet(str(folder / "predictions"))
    assert updated.count() == 49
    assert {estimate_column(spec.label), estimate_column(spec.label, previous=True)} <= set(updated.columns)


def test_update_is_skipped_and_cleared_without_hours_after_the_cutoff(spark, paths):
    history(spark).write.parquet(str(paths.processed / "hourly_features"))
    stale = paths.models / PRICE.name / "update" / "pipeline"
    stale.mkdir(parents=True)
    report = train(spark, paths, PRICE, grids=TINY_GRIDS, update_cutoff=date(2027, 1, 1))
    assert report["update"] is None
    assert not (paths.models / PRICE.name / "update").exists()


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
    assert isinstance(PipelineModel.load(str(paths.models / "day_types" / "pipeline")), PipelineModel)
    for row in report["summary"]:
        assert row["name"] and 0 <= row["peak_local_hour"] <= 23
    assert len({row["name"] for row in report["summary"]}) == 2
    assert report["training_days"] < report["assigned_days"]
    assert sum(row["days"] for row in report["summary"]) == report["assigned_days"]
