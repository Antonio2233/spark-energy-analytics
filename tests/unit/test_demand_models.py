from datetime import date, datetime

import pytest

from spain_electricity import models, sampling
from spain_electricity.sources import ree_forecast


def test_sample_is_bounded_seasonal_and_reproducible(paths):
    days = sampling.sample_days(today=date(2026, 11, 16))
    assert len(days) == 96 and days == sorted(set(days))
    assert {day.year for day in days} == set(range(2021, 2027))
    for year in range(2021, 2027):
        selected = [day for day in days if day.year == year]
        assert {day.month for day in selected} == {1, 4, 7, 10}
        assert len({day.weekday() for day in selected}) == 7
    jobs = sampling.sample_jobs(paths)
    assert len(jobs) == len({job.destination for job in jobs}) == 248
    assert not any("mibgas" in str(job.destination) or "redata" in str(job.destination) for job in jobs)
    assert not sampling.sample_days(today=date(2020, 1, 1))
    with pytest.raises(ValueError):
        sampling.sample_days(8)


def test_forecast_parser_uses_own_day_and_handles_repeated_hour(spark, paths):
    path = paths.raw / "ree_forecast" / "ree_forecast_2025-10-26.jsonp"
    path.parent.mkdir(parents=True)
    path.write_text(
        'cb({"valoresPrevistaProgramada":['
        '{"ts":"2025-10-25 23:00","pre":999},'
        '{"ts":"2025-10-26 2A:00","pre":100},'
        '{"ts":"2025-10-26 2B:00","pre":200}]});'
    )
    rows = ree_forecast.read_raw(spark, path.parent).orderBy("ts_utc").collect()
    assert [(row.ts_utc, row.peninsular_forecast_mw) for row in rows] == [
        (datetime(2025, 10, 26, 0), 100.0),
        (datetime(2025, 10, 26, 1), 200.0),
    ]
    assert ree_forecast.is_valid_payload(path.read_bytes())
    assert not ree_forecast.is_valid_payload(b'cb({"valoresPrevistaProgramada":[]});')


def test_temporal_split_excludes_blackout_and_invalid_values(spark):
    rows = [(date(year, 1, 1), 20000.0, 10.0, 19900.0) for year in range(2020, 2028)]
    rows.extend(
        [
            (date(2025, 4, 28), 15000.0, 10.0, 19900.0),
            (date(2025, 1, 2), 20000.0, None, 19900.0),
            (date(2025, 1, 3), 20000.0, float("nan"), 19900.0),
        ]
    )
    frame = spark.createDataFrame(
        rows, "local_date date, demand_mw double, temperature_c double, peninsular_forecast_mw double"
    )
    splits = models.temporal_split(models.valid_hours(frame))
    assert {name: df.count() for name, df in splits.items()} == {"train": 4, "validation": 1, "test": 1}


def test_baseline_pairing_does_not_substitute_national_forecast(spark):
    frame = spark.createDataFrame(
        [(1.0, None, 2.0), (1.0, 1.5, 2.0)],
        "demand_mw double, peninsular_forecast_mw double, forecast_demand_mw double",
    )
    assert models.paired_rows(frame).count() == 1


def test_real_forecast_fixture_keeps_scheduled_demand(spark, fixtures_dir):
    rows = ree_forecast.read_raw(spark, fixtures_dir / "ree_forecast").collect()
    assert rows
    assert any(row.peninsular_scheduled_mw is not None for row in rows)


def test_naive_baseline_is_the_same_hour_the_day_before(spark):
    df = spark.createDataFrame(
        [
            (datetime(2025, 1, 1, 10), 100.0),
            (datetime(2025, 1, 2, 10), 200.0),
            (datetime(2025, 1, 2, 11), 300.0),
        ],
        "hour_utc timestamp, demand_mw double",
    )
    rows = models.add_naive_baseline(df).orderBy("hour_utc").collect()
    assert [row[models.NAIVE] for row in rows] == [None, 100.0, None]


def test_candidate_grid_loops_param_grids_for_three_families():
    grid = models.candidate_grid()
    assert [kind for kind, _, _ in grid] == ["linear", "random_forest", "gbt"]
    assert [len(param_maps) for _, _, param_maps in grid] == [6, 2, 4]
    for _, pipeline, param_maps in grid:
        estimator = pipeline.getStages()[-1]
        assert all(param.parent == estimator.uid for params in param_maps for param in params)


def test_paired_rows_can_require_every_baseline(spark):
    df = spark.createDataFrame(
        [(1.0, 2.0), (1.0, None), (None, 2.0)], f"{models.BASELINE} double, {models.NAIVE} double"
    )
    assert models.paired_rows(df, (models.BASELINE, models.NAIVE)).count() == 1
    assert models.paired_rows(df).count() == 2
