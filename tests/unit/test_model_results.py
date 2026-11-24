import pandas as pd

from spain_electricity.model_results import comparison_table, update_table


def scores(mae):
    return {"mae": mae, "rmse": mae * 1.5, "r2": 0.5, "bias": 1.0}


REPORTS = {
    "demand": {
        "unit": "MW",
        "baselines": {"ree": "peninsular_forecast_mw", "same_hour_yesterday": "demand_mw_24h_ago"},
        "evaluation": {
            "validation": {
                "model": scores(9),
                "ree": scores(2),
                "same_hour_yesterday": scores(8),
                "hours": 20,
            },
            "test": {"model": scores(10), "ree": scores(3), "same_hour_yesterday": scores(9), "hours": 12},
        },
        "update": {
            "train_until": "2026-10-31",
            "evaluated_from": "2026-11-01",
            "evaluated_until": "2026-11-23",
            "evaluation": {
                "model": scores(4),
                "model_through_2025": scores(10),
                "ree": scores(3),
                "same_hour_yesterday": scores(9),
                "hours": 5,
            },
        },
    },
    "price": {
        "unit": "EUR/MWh",
        "baselines": {"same_hour_yesterday": "spot_price_eur_mwh_24h_ago"},
        "evaluation": {"test": {"model": scores(40), "same_hour_yesterday": scores(20), "hours": 12}},
        "update": None,
    },
    "day_types": {"chosen_k": 3},
}


def test_comparison_table_reads_every_regression_the_same_way():
    table = comparison_table(REPORTS)
    assert list(zip(table.target, table.source, strict=True)) == [
        ("demand", "model"),
        ("demand", "ree"),
        ("demand", "same_hour_yesterday"),
        ("price", "model"),
        ("price", "same_hour_yesterday"),
    ]
    assert table.set_index(["target", "source"]).loc[("price", "model"), "unit"] == "EUR/MWh"
    assert set(table.hours) == {12}


def test_update_table_compares_both_training_periods_on_the_same_hours():
    table = update_table(REPORTS)
    assert list(table.source) == ["model", "model_through_2025", "ree", "same_hour_yesterday"]
    assert set(table.target) == {"demand"}
    assert set(table.hours) == {5}
    assert table.iloc[0]["evaluated"] == "2026-11-01 to 2026-11-23"


def test_tables_are_empty_without_reports():
    assert comparison_table({}).empty and update_table({}).empty
    assert isinstance(update_table({}), pd.DataFrame)
