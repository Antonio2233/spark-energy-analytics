from datetime import date

from spain_electricity import regression_models
from spain_electricity.regression_models import PRICE, RENEWABLES


def test_price_lets_the_iberian_exception_change_the_gas_effect(spark):
    assert ("gas_price_eur_mwh", "iberian_exception") in PRICE.interactions
    stages = regression_models.build_pipeline(PRICE, "linear").getStages()
    assert "gas_price_eur_mwh_x_iberian_exception" in stages[0].getStatement()


def test_renewable_share_uses_demand_its_denominator():
    assert "demand_mw" in RENEWABLES.numeric


def test_candidate_families_match_the_demand_search(spark):
    assert [family for family, _, _ in regression_models.candidate_grid(PRICE)] == [
        "linear",
        "random_forest",
        "gbt",
    ]


def test_day_after_an_anomaly_is_not_scored_against_it(spark):
    frame = spark.createDataFrame(
        [(date(2025, 4, 29), 10.0, 9.0), (date(2025, 4, 30), 10.0, 9.0)],
        "local_date date, spot_price_eur_mwh double, spot_price_eur_mwh_24h_ago double",
    )
    scored = regression_models.paired(frame, PRICE).collect()
    assert [row.local_date for row in scored] == [date(2025, 4, 30)]
