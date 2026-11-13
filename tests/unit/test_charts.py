import pandas as pd

from spain_electricity import charts

COLUMNS = ["demand_mw", "temperature_c", "spot_price_eur_mwh"]


def _matrix() -> pd.DataFrame:
    values = [[1.0, -0.42, 0.46], [-0.42, 1.0, 0.1], [0.46, 0.1, 1.0]]
    return pd.DataFrame(values, index=COLUMNS, columns=COLUMNS)


def _mark(layer: dict) -> str:
    mark = layer["mark"]
    return mark if isinstance(mark, str) else mark["type"]


def _layer(spec: dict, mark: str) -> dict:
    return next(layer for layer in spec.get("layer", [spec]) if _mark(layer) == mark)


def _rows(spec: dict) -> list[dict]:
    return next(iter(spec["datasets"].values()))


def test_correlation_cells_keep_each_pair_once_with_readable_names():
    cells = charts.correlation_cells(_matrix())
    assert list(cells.itertuples(index=False, name=None)) == [
        ("Temperature", "Demand (5-min feed)", -0.42),
        ("Day-ahead price", "Demand (5-min feed)", 0.46),
        ("Day-ahead price", "Temperature", 0.1),
    ]


def test_correlation_heatmap_keeps_column_names_horizontal():
    spec = charts.correlation_heatmap(_matrix(), charts.LIGHT).to_dict()
    assert _layer(spec, "rect")["encoding"]["x"]["axis"]["labelAngle"] == 0


def test_correlation_heatmap_blends_colors_without_hue_shift():
    spec = charts.correlation_heatmap(_matrix(), charts.LIGHT).to_dict()
    assert _layer(spec, "rect")["encoding"]["color"]["scale"]["interpolate"] == "lab"


def test_correlation_heatmap_writes_the_value_on_each_cell():
    spec = charts.correlation_heatmap(_matrix(), charts.LIGHT).to_dict()
    text = _layer(spec, "text")["encoding"]["text"]
    assert (text["field"], text["format"]) == ("pearson_r", ".2f")
    assert len(_rows(spec)) == 3


def test_mix_chart_names_technologies_for_people():
    mix = pd.DataFrame(
        {
            "hour_utc": pd.to_datetime(["2025-01-01 00:00", "2025-01-01 00:00"]),
            "technology": ["solar_pv", "combined_cycle"],
            "mw": [0.0, 5000.0],
        }
    )
    spec = charts.mix_chart(mix, charts.LIGHT).to_dict()
    assert {row["technology"] for row in _rows(spec)} == {"Solar PV", "Combined cycle"}


def test_mix_chart_draws_one_unstacked_line_per_technology():
    mix = pd.DataFrame(
        {
            "hour_utc": pd.to_datetime(["2025-01-01 00:00", "2025-01-01 00:00"]),
            "technology": ["solar_pv", "combined_cycle"],
            "mw": [0.0, 5000.0],
        }
    )
    spec = charts.mix_chart(mix, charts.LIGHT).to_dict()
    assert not any(_mark(layer) == "area" for layer in spec["layer"])
    line = _layer(spec, "line")
    assert line["encoding"]["color"]["field"] == "technology"
    assert "stack" not in line["encoding"]["y"]


def test_mix_chart_legend_highlights_a_technology():
    mix = pd.DataFrame(
        {"hour_utc": pd.to_datetime(["2025-01-01 00:00"]), "technology": ["wind"], "mw": [9000.0]}
    )
    spec = charts.mix_chart(mix, charts.LIGHT).to_dict()
    params = spec.get("params", []) + [param for layer in spec["layer"] for param in layer.get("params", [])]
    assert any(param.get("bind") == "legend" for param in params)


def test_line_chart_formats_time_and_values():
    data = pd.DataFrame(
        {"hour_utc": pd.to_datetime(["2025-01-01 00:00"]), "mw": [26403.9], "series": ["Demand"]}
    )
    spec = charts.line_chart(
        data, x="hour_utc", y="mw", title="Demand", y_title="MW", domain=["Demand"], theme=charts.LIGHT
    ).to_dict()
    encoding = _layer(spec, "line")["encoding"]
    assert encoding["x"]["axis"]["format"] == charts.DAY_FORMAT
    assert encoding["y"]["axis"]["format"] == ",.0f"


def test_scatter_groups_holidays_with_weekends():
    daily = pd.DataFrame(
        {
            "local_date": pd.to_datetime(["2025-01-04", "2025-01-06", "2025-01-07"]),
            "demand_mw": [24000.0, 23500.0, 30000.0],
            "temperature_c": [8.0, 6.5, 7.0],
            "is_weekend": [True, False, False],
            "is_holiday": [False, True, False],
        }
    )
    spec = charts.demand_temperature_scatter(daily, charts.LIGHT).to_dict()
    assert [row["day_type"] for row in _rows(spec)] == [
        "Weekend or holiday",
        "Weekend or holiday",
        "Working day",
    ]


def test_themes_use_their_own_slots():
    assert charts.theme_for(dark=False) is charts.LIGHT
    assert charts.theme_for(dark=True).slots[0] == "#3987e5"
