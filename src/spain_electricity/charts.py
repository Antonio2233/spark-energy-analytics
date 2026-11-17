"""Altair charts for the UI: readable names, formatted axes and theme-aware colors.

Every builder takes small pandas frames from ``views`` and a ``ChartTheme``; none of them reads data.
"""

from dataclasses import dataclass

import altair as alt
import pandas as pd

from spain_electricity import catalog

DAY_FORMAT = "%a %d %b"
HOUR_FORMAT = "%H:%M"
TOOLTIP_TIME_FORMAT = "%a %d %b %H:%M"
TOOLTIP_DAY_FORMAT = "%a %d %b %Y"

CORRELATION_VARIABLES = (
    "demand_mw",
    "temperature_c",
    "wind_speed_100m_kmh",
    "shortwave_radiation_wm2",
    "renewable_share",
    "spot_price_eur_mwh",
    "gas_price_eur_mwh",
)
VARIABLE_LABELS = {key: catalog.label(key) for key in CORRELATION_VARIABLES}
# Color slot order of the mix, taken from the catalog: adjacent slots are the validated pairs.
TECHNOLOGY_LABELS = {entry.key: entry.label for entry in catalog.TECHNOLOGIES}
WORKING_DAY = "Working day"
DAY_OFF = catalog.label("day_type")


@dataclass(frozen=True)
class ChartTheme:
    """Colors of one Streamlit theme.

    ``slots`` are the validated categorical colors, assigned in order. ``diverging`` is the
    negative pole, the neutral midpoint and the positive pole. Cell values are written in ``ink``,
    or in ``strong_ink`` from ``strong_from`` (absolute value) up, where the cell gets saturated.
    """

    slots: tuple[str, ...]
    surface: str
    ink: str
    strong_ink: str
    strong_from: float
    diverging: tuple[str, str, str]


LIGHT = ChartTheme(
    slots=("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"),
    surface="#ffffff",
    ink="#0b0b0b",
    strong_ink="#0b0b0b",
    strong_from=1.0,
    diverging=("#e34948", "#f0efec", "#2a78d6"),
)
DARK = ChartTheme(
    slots=("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"),
    surface="#0e1117",
    ink="#f0efec",
    strong_ink="#0b0b0b",
    strong_from=0.75,
    diverging=("#e66767", "#383835", "#3987e5"),
)


def theme_for(dark: bool) -> ChartTheme:
    """Return the dark or the light chart theme."""
    return DARK if dark else LIGHT


def _title(text: str, subtitle: str | None = None, theme: ChartTheme | None = None) -> alt.TitleParams:
    colors = {} if theme is None else {"color": theme.ink, "subtitleColor": theme.ink}
    return alt.TitleParams(text=text, subtitle=subtitle or "", anchor="start", **colors)


def _color(domain: list[str], theme: ChartTheme, title: str | None = None, columns: int = 0) -> alt.Color:
    return alt.Color(
        "series:N",
        scale=alt.Scale(domain=domain, range=list(theme.slots[: len(domain)])),
        sort=domain,
        title=title,
        legend=alt.Legend(orient="top", columns=columns or len(domain)),
    )


def _time_axis(time_format: str) -> alt.Axis:
    tick_count = "day" if time_format == DAY_FORMAT else "hour"
    return alt.Axis(format=time_format, labelAngle=0, tickCount=tick_count, title=None)


def line_chart(
    data: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    y_title: str,
    domain: list[str],
    theme: ChartTheme,
    series: str = "series",
    subtitle: str | None = None,
    time_format: str = DAY_FORMAT,
    value_format: str = ",.0f",
) -> alt.LayerChart:
    """Return a multi-series line chart with a hover crosshair and a tooltip."""
    data = data.rename(columns={series: "series"})
    hover = alt.selection_point(fields=[x], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(data).encode(
        x=alt.X(f"{x}:T", axis=_time_axis(time_format)),
        y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=False), axis=alt.Axis(format=value_format)),
        color=_color(domain, theme),
    )
    lines = base.mark_line(strokeWidth=2)
    points = (
        base.mark_point(size=64, filled=True)
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip(f"{x}:T", title="Time (UTC)", format=TOOLTIP_TIME_FORMAT),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip(f"{y}:Q", title=y_title, format=value_format),
            ],
        )
        .add_params(hover)
    )
    rule = alt.Chart(data).mark_rule(strokeWidth=1, opacity=0.4).encode(x=f"{x}:T").transform_filter(hover)
    return alt.layer(lines, points, rule, title=_title(title, subtitle, theme)).properties(height=280)


def mix_chart(mix: pd.DataFrame, theme: ChartTheme) -> alt.LayerChart:
    """Return the hourly generation mix, one line per technology.

    Clicking a technology in the legend highlights its line and fades the others; the hover
    tooltip lists every technology at that hour.
    """
    order = list(TECHNOLOGY_LABELS.values())
    data = mix.assign(technology=mix["technology"].map(TECHNOLOGY_LABELS))
    hover = alt.selection_point(fields=["hour_utc"], nearest=True, on="pointerover", empty=False)
    picked = alt.selection_point(fields=["technology"], bind="legend")
    lines = (
        alt.Chart(data)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("hour_utc:T", axis=_time_axis(DAY_FORMAT)),
            y=alt.Y("mw:Q", title="MW", axis=alt.Axis(format=",.0f")),
            color=alt.Color(
                "technology:N",
                scale=alt.Scale(domain=order, range=list(theme.slots)),
                sort=order,
                title=None,
                legend=alt.Legend(orient="top", columns=4),
            ),
            opacity=alt.condition(picked, alt.value(1), alt.value(0.15)),
        )
        .add_params(picked)
    )
    tooltip = [alt.Tooltip("hour_utc:T", title="Time (UTC)", format=TOOLTIP_TIME_FORMAT)] + [
        alt.Tooltip(f"{label}:Q", title=f"{label} (MW)", format=",.0f") for label in order
    ]
    rule = (
        alt.Chart(data)
        .transform_pivot("technology", value="mw", groupby=["hour_utc"])
        .mark_rule(strokeWidth=8, color=theme.ink)
        .encode(
            x="hour_utc:T",
            opacity=alt.condition(hover, alt.value(0.25), alt.value(0)),
            tooltip=tooltip,
        )
        .add_params(hover)
    )
    title = _title(
        "Generation mix",
        "Hourly mean MW of each technology. Click a name in the legend to highlight it",
        theme,
    )
    return alt.layer(lines, rule, title=title).properties(height=340)


def demand_temperature_scatter(daily: pd.DataFrame, theme: ChartTheme) -> alt.LayerChart:
    """Return daily demand against temperature, one dot per day and a quadratic trend per day type.

    Holidays go with weekends: demand on a national holiday looks like a Sunday's.
    """
    day_off = daily["is_weekend"].astype(bool) | daily["is_holiday"].astype(bool)
    data = daily.assign(
        day_type=day_off.map({True: DAY_OFF, False: WORKING_DAY}),
        local_date=pd.to_datetime(daily["local_date"]),
    )
    domain = [WORKING_DAY, DAY_OFF]
    color = alt.Color(
        "day_type:N",
        scale=alt.Scale(domain=domain, range=list(theme.slots[: len(domain)])),
        sort=domain,
        title=None,
        legend=alt.Legend(orient="top"),
    )
    x = alt.X("temperature_c:Q", title="Mean temperature (°C)", scale=alt.Scale(zero=False))
    y = alt.Y(
        "demand_mw:Q", title="Mean demand (MW)", scale=alt.Scale(zero=False), axis=alt.Axis(format=",.0f")
    )
    dots = (
        alt.Chart(data)
        .mark_circle(size=80, opacity=0.85, stroke=theme.surface, strokeWidth=1)
        .encode(
            x=x,
            y=y,
            color=color,
            tooltip=[
                alt.Tooltip("local_date:T", title="Day", format=TOOLTIP_DAY_FORMAT),
                alt.Tooltip("day_type:N", title="Day type"),
                alt.Tooltip("temperature_c:Q", title="Temperature (°C)", format=".1f"),
                alt.Tooltip("demand_mw:Q", title="Demand (MW)", format=",.0f"),
            ],
        )
    )
    trend = (
        alt.Chart(data)
        .transform_regression("temperature_c", "demand_mw", groupby=["day_type"], method="poly", order=2)
        .mark_line(strokeWidth=2, strokeDash=[6, 4])
        .encode(x=x, y=y, color=color)
    )
    title = _title(
        "Daily demand vs national temperature",
        "One dot per day; dashed lines are a quadratic trend, because over a year demand rises with both "
        "cold and heat",
        theme,
    )
    return alt.layer(dots, trend, title=title).properties(height=340)


def correlation_cells(matrix: pd.DataFrame) -> pd.DataFrame:
    """Return each pair of the matrix once (below the diagonal) as ``row``, ``column``, ``pearson_r``."""
    names = [VARIABLE_LABELS.get(column, column) for column in matrix.columns]
    records = [
        (names[row], names[column], float(matrix.iat[row, column]))
        for row in range(len(names))
        for column in range(row)
    ]
    return pd.DataFrame(records, columns=["row", "column", "pearson_r"])


def correlation_heatmap(matrix: pd.DataFrame, theme: ChartTheme) -> alt.LayerChart:
    """Return the lower triangle of a correlation matrix, colored by sign and labelled with each value."""
    cells = correlation_cells(matrix)
    names = [VARIABLE_LABELS.get(column, column) for column in matrix.columns]
    x = alt.X(
        "column:N",
        sort=names[:-1],
        title=None,
        axis=alt.Axis(labelAngle=0, labelLimit=0, orient="bottom"),
    )
    y = alt.Y("row:N", sort=names[1:], title=None, axis=alt.Axis(labelLimit=0))
    tooltip = [
        alt.Tooltip("row:N", title="Variable"),
        alt.Tooltip("column:N", title="With"),
        alt.Tooltip("pearson_r:Q", title="Pearson r", format=".2f"),
    ]
    rects = (
        alt.Chart(cells)
        .mark_rect(stroke=theme.surface, strokeWidth=2, cornerRadius=4)
        .encode(
            x=x,
            y=y,
            color=alt.Color(
                "pearson_r:Q",
                # Lab keeps each arm on its own hue; the default HCL blend drifts through pink.
                scale=alt.Scale(domain=[-1, 0, 1], range=list(theme.diverging), interpolate="lab"),
                title="Pearson r",
            ),
            tooltip=tooltip,
        )
    )
    values = (
        alt.Chart(cells)
        .mark_text(fontSize=13)
        .encode(
            x=x,
            y=y,
            text=alt.Text("pearson_r:Q", format=".2f"),
            color=alt.condition(
                f"abs(datum.pearson_r) >= {theme.strong_from}",
                alt.value(theme.strong_ink),
                alt.value(theme.ink),
            ),
            tooltip=tooltip,
        )
    )
    title = _title(
        "Correlation between hourly variables (Pearson r)",
        "+1: they rise together · −1: one rises as the other falls · 0: no linear relation",
        theme,
    )
    return alt.layer(rects, values, title=title).properties(height=380)


DESK = ChartTheme(
    slots=("#53b4ff", "#49dcb1", "#f6c76b", "#bd9bff", "#ff996b", "#61c5cf", "#ec86b8", "#a6b5ca"),
    surface="#111e30",
    ink="#dbe6f5",
    strong_ink="#07111f",
    strong_from=0.75,
    diverging=("#ff996b", "#24344e", "#49dcb1"),
)


def desk_demand_chart(timeline: pd.DataFrame, theme: ChartTheme = DESK) -> alt.LayerChart:
    """Compare 5-minute observations with hourly estimates on a common MW axis."""
    names = {
        "demand_mw": catalog.label("demand_mw"),
        "expected_demand_mw": catalog.label("expected_demand_mw"),
        "peninsular_forecast_mw": catalog.label("peninsular_forecast_mw"),
    }
    names = {key: value for key, value in names.items() if timeline[key].notna().any()}
    long = (
        timeline[["ts_utc", *names]]
        .rename(columns=names)
        .melt(id_vars="ts_utc", var_name="series", value_name="mw")
        .dropna(subset=["mw"])
    )
    return line_chart(
        long,
        x="ts_utc",
        y="mw",
        title="Demand & expectations",
        y_title="MW",
        domain=list(names.values()),
        theme=theme,
        time_format=HOUR_FORMAT,
        subtitle="Observed every 5 min · ML and REE are hourly estimates / forecasts",
    )


def desk_mix_area(timeline: pd.DataFrame, theme: ChartTheme = DESK) -> alt.Chart:
    """Show the tracked generation stack; it is not forced to equal measured demand."""
    columns = {f"{key}_mw": label for key, label in TECHNOLOGY_LABELS.items()}
    data = timeline[["ts_utc", *columns]].rename(columns=columns)
    return (
        alt.Chart(data)
        .transform_fold(list(columns.values()), as_=["series", "mw"])
        .transform_filter("isValid(datum.mw) && datum.mw >= 0")
        .transform_calculate(gw="datum.mw / 1000")
        .mark_area(opacity=0.9)
        .encode(
            x=alt.X("ts_utc:T", axis=_time_axis(HOUR_FORMAT)),
            y=alt.Y("gw:Q", stack="zero", title="GW · tracked generation"),
            color=_color(list(columns.values()), theme, columns=4),
            tooltip=[
                alt.Tooltip("ts_utc:T", title="UTC", format=TOOLTIP_TIME_FORMAT),
                alt.Tooltip("series:N", title="Technology"),
                alt.Tooltip("gw:Q", title="GW", format=".2f"),
            ],
        )
        .properties(
            height=280,
            title=_title("Generation stack", "Positive output of eight tracked technologies", theme),
        )
    )


def desk_mix_ring(mix: pd.DataFrame, theme: ChartTheme = DESK) -> alt.LayerChart:
    """Show current shares of tracked output, with demand-based renewables kept separate."""
    data = mix.assign(series=mix.technology.map(TECHNOLOGY_LABELS))
    arcs = (
        alt.Chart(data)
        .mark_arc(innerRadius=44, outerRadius=64, stroke=theme.surface, strokeWidth=2)
        .encode(
            theta=alt.Theta("mw:Q"),
            color=alt.Color(
                "series:N",
                title=None,
                scale=alt.Scale(domain=list(TECHNOLOGY_LABELS.values()), range=list(theme.slots)),
                legend=alt.Legend(
                    orient="right", labelLimit=135, labelFontSize=10, symbolSize=65, rowPadding=2
                ),
            ),
            tooltip=[
                alt.Tooltip("series:N", title="Technology"),
                alt.Tooltip("mw:Q", title="MW", format=",.0f"),
                alt.Tooltip("generation_fraction:Q", title="Tracked output share", format=".1%"),
            ],
        )
    )
    total = float(mix.mw.sum()) / 1000
    center = (
        alt.Chart(pd.DataFrame({"label": [f"{total:.1f} GW"]}))
        .mark_text(fontSize=19, fontWeight=600, color=theme.ink)
        .encode(text="label:N")
    )
    return alt.layer(arcs, center).properties(
        height=280, title=_title("Supply mix", "Shares of tracked generation", theme)
    )


def desk_price_profile(
    prices: pd.DataFrame, fuel_cost: float | None, theme: ChartTheme = DESK
) -> alt.LayerChart:
    """Display published day-ahead delivery prices, including negative prices and future delivery hours."""
    bars = (
        alt.Chart(prices)
        .mark_bar(size=14, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("hour_utc:T", axis=_time_axis(HOUR_FORMAT), title="Delivery hour · UTC"),
            y=alt.Y("spot_price_eur_mwh:Q", title="€/MWh"),
            color=alt.condition(
                "datum.spot_price_eur_mwh < 0", alt.value(theme.slots[1]), alt.value(theme.slots[0])
            ),
            tooltip=[
                alt.Tooltip("hour_utc:T", title="Delivery UTC", format=TOOLTIP_TIME_FORMAT),
                alt.Tooltip("spot_price_eur_mwh:Q", title="Day-ahead spot €/MWh", format=".2f"),
                alt.Tooltip("pvpc_eur_mwh:Q", title="PVPC €/MWh", format=".2f"),
            ],
        )
    )
    # A visible key keeps the price bar and scenario line interpretable without relying on color alone.
    key = ["Day-ahead spot"] + (["Fuel-only cost scenario"] if fuel_cost is not None else [])
    legend = (
        alt.Chart(pd.DataFrame({"series": key, "dummy": [0] * len(key)}))
        .mark_point(opacity=0)
        .encode(color=_color(key, theme))
    )
    layers = [bars, legend]
    if fuel_cost is not None:
        layers.append(
            alt.Chart(pd.DataFrame({"value": [fuel_cost]}))
            .mark_rule(strokeDash=[6, 4], color=theme.slots[1])
            .encode(y="value:Q")
        )
    return alt.layer(*layers).properties(
        height=260,
        title=_title(
            "Day-ahead delivery curve",
            "Published hourly means · includes remaining delivery hours, not future trades",
            theme,
        ),
    )


def desk_price_scatter(hourly: pd.DataFrame, theme: ChartTheme = DESK) -> alt.Chart:
    """Explore co-movement, without claiming causation or predicting a trading return."""
    data = hourly.dropna(subset=["residual_demand_mw", "spot_price_eur_mwh", "renewable_share"])
    return (
        alt.Chart(data)
        .transform_calculate(residual_gw="datum.residual_demand_mw / 1000")
        .mark_circle(size=95, opacity=0.85)
        .encode(
            x=alt.X("residual_gw:Q", title="Residual demand · GW", scale=alt.Scale(zero=False)),
            y=alt.Y("spot_price_eur_mwh:Q", title="Spot · €/MWh", scale=alt.Scale(zero=False)),
            color=alt.Color(
                "renewable_share:Q",
                title="Renewables / demand",
                scale=alt.Scale(range=[theme.slots[0], theme.slots[1]]),
                legend=alt.Legend(format=".0%"),
            ),
            tooltip=[
                alt.Tooltip("hour_utc:T", title="UTC", format=TOOLTIP_TIME_FORMAT),
                alt.Tooltip("residual_gw:Q", title="Residual GW", format=".2f"),
                alt.Tooltip("spot_price_eur_mwh:Q", title="Spot €/MWh", format=".2f"),
                alt.Tooltip("renewable_share:Q", title="Renewables / demand", format=".1%"),
            ],
        )
        .properties(
            height=260,
            title=_title(
                "Power-price drivers", "One point per observed hour · co-movement, not causation", theme
            ),
        )
    )
