"""Streamlit UI over the Spark views: data status, history, live stream and first analysis.

Run with:
    uv run streamlit run app/streamlit_app.py
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from spain_electricity import views
from spain_electricity.config import DataPaths
from spain_electricity.spark_session import get_spark

NO_DATA = "No data yet: run scripts/download_history.py and scripts/ingest.py"
NO_STREAM = (
    "No streaming output yet: run scripts/run_stream.py together with "
    "scripts/replay_history.py (replay) or scripts/capture_realtime.py (live)"
)
# Validated categorical slots (dataviz reference palette), assigned in fixed order.
LIGHT_SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
DARK_SLOTS = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
MIX_ORDER = [
    "hydro",
    "solar_pv",
    "wind",
    "solar_thermal",
    "nuclear",
    "combined_cycle",
    "coal",
    "cogeneration_other",
]
CORRELATION_COLUMNS = [
    "demand_mw",
    "temperature_c",
    "wind_speed_100m_kmh",
    "shortwave_radiation_wm2",
    "renewable_share",
    "spot_price_eur_mwh",
    "gas_price_eur_mwh",
]

st.set_page_config(page_title="Spain electricity · Spark", layout="wide")


@st.cache_resource
def spark_session():
    """Return the app's SparkSession, created once per server process."""
    return get_spark("streamlit", "local[2]", 4)


def slots() -> list[str]:
    """Return the categorical slots for the active theme."""
    theme = getattr(getattr(st.context, "theme", None), "type", "light")
    return DARK_SLOTS if theme == "dark" else LIGHT_SLOTS


def color_scale(domain: list[str]) -> alt.Scale:
    """Map series names to slots in fixed order."""
    return alt.Scale(domain=domain, range=slots()[: len(domain)])


def line_chart(data: pd.DataFrame, x: str, y: str, series: str, title: str, y_title: str, domain: list[str]):
    """Return a multi-series line chart with a hover crosshair and tooltip."""
    hover = alt.selection_point(fields=[x], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(data, title=title).encode(
        x=alt.X(f"{x}:T", title="Time (UTC)"),
        y=alt.Y(f"{y}:Q", title=y_title),
        color=alt.Color(
            f"{series}:N", scale=color_scale(domain), title=None, legend=alt.Legend(orient="top")
        ),
    )
    lines = base.mark_line(strokeWidth=2)
    points = (
        base.mark_point(size=64, filled=True)
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip(f"{x}:T", title="Time (UTC)"),
                alt.Tooltip(f"{series}:N"),
                alt.Tooltip(f"{y}:Q", format=",.1f"),
            ],
        )
        .add_params(hover)
    )
    rule = alt.Chart(data).mark_rule(strokeWidth=1, opacity=0.4).encode(x=f"{x}:T").transform_filter(hover)
    return alt.layer(lines, points, rule).properties(height=280)


def long_format(frame: pd.DataFrame, x: str, columns: dict[str, str], value_name: str) -> pd.DataFrame:
    """Turn wide columns into ``x``, ``series``, ``value_name`` rows, keeping the given series names."""
    renamed = frame[[x, *columns]].rename(columns=columns)
    return renamed.melt(id_vars=[x], var_name="series", value_name=value_name).dropna(subset=[value_name])


paths = DataPaths.from_env()
spark = spark_session()
root = str(paths.root)


@st.cache_data(ttl=timedelta(minutes=1), show_spinner=False)
def cached_status(_spark, data_root: str) -> pd.DataFrame:
    """Return the dataset status, refreshed at most once a minute."""
    return views.data_status(_spark, DataPaths(Path(data_root)))


st.title("Spain electricity: demand, generation mix and price")
st.caption("Red Eléctrica · Open-Meteo · MIBGAS — processed with Spark, shown with Streamlit")
status_tab, history_tab, live_tab, analysis_tab = st.tabs(["Data status", "History", "Live", "Analysis"])
status = cached_status(spark, root)
hourly_row = status.set_index("dataset").loc["hourly_features"]
has_history = int(hourly_row["rows"]) > 0

with status_tab:
    if int(status["rows"].sum()) == 0:
        st.info(NO_DATA)
    st.dataframe(status, hide_index=True, width="stretch")
    st.caption(f"Data folder: {root}")


def render_history(start, end) -> None:
    """Render the History charts for a local date range."""
    history = views.hourly_history(spark, paths, start, end)
    demand = long_format(
        history, "hour_utc", {"actual_demand_mw": "Actual demand", "forecast_demand_mw": "REE forecast"}, "mw"
    )
    st.altair_chart(
        line_chart(
            demand,
            "hour_utc",
            "mw",
            "series",
            "Demand vs REE forecast",
            "MW",
            ["Actual demand", "REE forecast"],
        ),
        width="stretch",
    )
    mix = views.mix_history(spark, paths, start, end)
    mix_chart = (
        alt.Chart(mix, title="Generation mix (hourly mean)")
        .mark_area()
        .encode(
            x=alt.X("hour_utc:T", title="Time (UTC)"),
            y=alt.Y("mw:Q", stack=True, title="MW"),
            color=alt.Color("technology:N", scale=color_scale(MIX_ORDER), sort=MIX_ORDER, title="Technology"),
            order=alt.Order("technology_rank:Q"),
            tooltip=["hour_utc:T", "technology:N", alt.Tooltip("mw:Q", format=",.0f")],
        )
        .transform_calculate(technology_rank=f"indexof({MIX_ORDER}, datum.technology)")
        .properties(height=300)
    )
    st.altair_chart(mix_chart, width="stretch")
    prices = long_format(
        history, "hour_utc", {"spot_price_eur_mwh": "Spot market", "pvpc_eur_mwh": "PVPC"}, "eur_mwh"
    )
    st.altair_chart(
        line_chart(
            prices, "hour_utc", "eur_mwh", "series", "Electricity price", "€/MWh", ["Spot market", "PVPC"]
        ),
        width="stretch",
    )
    with st.expander("Show hourly data"):
        st.dataframe(history, hide_index=True, width="stretch")


with history_tab:
    if not has_history:
        st.info(NO_DATA)
    else:
        last_day = pd.Timestamp(hourly_row["last"]).date()
        first_day = pd.Timestamp(hourly_row["first"]).date()
        selection = st.date_input(
            "Local dates",
            value=(max(first_day, last_day - timedelta(days=6)), last_day),
            min_value=first_day,
            max_value=last_day,
        )
        if not isinstance(selection, tuple) or len(selection) < 2:
            st.info("Pick an end date to show the range.")
        else:
            render_history(*selection)


@st.fragment(run_every=timedelta(seconds=30))
def live_panel() -> None:
    """Render the streaming output; re-runs every 30 seconds."""
    readings = views.live_readings(spark, paths)
    if readings.empty:
        st.info(NO_STREAM)
        return
    now = datetime.now(UTC)
    mode = views.stream_mode(paths)
    latest = readings.iloc[-1]
    first, second, third = st.columns(3)
    first.metric("Latest reading (UTC)", pd.Timestamp(latest.ts_utc).strftime("%Y-%m-%d %H:%M"))
    second.metric("Demand", f"{latest.demand_mw:,.0f} MW")
    third.metric("Renewable share", f"{latest.renewable_share:.0%}")
    warning = views.stale_warning(paths, readings, now)
    if warning:
        st.warning(warning)
    elif mode == "replay":
        st.caption("Replaying downloaded history: event times are in the past by design.")
    else:
        age = views.minutes_since(latest.ts_utc, now)
        st.caption(f"Newest reading is {age:.0f} min old" if age is not None else "")
    demand = readings.assign(series="Demand")
    st.altair_chart(
        line_chart(demand, "ts_utc", "demand_mw", "series", "Streamed 5-minute demand", "MW", ["Demand"]),
        width="stretch",
    )
    st.dataframe(views.live_windows(spark, paths).head(24), hide_index=True, width="stretch")


with live_tab:
    live_panel()

with analysis_tab:
    if not has_history:
        st.info(NO_DATA)
    else:
        daily = views.daily_demand_vs_temperature(spark, paths).dropna()
        daily["day_type"] = daily["is_weekend"].map({True: "Weekend", False: "Weekday"})
        scatter = (
            alt.Chart(daily, title="Daily demand vs national temperature")
            .mark_circle(size=64, opacity=0.85)
            .encode(
                x=alt.X("temperature_c:Q", title="Mean temperature (°C)"),
                y=alt.Y("demand_mw:Q", title="Mean demand (MW)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "day_type:N",
                    scale=color_scale(["Weekday", "Weekend"]),
                    title=None,
                    legend=alt.Legend(orient="top"),
                ),
                tooltip=[
                    "local_date:T",
                    alt.Tooltip("temperature_c:Q", format=".1f"),
                    alt.Tooltip("demand_mw:Q", format=",.0f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(scatter, width="stretch")
        matrix = views.correlation_matrix(spark, paths, CORRELATION_COLUMNS)
        if matrix.empty:
            st.info("Not enough complete hours for a correlation matrix yet.")
        else:
            cells = matrix.reset_index(names="row").melt(
                id_vars="row", var_name="column", value_name="pearson_r"
            )
            heatmap = (
                alt.Chart(cells, title="Pearson correlation (hourly)")
                .mark_rect()
                .encode(
                    x=alt.X("column:N", sort=CORRELATION_COLUMNS, title=None),
                    y=alt.Y("row:N", sort=CORRELATION_COLUMNS, title=None),
                    color=alt.Color(
                        "pearson_r:Q",
                        scale=alt.Scale(domain=[-1, 0, 1], range=["#e34948", "#f0efec", "#2a78d6"]),
                        title="Pearson r",
                    ),
                    tooltip=["row:N", "column:N", alt.Tooltip("pearson_r:Q", format=".2f")],
                )
                .properties(height=360)
            )
            st.altair_chart(heatmap, width="stretch")
            with st.expander("Show correlation table"):
                st.dataframe(matrix.round(2), width="stretch")
