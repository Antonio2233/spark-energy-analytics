"""Streamlit UI over the Spark views: data status, history, live stream and first analysis.

Run with:
    uv run streamlit run app/streamlit_app.py
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from spain_electricity import charts, views
from spain_electricity.config import DataPaths
from spain_electricity.spark_session import get_spark

NO_DATA = "No data yet: run scripts/download_history.py and scripts/ingest.py"
NO_STREAM = (
    "No streaming output yet: run scripts/run_stream.py together with "
    "scripts/replay_history.py (replay) or scripts/capture_realtime.py (live)"
)
DATASET_DESCRIPTIONS = {
    "generation": "REE · demand and generation mix, every 5 minutes",
    "demand_forecast": "REE REData · actual, forecast and scheduled demand",
    "market_price": "REE REData · day-ahead market price and PVPC",
    "weather": "Open-Meteo · hourly weather in five cities",
    "gas_price": "MIBGAS · daily natural gas price (day-ahead)",
    "hourly_features": "All of the above joined by hour: the input of the models",
}
CORRELATION_COLUMNS = list(charts.VARIABLE_LABELS)
TIME_FORMAT = "ddd DD MMM YYYY, HH:mm"

st.set_page_config(page_title="Spain electricity · Spark", layout="wide")


@st.cache_resource
def spark_session():
    """Return the app's SparkSession, created once per server process."""
    return get_spark("streamlit", "local[2]", 4)


def chart_theme() -> charts.ChartTheme:
    """Return the chart colors of the active Streamlit theme."""
    theme = getattr(getattr(st.context, "theme", None), "type", "light")
    return charts.theme_for(dark=theme == "dark")


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
st.caption("Red Eléctrica · Open-Meteo · MIBGAS — processed with Spark, shown with Streamlit. Times are UTC.")
status_tab, history_tab, live_tab, analysis_tab = st.tabs(["Data status", "History", "Live", "Analysis"])
status = cached_status(spark, root)
hourly_row = status.set_index("dataset").loc["hourly_features"]
has_history = int(hourly_row["rows"]) > 0

with status_tab:
    st.caption("What Spark has ingested into data/processed: one Parquet dataset per source.")
    if int(status["rows"].sum()) == 0:
        st.info(NO_DATA)
    st.dataframe(
        status.assign(source=status["dataset"].map(DATASET_DESCRIPTIONS))[
            ["dataset", "source", "rows", "first", "last"]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "dataset": "Dataset",
            "source": "Source and content",
            "rows": st.column_config.NumberColumn("Rows", format="localized"),
            "first": st.column_config.DatetimeColumn("First (UTC)", format=TIME_FORMAT),
            "last": st.column_config.DatetimeColumn("Last (UTC)", format=TIME_FORMAT),
        },
    )
    st.caption(f"Data folder: {root}")


def render_history(start, end) -> None:
    """Render the History charts for a local date range."""
    theme = chart_theme()
    history = views.hourly_history(spark, paths, start, end)
    demand_series = {"actual_demand_mw": "Actual demand", "forecast_demand_mw": "REE forecast"}
    st.altair_chart(
        charts.line_chart(
            long_format(history, "hour_utc", demand_series, "mw"),
            x="hour_utc",
            y="mw",
            title="Demand: actual vs REE forecast",
            subtitle="Hourly means of REE's REData series, so both measure the same demand",
            y_title="MW",
            domain=list(demand_series.values()),
            theme=theme,
        ),
        width="stretch",
    )
    st.altair_chart(charts.mix_chart(views.mix_history(spark, paths, start, end), theme), width="stretch")
    price_series = {"spot_price_eur_mwh": "Day-ahead market", "pvpc_eur_mwh": "PVPC (regulated tariff)"}
    st.altair_chart(
        charts.line_chart(
            long_format(history, "hour_utc", price_series, "eur_mwh"),
            x="hour_utc",
            y="eur_mwh",
            title="Electricity price",
            subtitle="Wholesale day-ahead price and the regulated consumer price, hourly means",
            y_title="€/MWh",
            domain=list(price_series.values()),
            theme=theme,
            value_format=",.1f",
        ),
        width="stretch",
    )
    with st.expander("Show hourly data"):
        st.dataframe(
            history.round({"demand_mw": 0, "actual_demand_mw": 0, "forecast_demand_mw": 0}),
            hide_index=True,
            width="stretch",
            column_config={
                "hour_utc": st.column_config.DatetimeColumn("Hour (UTC)", format=TIME_FORMAT),
                "demand_mw": st.column_config.NumberColumn("Demand, 5-min feed (MW)", format="localized"),
                "actual_demand_mw": st.column_config.NumberColumn("Demand, REData (MW)", format="localized"),
                "forecast_demand_mw": st.column_config.NumberColumn("REE forecast (MW)", format="localized"),
                "renewable_share": st.column_config.NumberColumn("Renewable share", format="percent"),
                "spot_price_eur_mwh": st.column_config.NumberColumn("Day-ahead (€/MWh)", format="%.2f"),
                "pvpc_eur_mwh": st.column_config.NumberColumn("PVPC (€/MWh)", format="%.2f"),
                "temperature_c": st.column_config.NumberColumn("Temperature (°C)", format="%.1f"),
            },
        )


with history_tab:
    st.caption("Hourly history from the batch pipeline (scripts/ingest.py). Pick a range of local dates.")
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
    first.metric("Latest reading (UTC)", pd.Timestamp(latest.ts_utc).strftime("%a %d %b %H:%M"))
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
    st.altair_chart(
        charts.line_chart(
            readings.assign(series="Demand"),
            x="ts_utc",
            y="demand_mw",
            title="Streamed demand, every 5 minutes",
            subtitle="The last 6 hours that Spark Structured Streaming has processed",
            y_title="MW",
            domain=["Demand"],
            theme=chart_theme(),
            time_format=charts.HOUR_FORMAT,
        ),
        width="stretch",
    )
    st.markdown("**15-minute windows** computed by the stream (newest first)")
    windows = views.live_windows(spark, paths).head(24)
    st.dataframe(
        windows.round({"avg_demand_mw": 0, "max_demand_mw": 0}),
        hide_index=True,
        width="stretch",
        column_config={
            "window_start": st.column_config.DatetimeColumn("Window start (UTC)", format=TIME_FORMAT),
            "avg_demand_mw": st.column_config.NumberColumn("Mean demand (MW)", format="localized"),
            "max_demand_mw": st.column_config.NumberColumn("Peak demand (MW)", format="localized"),
            "avg_renewable_share": st.column_config.NumberColumn("Renewable share", format="percent"),
            "n_readings": st.column_config.NumberColumn("Readings (of 3)"),
        },
    )


with live_tab:
    st.caption(
        "Output of scripts/run_stream.py: Spark reads each file that lands in data/stream/landing "
        "and updates these results."
    )
    live_panel()

with analysis_tab:
    st.caption("First look at what drives demand and price, over the whole hourly table.")
    if not has_history:
        st.info(NO_DATA)
    else:
        theme = chart_theme()
        daily = views.daily_demand_vs_temperature(spark, paths).dropna()
        st.altair_chart(charts.demand_temperature_scatter(daily, theme), width="stretch")
        matrix = views.correlation_matrix(spark, paths, CORRELATION_COLUMNS)
        if matrix.empty:
            st.info("Not enough complete hours for a correlation matrix yet.")
        else:
            st.altair_chart(charts.correlation_heatmap(matrix, theme), width="stretch")
            with st.expander("Show correlation table"):
                labels = [charts.VARIABLE_LABELS[column] for column in CORRELATION_COLUMNS]
                st.dataframe(
                    matrix.set_axis(labels, axis=0).set_axis(labels, axis=1).round(2), width="stretch"
                )
