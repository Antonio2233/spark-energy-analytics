"""Streamlit UI over the Spark views: data status, history, live stream, first analysis and a data guide.

Run with:
    uv run streamlit run app/streamlit_app.py
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from spain_electricity import catalog, charts, views
from spain_electricity.config import DataPaths
from spain_electricity.spark_session import get_spark

NO_DATA = "No data yet: run scripts/download_history.py and scripts/ingest.py"
NO_STREAM = (
    "No streaming output yet: run scripts/run_stream.py together with "
    "scripts/replay_history.py (replay) or scripts/capture_realtime.py (live)"
)
CORRELATION_COLUMNS = list(charts.VARIABLE_LABELS)
TIME_FORMAT = "ddd DD MMM YYYY, HH:mm"
EXPLAIN = "What these series are and where they come from"

st.set_page_config(page_title="Spain electricity · Spark", layout="wide")


@st.cache_resource
def spark_session():
    """Return the app's SparkSession, created once per server process."""
    return get_spark("streamlit", "local[2]", 4)


def chart_theme() -> charts.ChartTheme:
    """Return the chart colors of the active Streamlit theme."""
    theme = getattr(getattr(st.context, "theme", None), "type", "light")
    return charts.theme_for(dark=theme == "dark")


def explain(keys: list[str], note: str | None = None) -> None:
    """Show, folded under a chart, what each of its series means and its linked source."""
    with st.expander(EXPLAIN):
        st.markdown(catalog.markdown(catalog.entry(key) for key in keys))
        if note:
            st.markdown(note)


def column_name(key: str) -> str:
    """Return a table header with the series name and its unit."""
    entry = catalog.entry(key)
    return f"{entry.label} ({entry.unit})" if entry.unit else entry.label


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


@st.cache_data(ttl=timedelta(minutes=1), show_spinner=False)
def cached_attributes(_spark, folder: str) -> pd.DataFrame:
    """Return a dataset's columns with their Spark type, unit and meaning."""
    attributes = views.dataset_attributes(_spark, Path(folder))
    described = [catalog.attribute(column) for column in attributes["column"]]
    return attributes.assign(
        unit=[unit for unit, _ in described], description=[description for _, description in described]
    )


def dataset_folders(data_paths: DataPaths) -> dict[str, Path]:
    """Return the processed datasets and the streaming outputs, by display name."""
    folders = {name: data_paths.processed / name for name in views.DATASETS}
    folders["stream readings (5-minute)"] = data_paths.stream_output / "readings"
    folders["stream windows (15-minute)"] = data_paths.stream_output / "windows"
    return folders


st.title("Spain electricity: demand, generation mix and price")
st.caption("Red Eléctrica · Open-Meteo · MIBGAS — processed with Spark, shown with Streamlit. Times are UTC.")
status_tab, history_tab, live_tab, analysis_tab, about_tab = st.tabs(
    ["Data status", "History", "Live", "Analysis", "About the data"]
)
status = cached_status(spark, root)
hourly_row = status.set_index("dataset").loc["hourly_features"]
has_history = int(hourly_row["rows"]) > 0

with status_tab:
    st.caption("What Spark has ingested into data/processed: one Parquet dataset per source.")
    if int(status["rows"].sum()) == 0:
        st.info(NO_DATA)
    status = status.assign(
        content=status["dataset"].map(lambda name: catalog.DATASETS[name][0]),
        source=status["dataset"].map(lambda name: catalog.DATASETS[name][1].name),
        link=status["dataset"].map(lambda name: catalog.DATASETS[name][1].url),
    )
    st.dataframe(
        status[["dataset", "content", "source", "link", "rows", "first", "last"]],
        hide_index=True,
        width="stretch",
        column_config={
            "dataset": "Dataset",
            "content": "Content",
            "source": "Source",
            "link": st.column_config.LinkColumn("Link"),
            "rows": st.column_config.NumberColumn("Rows", format="localized"),
            "first": st.column_config.DatetimeColumn("First (UTC)", format=TIME_FORMAT),
            "last": st.column_config.DatetimeColumn("Last (UTC)", format=TIME_FORMAT),
        },
    )
    st.caption(f"Data folder: {root}")
    st.subheader("Attributes of each dataset")
    st.caption("The columns Spark finds in each Parquet dataset, with their type, unit and meaning.")
    for name, folder in dataset_folders(paths).items():
        with st.expander(name):
            attributes = cached_attributes(spark, str(folder))
            if attributes.empty:
                st.info("Not written yet.")
            else:
                st.dataframe(
                    attributes,
                    hide_index=True,
                    width="stretch",
                    column_config={
                        "column": "Attribute",
                        "type": "Spark type",
                        "unit": "Unit",
                        "description": st.column_config.TextColumn("What it is", width="large"),
                    },
                )


def render_history(start, end) -> None:
    """Render the History charts for a local date range."""
    theme = chart_theme()
    history = views.hourly_history(spark, paths, start, end)
    demand_series = {key: catalog.label(key) for key in ("actual_demand_mw", "forecast_demand_mw")}
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
    explain(list(demand_series))
    st.altair_chart(charts.mix_chart(views.mix_history(spark, paths, start, end), theme), width="stretch")
    explain(
        list(charts.TECHNOLOGY_LABELS),
        "Values are the hourly mean power of each technology on the peninsular system, in MW.",
    )
    price_series = {key: catalog.label(key) for key in ("spot_price_eur_mwh", "pvpc_eur_mwh")}
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
    explain(list(price_series))
    with st.expander("Show hourly data"):
        st.dataframe(
            history.round({"demand_mw": 0, "actual_demand_mw": 0, "forecast_demand_mw": 0}),
            hide_index=True,
            width="stretch",
            column_config={
                "hour_utc": st.column_config.DatetimeColumn("Hour (UTC)", format=TIME_FORMAT),
                "demand_mw": st.column_config.NumberColumn(column_name("demand_mw"), format="localized"),
                "actual_demand_mw": st.column_config.NumberColumn(
                    column_name("actual_demand_mw"), format="localized"
                ),
                "forecast_demand_mw": st.column_config.NumberColumn(
                    column_name("forecast_demand_mw"), format="localized"
                ),
                "renewable_share": st.column_config.NumberColumn(
                    catalog.label("renewable_share"), format="percent"
                ),
                "spot_price_eur_mwh": st.column_config.NumberColumn(
                    column_name("spot_price_eur_mwh"), format="%.2f"
                ),
                "pvpc_eur_mwh": st.column_config.NumberColumn(column_name("pvpc_eur_mwh"), format="%.2f"),
                "temperature_c": st.column_config.NumberColumn(column_name("temperature_c"), format="%.1f"),
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
            readings.assign(series=catalog.label("demand_mw")),
            x="ts_utc",
            y="demand_mw",
            title="Streamed demand, every 5 minutes",
            subtitle="The last 6 hours that Spark Structured Streaming has processed",
            y_title="MW",
            domain=[catalog.label("demand_mw")],
            theme=chart_theme(),
            time_format=charts.HOUR_FORMAT,
        ),
        width="stretch",
    )
    explain(
        ["demand_mw", "renewable_share"],
        "The **15-minute windows** below group the readings of each quarter of an hour: the mean and "
        "the peak demand, the mean renewable share, and how many readings arrived (3 when complete).",
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
        explain(
            ["demand_mw", "temperature_c", "day_type"],
            "Each dot is the mean of one local day. The trend is a parabola fitted to each group of days.",
        )
        matrix = views.correlation_matrix(spark, paths, CORRELATION_COLUMNS)
        if matrix.empty:
            st.info("Not enough complete hours for a correlation matrix yet.")
        else:
            st.altair_chart(charts.correlation_heatmap(matrix, theme), width="stretch")
            explain(
                CORRELATION_COLUMNS,
                "Pearson r is computed over every hour that has all seven values, with Spark ML's "
                "`Correlation`. It measures a linear relation, not a cause.",
            )
            with st.expander("Show correlation table"):
                labels = [charts.VARIABLE_LABELS[column] for column in CORRELATION_COLUMNS]
                st.dataframe(
                    matrix.set_axis(labels, axis=0).set_axis(labels, axis=1).round(2), width="stretch"
                )

with about_tab:
    st.caption(
        "What every series in this app means, its unit, and where it comes from. Times are UTC. Power "
        "values (MW) are means: an hourly value is the mean of its twelve 5-minute readings."
    )
    st.subheader("Variables")
    st.markdown(catalog.markdown(catalog.VARIABLES))
    st.subheader("Generation technologies")
    st.markdown(catalog.markdown(catalog.TECHNOLOGIES))
    st.subheader("Sources")
    st.dataframe(
        pd.DataFrame(
            [(source.name, source.endpoint, source.url) for source in catalog.SOURCES],
            columns=["source", "endpoint", "link"],
        ),
        hide_index=True,
        width="stretch",
        column_config={
            "source": "Source",
            "endpoint": "What the project calls",
            "link": st.column_config.LinkColumn("Link"),
        },
    )
