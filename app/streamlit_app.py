"""Streamlit UI over the Spark views: data status and guide, history, live stream and first analysis.

Run with:
    uv run streamlit run app/streamlit_app.py
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from spain_electricity import catalog, charts, desk_ui, runtime_ui, views
from spain_electricity.config import DataPaths
from spain_electricity.model_ui import render_models
from spain_electricity.spark_session import get_spark

NO_DATA = "No data yet: run scripts/download_history.py and scripts/ingest.py"
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
    folders["stream hourly demand estimates"] = data_paths.stream_output / "hourly"
    return folders


desk_ui.apply_style()
with st.sidebar:
    st.html('<div class="desk-brand">IBERIA<span>.</span></div><div class="desk-eyebrow">ENERGY DESK</div>')
    st.caption("Peninsular power · market and system intelligence")
    st.divider()
    source_selection = runtime_ui.source_selector(spark, paths)
    st.divider()
    observation_hours = st.selectbox(
        "Observation window", [6, 12, 24, 48], index=2, format_func=lambda value: f"Last {value} hours"
    )
    efficiency = (
        st.slider(
            "Conversion efficiency (%)",
            40,
            65,
            55,
            help="Fuel-only scenario on the gas quote's energy basis; excludes CO₂ and operating costs.",
        )
        / 100
    )
    st.caption("The observation window follows the newest processed reading, including in replay mode.")
    st.divider()
    st.caption("REE · Open-Meteo · MIBGAS")
    st.caption("Spark ML + Structured Streaming")

live_tab, history_tab, analysis_tab, models_tab, status_tab = st.tabs(
    ["Live", "History", "Analysis", "Models", "Data status"]
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
    for name, folder in dataset_folders(source_selection.paths).items():
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
    st.subheader("About the data")
    st.caption(
        "What every series in this app means, its unit, and where it comes from. Power values (MW) are "
        "means: an hourly value is the mean of its twelve 5-minute readings."
    )
    with st.expander("Variables"):
        st.markdown(catalog.markdown(catalog.VARIABLES))
    with st.expander("Generation technologies"):
        st.markdown(catalog.markdown(catalog.TECHNOLOGIES))
    with st.expander("Sources"):
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


def render_history(start, end) -> None:
    """Render the History charts for a local date range."""
    theme = chart_theme()
    history = views.hourly_history(spark, paths, start, end)
    if history.empty:
        st.info("No observations in this interval. The compact dataset contains sampled seasonal blocks.")
        return
    demand_series = {
        key: catalog.label(key)
        for key in ("demand_mw", "peninsular_forecast_mw")
        if history[key].notna().any()
    }
    st.altair_chart(
        charts.line_chart(
            long_format(history, "hour_utc", demand_series, "mw"),
            x="hour_utc",
            y="mw",
            title="Peninsular demand & REE forecast",
            subtitle="Hourly means for the same peninsular system; forecast only where downloaded",
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
    price_series = {
        key: catalog.label(key)
        for key in ("spot_price_eur_mwh", "pvpc_eur_mwh")
        if history[key].notna().any()
    }
    if price_series:
        st.altair_chart(
            charts.line_chart(
                long_format(history, "hour_utc", price_series, "eur_mwh"),
                x="hour_utc",
                y="eur_mwh",
                title="Electricity delivery prices",
                subtitle="Hourly means, where market data is available",
                y_title="€/MWh",
                domain=list(price_series.values()),
                theme=theme,
                value_format=",.1f",
            ),
            width="stretch",
        )
        explain(list(price_series))
    else:
        st.caption("Price history is not downloaded for these dates; demand and generation remain available.")
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


@st.fragment(run_every=timedelta(seconds=10))
def live_panel() -> None:
    """Keep the selected source alive and refresh its isolated dashboard every ten seconds."""
    connection = runtime_ui.connection_status(source_selection)
    if source_selection.real_time and not connection.ready:
        return
    if (
        not source_selection.real_time
        and source_selection.paths.stream_name is None
        and source_selection.historical_day is None
    ):
        st.info(NO_DATA)
        return
    desk_ui.render_live(
        spark,
        source_selection.paths,
        observation_hours,
        efficiency,
        historical_day=source_selection.historical_day,
    )


with live_tab:
    live_panel()


with models_tab:
    render_models(spark, paths, chart_theme())

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
        available = views.available_correlation_columns(spark, paths, CORRELATION_COLUMNS)
        matrix = views.correlation_matrix(spark, paths, available) if len(available) >= 2 else pd.DataFrame()
        if matrix.empty:
            st.info("Not enough complete hours for a correlation matrix yet.")
        else:
            st.altair_chart(charts.correlation_heatmap(matrix, theme), width="stretch")
            explain(
                available,
                "Pearson r uses hours with all selected variables present, with Spark ML's "
                "`Correlation`. It measures a linear relation, not a cause.",
            )
            with st.expander("Show correlation table"):
                labels = [charts.VARIABLE_LABELS[column] for column in available]
                st.dataframe(
                    matrix.set_axis(labels, axis=0).set_axis(labels, axis=1).round(2), width="stretch"
                )
