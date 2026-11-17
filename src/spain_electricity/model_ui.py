"""Demand model results and monitoring panels, rendered only when called by the app."""

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from spain_electricity import catalog, charts, models, views
from spain_electricity.config import DataPaths


def _comparison(frame: pd.DataFrame, actual: str, theme: charts.ChartTheme, include_ree: bool = False):
    series = {actual: catalog.label("demand_mw"), "expected_demand_mw": catalog.label("expected_demand_mw")}
    if include_ree:
        series[models.BASELINE] = catalog.label(models.BASELINE)
    long = (
        frame[["hour_utc", *series]]
        .rename(columns=series)
        .melt(id_vars="hour_utc", var_name="series", value_name="mw")
        .dropna(subset=["mw"])
    )
    return charts.line_chart(
        long,
        x="hour_utc",
        y="mw",
        title="Demand: observed and expected",
        y_title="MW",
        domain=list(series.values()),
        theme=theme,
        subtitle="The ML estimate uses weather observed during the hour; REE is an operational forecast",
    )


def render_models(spark: SparkSession, paths: DataPaths, theme: charts.ChartTheme) -> None:
    """Show sample coverage, model selection and chronological holdout results."""
    metadata = models.load_metadata(paths)
    if metadata is None:
        st.info("No trained demand model yet. Prepare the demand sample, then run scripts/train_demand.py.")
        return
    st.subheader("Hourly peninsular demand")
    st.caption(metadata["interpretation"])
    if metadata.get("sample"):
        st.warning(
            "Small seasonal sample: these results describe sampled dates, not entire years. "
            "Other months, regional holidays and extreme events may behave differently."
        )
    st.write(
        f"Selected model: **{metadata['selected']['model']}** · "
        f"Training and evaluation: **{metadata['training_seconds']:.1f} s**"
    )
    st.caption(
        "Fit 2021–2024 → select parameters on 2025 MAE → evaluate the winner once on 2026. "
        "The 28 April 2025 blackout is excluded. Model and REE metrics use identical hours."
    )
    records = []
    for split, result in metadata["evaluation"].items():
        for source in ("model", "ree"):
            records.append({"split": split, "source": source, "hours": result["hours"], **result[source]})
    st.dataframe(pd.DataFrame(records).round(3), hide_index=True, width="stretch")
    st.caption(
        "MAE and RMSE are in MW; lower is better. R² can be negative. "
        "The model and REE have different information sets; this compares their errors, "
        "not equally informed day-ahead forecasts."
    )
    with st.expander("Validation candidates and data coverage"):
        st.dataframe(pd.DataFrame(metadata["candidates"]), hide_index=True, width="stretch")
        st.dataframe(pd.DataFrame(metadata["audit"]["by_year"]), hide_index=True, width="stretch")
        st.write("Usable hours by split:", metadata["split_hours"])
        if metadata.get("sample"):
            st.write("Requested days by year:", metadata["sample"]["days_by_year"])
    split = st.selectbox("Evaluation split", ["test", "validation"])
    predictions = views.model_predictions(spark, paths, split)
    if predictions.empty:
        st.info("No saved evaluation predictions.")
        return
    months = pd.to_datetime(predictions.hour_utc).dt.strftime("%Y-%m")
    month = st.selectbox("Sampled evaluation month", sorted(months.unique()), index=len(months.unique()) - 1)
    st.altair_chart(_comparison(predictions[months == month], "demand_mw", theme, True), width="stretch")
    st.caption(
        "Observed: REE generation feed. Baseline: pre from prevProgPeninsula. "
        "Spark ML: saved pipeline using five-city weighted temperature and calendar."
    )
    st.markdown("**Where the model has the largest errors**")
    st.dataframe(views.worst_model_errors(spark, paths, split), hide_index=True, width="stretch")
    st.caption(
        "Inspect holidays, unusual weather and changed daily patterns before attributing an error to a cause."
    )
    st.write(
        f"Monitoring threshold: **{metadata['alert_threshold_mw']:,.0f} MW** — "
        "99th percentile of absolute validation residuals. This screening rule is not a confidence interval."
    )
