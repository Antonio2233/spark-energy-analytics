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


def _render_demand(spark: SparkSession, paths: DataPaths, theme: charts.ChartTheme) -> None:
    """Show sample coverage, model selection and chronological holdout results."""
    metadata = models.load_metadata(paths)
    if metadata is None:
        st.info(
            "No trained demand model yet. Download and ingest the history, then run scripts/train_demand.py."
        )
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
        "Fit 2021–2024 → select parameters on 2025 MAE → refit the winner on 2021–2025 → evaluate it "
        "once on 2026. The 28 April 2025 blackout is excluded. The model and both baselines (REE's "
        "peninsular forecast and the same hour the day before) use identical hours."
    )
    records = []
    for split, result in metadata["evaluation"].items():
        # Older metrics files have no same-hour-yesterday baseline: show the sources each file has.
        for source in ("model", *models.BASELINES):
            if source in result:
                records.append({"split": split, "source": source, "hours": result["hours"], **result[source]})
    st.dataframe(pd.DataFrame(records).round(3), hide_index=True, width="stretch")
    st.caption(
        "MAE, RMSE and bias (actual minus estimate) are in MW; lower MAE and RMSE are better. "
        "R² can be negative. "
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
    month = st.selectbox("Evaluation month", sorted(months.unique()), index=len(months.unique()) - 1)
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
    if metadata.get("alert_rate"):
        rates = metadata["alert_rate"]
        st.caption(
            f"Hours over the threshold: {rates['validation']:.1%} in validation (2025), "
            f"{rates['test']:.1%} in test (2026)."
        )


def render_models(spark: SparkSession, paths: DataPaths, theme: charts.ChartTheme) -> None:
    """Present saved demand, price, renewables and clustering results independently."""
    import json

    _render_demand(spark, paths, theme)
    demand = models.load_metadata(paths)
    if demand and demand.get("features"):
        st.altair_chart(charts.importance_chart(pd.DataFrame(demand["features"]), theme), width="stretch")
        st.caption("Demand weights are tree importances or standardized coefficients, not causal effects.")
    for name in ("price", "renewable_share"):
        folder = paths.models / name
        report_path = folder / "metrics.json"
        st.subheader("Electricity price" if name == "price" else "Renewable share of demand")
        if not report_path.exists():
            st.info(f"No saved {name} model. Run scripts/train_models.py --models {name}.")
            continue
        report = json.loads(report_path.read_text(encoding="utf-8"))
        st.caption(report["interpretation"])
        st.write("Selected model:", report["selected"], "Complete hours:", report["rows"])
        st.caption(f"MAE and RMSE unit: {report['unit']}. All comparisons use identical hours.")
        records = [{"split": "validation", "source": item["model"], **item} for item in report["candidates"]]
        records += [{"split": "validation", "source": "t-24h baseline", **report["validation_baseline"]}]
        records += [
            {"split": "test", "source": source, **report["test"][source]} for source in ("model", "baseline")
        ]
        st.dataframe(pd.DataFrame(records), hide_index=True, width="stretch")
        st.altair_chart(charts.importance_chart(pd.DataFrame(report["features"]), theme), width="stretch")
        st.caption(
            f"Weights: {report['weight_kind']}. "
            "Correlated inputs may share importance; these are not causal effects."
        )
        from spain_electricity.storage import has_parquet

        if has_parquet(folder / "predictions"):
            frame = (
                spark.read.parquet(str(folder / "predictions")).orderBy("hour_utc").limit(10000).toPandas()
            )
            months = pd.to_datetime(frame.local_date).dt.strftime("%Y-%m")
            month = st.selectbox("Test month", sorted(months.unique()), key=f"{name}_month")
            frame = frame[months == month].head(168)
            frame["segment"] = (
                pd.to_datetime(frame.hour_utc).astype("datetime64[ns]").astype("int64").diff()
                > 3_600_000_000_000
            ).cumsum()
            series = {
                report["label"]: "Observed",
                "prediction": "Model",
                report["baseline"]: "t-24h baseline",
            }
            long = (
                frame[["hour_utc", "segment", *series]]
                .rename(columns=series)
                .melt(id_vars=["hour_utc", "segment"], var_name="series", value_name="value")
            )
            st.altair_chart(
                charts.line_chart(
                    long,
                    x="hour_utc",
                    y="value",
                    title="Held-out test observations",
                    y_title=report["unit"],
                    value_format=".2f",
                    domain=list(series.values()),
                    theme=theme,
                ).encode(detail="segment:N"),
                width="stretch",
            )
            st.caption(
                "Up to 168 saved test hours, ordered by UTC. Missing periods are gaps in sample coverage."
            )
    path = paths.models / "day_types" / "metrics.json"
    st.subheader("Daily demand types with KMeans")
    if not path.exists():
        st.info("No saved day types. Run scripts/train_models.py --models day_types.")
        return
    report = json.loads(path.read_text(encoding="utf-8"))
    st.caption(report["interpretation"])
    st.write(
        "Selected k:",
        report["chosen_k"],
        "Training days:",
        report["training_days"],
        "Assigned days:",
        report["assigned_days"],
    )
    st.dataframe(
        pd.DataFrame([{"k": k, "silhouette": value} for k, value in report["silhouettes"].items()]),
        hide_index=True,
    )
    summary = pd.DataFrame(report["summary"])
    st.altair_chart(charts.profiles_chart(summary, theme), width="stretch")
    st.dataframe(summary.drop(columns="mean_profile"), hide_index=True, width="stretch")
    with st.expander("Model inputs and sources"):
        st.markdown(
            catalog.markdown(
                catalog.entry(key)
                for key in (
                    "spot_price_eur_mwh",
                    "gas_price_eur_mwh",
                    "renewable_share",
                    "demand_mw",
                    "temperature_c",
                )
            )
        )
