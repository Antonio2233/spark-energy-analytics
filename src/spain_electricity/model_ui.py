"""Models tab: each regression's saved results in the chosen version, then the KMeans day types."""

from collections.abc import Iterable
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from spain_electricity import catalog, charts, model_results, views
from spain_electricity.config import DataPaths
from spain_electricity.models import estimate_column

TITLES = {
    "demand": "Hourly peninsular demand",
    "price": "Electricity price",
    "renewable_share": "Renewable share of demand",
}
FAMILIES = {"linear": "Linear regression", "random_forest": "Random Forest", "gbt": "Gradient-boosted trees"}
SOURCES = {
    "model": "Spark ML",
    "model_through_2025": "Spark ML, trained through 2025",
    "ree": "REE peninsular forecast",
    "same_hour_yesterday": "Same hour yesterday",
}
INPUTS = (
    "demand_mw",
    "spot_price_eur_mwh",
    "renewable_share",
    "temperature_c",
    "gas_price_eur_mwh",
    "wind_speed_100m_kmh",
    "shortwave_radiation_wm2",
)
HOUR_NANOSECONDS = 3_600_000_000_000
VERSION_HELP = (
    "Both versions use the configuration selected on 2025. The 2021–2025 model is the one saved for "
    "streaming and is evaluated once on all of 2026. The update adds 2026 to the training data and is "
    "evaluated, beside the 2021–2025 model, on the same hours after its training period."
)


def _month(day: str) -> str:
    """Format an ISO date as its month, e.g. 'October 2026'."""
    return date.fromisoformat(day).strftime("%B %Y")


def _period(first: str, last: str) -> str:
    """Format an inclusive date range compactly, e.g. '1–23 November 2026'."""
    start, end = date.fromisoformat(first), date.fromisoformat(last)
    if (start.year, start.month) == (end.year, end.month):
        return f"{start.day}–{end.day} {end:%B %Y}"
    return f"{start:%d %B %Y} – {end:%d %B %Y}"


def _parameters(parameters: dict) -> str:
    """Format a candidate's hyperparameters, e.g. 'maxIter 120, maxDepth 6'."""
    return ", ".join(f"{key} {value:g}" for key, value in parameters.items())


def version_labels(reports: dict) -> dict[str, str]:
    """Name the 2021-2025 model and, when one was saved, the update after their measured periods."""
    labels = {"main": "Trained 2021–2025, tested on 2026"}
    updates = [reports[name].get("update") for name in model_results.REGRESSIONS if name in reports]
    update = next((item for item in updates if item), None)
    if update:
        tested = _period(update["evaluated_from"], update["evaluated_until"])
        labels["update"] = f"Trained through {_month(update['train_until'])}, tested on {tested}"
    return labels


def series_labels(report: dict, version: str) -> dict[str, str]:
    """Map the charted columns to catalog names: observed, estimate(s) and the first baseline."""
    label = report["label"]
    estimate = estimate_column(label)
    names = {label: catalog.label(label), estimate: catalog.label(estimate)}
    if version == "update":
        names[estimate] += f", through {_month(report['update']['train_until'])}"
        previous = estimate_column(label, previous=True)
        names[previous] = catalog.label(previous)
    baseline = next(iter(report["baselines"].values()))
    names[baseline] = catalog.label(baseline)
    return names


def _scores_table(name: str, report: dict, scored: Iterable[tuple[str, dict]]) -> None:
    """Show model and baseline errors per period, each scored on identical paired hours."""
    rows = [
        {"period": period, **row}
        for period, scores in scored
        for row in model_results.score_rows(name, report, scores)
    ]
    table = pd.DataFrame(rows).drop(columns=["target", "unit"])
    table["source"] = table["source"].map(lambda source: SOURCES.get(source, source))
    digits = 0 if report["unit"] == "MW" else 3
    st.dataframe(table.round({"mae": digits, "rmse": digits, "r2": 3, "bias": digits}), hide_index=True)
    st.caption(
        f"MAE, RMSE and bias (actual minus estimate) are in {report['unit']}; lower MAE and RMSE are better. "
        "R² can be negative. The estimates use contemporaneous inputs and are not day-ahead forecasts."
    )


def _comparison_chart(frame: pd.DataFrame, report: dict, version: str, theme: charts.ChartTheme):
    """Draw observed values, the estimate(s) and the first baseline, breaking lines across gaps."""
    names = series_labels(report, version)
    # Nanoseconds since the epoch: pd.Timedelta warns about NumPy's deprecated generic unit here.
    nanoseconds = pd.to_datetime(frame.hour_utc).astype("datetime64[ns]").astype("int64")
    frame = frame.assign(segment=(nanoseconds.diff() > HOUR_NANOSECONDS).cumsum())
    long = (
        frame[["hour_utc", "segment", *names]]
        .rename(columns=names)
        .melt(id_vars=["hour_utc", "segment"], var_name="series", value_name="value")
        .dropna(subset=["value"])
    )
    return charts.line_chart(
        long,
        x="hour_utc",
        y="value",
        title="Observed and estimated",
        y_title=report["unit"],
        value_format=",.0f" if report["unit"] == "MW" else ".2f",
        domain=list(names.values()),
        theme=theme,
    ).encode(detail="segment:N")


def _predictions_panel(
    spark: SparkSession, folder: Path, name: str, report: dict, version: str, theme: charts.ChartTheme
) -> None:
    """Chart one month of saved predictions and list the hours with the largest errors."""
    splits = ["update"] if version == "update" else ["test", "validation"]
    split = splits[0] if len(splits) == 1 else st.selectbox("Evaluation split", splits, key=f"{name}_split")
    months = views.prediction_months(spark, folder, split)
    if not months:
        st.info("No saved evaluation predictions.")
        return
    month = st.selectbox("Evaluation month", months, index=len(months) - 1, key=f"{name}_{version}_month")
    frame = views.saved_predictions(spark, folder, split, month)
    st.altair_chart(_comparison_chart(frame, report, version, theme), width="stretch")
    st.caption(
        "Hours where the model and its baselines can all be scored, in UTC. Validation estimates come "
        "from the 2021–2024 fit that was selected; test and update estimates from the saved models."
    )
    with st.expander("Hours with the largest errors"):
        errors = views.largest_errors(spark, folder, report["label"], estimate_column(report["label"]), split)
        st.dataframe(errors, hide_index=True, width="stretch")
        st.caption(
            "Check holidays, unusual weather and changed patterns before attributing an error to a cause."
        )


def _weights(features: list[dict], report: dict, theme: charts.ChartTheme) -> None:
    """Chart the named feature weights of the shown model."""
    st.altair_chart(charts.importance_chart(pd.DataFrame(features), theme), width="stretch")
    st.caption(
        f"Weights: {report['weight_kind']}. "
        "Correlated inputs may share importance; these are not causal effects."
    )


def _render_main(spark: SparkSession, paths: DataPaths, name: str, report: dict, theme) -> None:
    """Show the 2021-2025 model: selection, 2025 and 2026 scores, predictions, alerts and weights."""
    _scores_table(name, report, report["evaluation"].items())
    with st.expander("Validation candidates and data coverage"):
        candidates = [
            {
                "model": FAMILIES.get(item["model"], item["model"]),
                "parameters": _parameters(item["parameters"]),
                **{metric: item[metric] for metric in ("mae", "rmse", "r2", "bias") if metric in item},
            }
            for item in report["candidates"]
        ]
        st.dataframe(pd.DataFrame(candidates), hide_index=True, width="stretch")
        st.caption("Each candidate is fitted on 2021–2024 and scored on 2025; the lowest MAE is selected.")
        if report.get("audit"):
            st.dataframe(pd.DataFrame(report["audit"]["by_year"]), hide_index=True, width="stretch")
        hours = report["split_hours"]
        st.write(
            f"Usable hours: {hours['train']:,} for training (2021–2024), {hours['validation']:,} paired in "
            f"2025 and {hours['test']:,} paired in 2026. The winner is refit on {report['refit']['hours']:,} "
            f"hours of {report['refit']['years']}."
        )
    _predictions_panel(spark, paths.models / name / "predictions", name, report, "main", theme)
    if "alert_threshold_mw" in report:
        rates = report["alert_rate"]
        st.write(
            f"Monitoring threshold: **{report['alert_threshold_mw']:,.0f} MW**, the 99th percentile of the "
            "absolute 2025 errors of the 2021–2024 fit. It is a screening rule, not a confidence interval."
        )
        st.caption(
            f"Hours over the threshold: {rates['validation']:.1%} in 2025 and {rates['test']:.1%} in 2026."
        )
    _weights(report["features"], report, theme)


def _render_update(spark: SparkSession, paths: DataPaths, name: str, report: dict, theme) -> None:
    """Show the update beside the 2021-2025 model on the same hours after the update's training period."""
    update = report.get("update")
    if not update:
        st.info(
            "This model has no update: no paired hours after its cutoff. "
            "Retrain with scripts/train_models.py."
        )
        return
    tested = _period(update["evaluated_from"], update["evaluated_until"])
    _scores_table(name, report, [(tested, update["evaluation"])])
    st.caption(
        f"The update is the same configuration refit on every valid hour through {update['train_until']} "
        f"({update['training_hours']:,} hours). Spark ML, trained through 2025, is the saved 2021–2025 model "
        "on the same hours."
    )
    if update.get("alert_rate"):
        rates = update["alert_rate"]
        st.caption(
            f"Hours over the 2025 alert threshold: {rates['model']:.1%} with the update and "
            f"{rates['model_through_2025']:.1%} with the 2021–2025 model."
        )
    _predictions_panel(spark, paths.models / name / "update" / "predictions", name, report, "update", theme)
    _weights(update["features"], report, theme)


def _render_regression(
    spark: SparkSession, paths: DataPaths, name: str, report: dict | None, version: str, theme
) -> None:
    """Show one regression model in the chosen version."""
    st.subheader(TITLES[name])
    if report is None:
        st.info(f"No saved {name} model. Run scripts/train_models.py --models {name}.")
        return
    if "baselines" not in report:
        st.info(f"The saved {name} report uses an older format. Run scripts/train_models.py --models {name}.")
        return
    st.caption(report["interpretation"])
    if report.get("sample"):
        st.warning("Small seasonal sample: these results describe sampled dates, not entire years.")
    selected = report["selected"]
    st.write(
        f"Selected on 2025: **{FAMILIES.get(selected['model'], selected['model'])}** "
        f"({_parameters(selected['parameters'])}) · "
        f"training and evaluation: {report['training_seconds']:.0f} s"
    )
    if version == "update":
        _render_update(spark, paths, name, report, theme)
    else:
        _render_main(spark, paths, name, report, theme)


def _render_day_types(report: dict | None, theme: charts.ChartTheme) -> None:
    """Show the KMeans silhouettes, the mean profile of each day type and its descriptors."""
    st.subheader("Daily demand types with KMeans")
    if report is None:
        st.info("No saved day types. Run scripts/train_models.py --models day_types.")
        return
    st.caption(report["interpretation"])
    best = report["silhouettes"][str(report["chosen_k"])]
    st.write(
        f"Selected k: **{report['chosen_k']}** (best training silhouette, {best:.3f}) · "
        f"training days: {report['training_days']:,} · assigned days: {report['assigned_days']:,}"
    )
    summary = pd.DataFrame(report["summary"])
    st.altair_chart(charts.profiles_chart(summary, theme), width="stretch")
    st.dataframe(summary.drop(columns="mean_profile").round(3), hide_index=True, width="stretch")
    st.caption(
        "Names are an inference: a group whose days are mostly weekends or holidays is a day off, and the "
        "others are working days, called warm or cool by their mean temperature. KMeans only sees the shape."
    )
    with st.expander("Silhouette by k"):
        silhouettes = [{"k": k, "silhouette": value} for k, value in report["silhouettes"].items()]
        st.dataframe(pd.DataFrame(silhouettes), hide_index=True)


def render_models(spark: SparkSession, paths: DataPaths, theme: charts.ChartTheme) -> None:
    """Present the saved demand, price, renewable-share and day-type results."""
    reports = model_results.saved_reports(paths)
    labels = version_labels(reports)
    version = st.radio(
        "Model version", list(labels), format_func=labels.get, horizontal=True, key="model_version"
    )
    st.caption(VERSION_HELP)
    for name in TITLES:
        _render_regression(spark, paths, name, reports.get(name), version, theme)
    _render_day_types(reports.get("day_types"), theme)
    with st.expander("Model inputs and sources"):
        st.markdown(catalog.markdown(catalog.entry(key) for key in INPUTS))
