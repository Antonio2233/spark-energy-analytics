"""Saved model reports and the comparison tables shared by the Models tab and the notebook."""

import json

import pandas as pd

from spain_electricity.config import DataPaths

MODELS = ("demand", "price", "renewable_share", "day_types")
# Reports with an hourly target, an estimate and baselines; day types are clusters, not estimates.
REGRESSIONS = ("demand", "price", "renewable_share")
COLUMNS = ["target", "source", "hours", "unit", "mae", "rmse", "r2", "bias"]


def saved_reports(paths: DataPaths) -> dict:
    """Read available reports, without requiring raw data or starting Spark."""
    result = {}
    for name in MODELS:
        path = paths.models / name / "metrics.json"
        if path.exists():
            result[name] = json.loads(path.read_text(encoding="utf-8"))
    return result


def score_rows(name: str, report: dict, scores: dict) -> list[dict]:
    """One row per source (the model or models first, then the baselines) scored on the same hours."""
    sources = [source for source in scores if source != "hours"]
    return [
        {"target": name, "source": source, "hours": scores["hours"], "unit": report["unit"], **scores[source]}
        for source in sources
    ]


def comparison_table(reports: dict) -> pd.DataFrame:
    """Compare held-out 2026 errors of each model and its baselines, with their units and hours."""
    rows = [
        row
        for name in REGRESSIONS
        if name in reports
        for row in score_rows(name, reports[name], reports[name]["evaluation"]["test"])
    ]
    return pd.DataFrame(rows, columns=COLUMNS)


def update_table(reports: dict) -> pd.DataFrame:
    """Compare the update and the 2021-2025 model on the same hours after the update's cutoff."""
    rows = []
    for name in REGRESSIONS:
        update = reports.get(name, {}).get("update")
        if update:
            evaluated = f"{update['evaluated_from']} to {update['evaluated_until']}"
            scored = score_rows(name, reports[name], update["evaluation"])
            rows += [{**row, "evaluated": evaluated} for row in scored]
    return pd.DataFrame(rows, columns=[*COLUMNS, "evaluated"])
