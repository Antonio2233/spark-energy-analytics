"""Small saved-result tables shared by the notebook and presentation views."""

import json

import pandas as pd

from spain_electricity.config import DataPaths


def saved_reports(paths: DataPaths) -> dict:
    """Read available reports, without requiring raw data or starting Spark."""
    result = {}
    for name in ("demand", "price", "renewable_share", "day_types"):
        path = paths.models / name / "metrics.json"
        if path.exists():
            result[name] = json.loads(path.read_text(encoding="utf-8"))
    return result


def comparison_table(reports: dict) -> pd.DataFrame:
    """Compare held-out model and baseline errors with their own units and sample sizes."""
    rows = []
    for name, report in reports.items():
        if name == "day_types":
            continue
        test = report["evaluation"]["test"] if name == "demand" else report["test"]
        baseline = "ree" if name == "demand" else "baseline"
        for source in ("model", baseline):
            rows.append(
                {
                    "target": name,
                    "source": source,
                    "hours": test["hours"],
                    "unit": "MW" if name == "demand" else report["unit"],
                    **test[source],
                }
            )
    return pd.DataFrame(rows)
