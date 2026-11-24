import importlib.util
import json
import sys

from spain_electricity.config import DATA_DIR_ENV, REPO_ROOT


def load_cli():
    spec = importlib.util.spec_from_file_location(
        "train_models_cli", REPO_ROOT / "scripts" / "train_models.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_training_other_models_leaves_the_demand_model_untouched(spark, paths, monkeypatch):
    report = paths.models / "demand" / "metrics.json"
    report.parent.mkdir(parents=True)
    report.write_text(json.dumps({"selected": {"model": "gbt"}}), encoding="utf-8")
    before = report.read_bytes()
    cli = load_cli()
    calls = []
    monkeypatch.setenv(DATA_DIR_ENV, str(paths.root))
    monkeypatch.setattr(cli, "get_spark", lambda *args, **kwargs: spark)
    monkeypatch.setattr(cli, "stop_spark", lambda: None)
    monkeypatch.setattr(cli, "train_demand", lambda *args, **kwargs: calls.append("demand"))
    monkeypatch.setattr(cli, "train_day_types", lambda *args, **kwargs: calls.append("day_types") or {})
    monkeypatch.setattr(sys, "argv", ["train_models.py", "--models", "day_types"])

    cli.main()

    assert calls == ["day_types"]
    assert report.read_bytes() == before
