from spain_electricity.config import CITIES, DataPaths


def test_data_paths_layout(tmp_path):
    paths = DataPaths(tmp_path)
    assert paths.raw == tmp_path / "raw"
    assert paths.processed == tmp_path / "processed"
    assert paths.landing == tmp_path / "stream" / "landing"
    assert paths.checkpoints == tmp_path / "stream" / "checkpoints"
    assert paths.stream_output == tmp_path / "stream" / "output"
    assert paths.models == tmp_path / "models"


def test_data_paths_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SPAIN_ELECTRICITY_DATA_DIR", str(tmp_path))
    assert DataPaths.from_env().root == tmp_path


def test_data_paths_default_is_repo_data(monkeypatch):
    monkeypatch.delenv("SPAIN_ELECTRICITY_DATA_DIR", raising=False)
    root = DataPaths.from_env().root
    assert root.name == "data"
    assert (root.parent / "pyproject.toml").exists()


def test_cities_weights_positive():
    assert set(CITIES) == {"madrid", "barcelona", "valencia", "seville", "bilbao"}
    assert all(city.weight > 0 for city in CITIES.values())
    assert CITIES["madrid"].latitude == 40.4168
