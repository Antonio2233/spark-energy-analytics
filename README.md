# Electricity demand, generation mix and price in Spain with Spark

Final project for the Big Data course (UMA Master's). We combine Red Eléctrica's data (demand, generation by technology and price) with weather and the natural gas price. With Spark ML we explain the hourly demand, generation mix and price from the 2021–2026 history. With Structured Streaming we follow the system in real time and raise alerts when it departs from what is expected.

## Documentation

- [Project plan](docs/plan.md): questions, sources, models, streaming and schedule by iteration.
- [Phase 1 implementation plan](docs/superpowers/plans/2026-11-10-phase-1-data.md): data ingestion, first stream and UI.
- [AGENTS.md](AGENTS.md): instructions for AI-assisted development, data-source facts, Spark usage, Python guide and tests.

## Requirements

- [uv](https://docs.astral.sh/uv/) to manage Python and dependencies.
- Java 17 or 21, required by Spark 4.

## Getting started

```bash
uv sync
```

### 1. History: download and ingest with Spark

```bash
# One sample month of every source (about 40 requests, one second apart)
uv run python scripts/download_history.py --start 2025-01-01 --end 2025-01-31
# Raw files → Parquet in data/processed, plus the hourly feature table
uv run python scripts/ingest.py
```

Downloads are resumable: a rerun skips files whose period has settled and refreshes those fetched while their day or month was still open.

### 2. Streaming

Start the streaming job in one terminal:

```bash
uv run python scripts/run_stream.py
```

and feed it from another terminal, either with replayed history or with live data:

```bash
# Replay: one downloaded day every 5 seconds, as if it were arriving live
uv run python scripts/replay_history.py --start 2025-01-01 --end 2025-01-07 --delay 5
# Live: capture REE and Open-Meteo every 5 minutes (catches up missed days on start)
uv run python scripts/capture_realtime.py
```

Do not mix replay and live captures in the same `data/stream`: once live readings advance the watermark, older replayed readings are dropped as late. Use a separate folder (`SPAIN_ELECTRICITY_DATA_DIR=/path/to/other/data`) or delete `data/stream` between modes.

### 3. UI

```bash
uv run streamlit run app/streamlit_app.py
```

Four tabs: **Data status**, **History** (demand vs REE forecast, generation mix, prices), **Live** (refreshed every 30 seconds, with a warning when no new readings arrive for 20 minutes) and **Analysis** (daily demand vs temperature, correlation matrix).

### Tests

```bash
uv run pytest -m "not integration"   # unit tests
uv run pytest -m integration         # end-to-end Spark, streaming and UI tests
```

## Layout

- `src/spain_electricity/`: package code (sources, features, capture, streaming, views).
- `scripts/`: command-line entry points.
- `app/`: Streamlit UI.
- `notebooks/`: thin analysis notebooks over the package.
- `docs/`: plan and documentation.
- `data/`: downloaded and generated data; not versioned.
