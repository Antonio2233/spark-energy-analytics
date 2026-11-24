# Run the Energy Desk on macOS

Run these commands in macOS Terminal (zsh). The application manages live capture and Spark Structured Streaming from its sidebar.

## 1. Install the requirements once

Install [Homebrew](https://brew.sh/) first if `brew --version` is unavailable. Then install uv and Java 21:

```bash
brew install uv openjdk@21
```

Official installation references: [uv with Homebrew](https://docs.astral.sh/uv/getting-started/installation/#homebrew) and [Homebrew OpenJDK 21](https://formulae.brew.sh/formula/openjdk@21).

## 2. Open the repository and configure Java

Replace the example path with the location of your checkout. The correct folder contains `pyproject.toml`, `uv.lock`, `app/` and `src/`.

```bash
cd "/path/to/spark-energy-analytics"

export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
unset SPARK_HOME HADOOP_HOME PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON

java -version
```

`java -version` should report Java 21. `brew --prefix` selects the installed Homebrew location, including on Apple Silicon and Intel Macs. These environment settings apply to the current terminal; repeat this block when opening a new terminal.

## 3. Install the project's Python environment once

```bash
uv python install 3.13
uv sync --locked
```

The project selects Python 3.13 in `.python-version`; `uv.lock` fixes the dependencies. uv creates the local `.venv` automatically. Create it on the Mac instead of copying a virtual environment from another computer. There is no need to activate it manually or install Spark separately.

Python-management reference: [uv Python installation](https://docs.astral.sh/uv/guides/install-python/).

## 4. Prepare historical and market data

The trained models and their evaluation results are included; raw and processed data are not. Prepare a compact dataset before using History or the historical desk:

```bash
uv run --locked python scripts/prepare_demand.py
uv run --locked python scripts/prepare_desk_market.py --month 2026-10 --forecast-day 2026-10-12
```

The first command downloads a seasonal sample and ingests it. The second adds the demonstration market references. Retraining is optional. Downloads need internet access and remain local.

## 5. Launch the dashboard

From the repository folder, with the Java environment above configured:

```bash
uv run --locked streamlit run app/streamlit_app.py --server.port 8501
```

Open **http://localhost:8501**. Keep this terminal running and use the sidebar:

- **Data source → Real time:** starts capture and Structured Streaming automatically. Generation/weather are captured every five minutes; the first view appears after the initial capture and processing. Connection state, observation age and errors are shown in the app.
- **Data source → Historical:** choose a downloaded day. Switching views preserves the historical data and checkpoints.

No extra capture or streaming terminals are required. Live state is isolated in `data/stream_live`; batch history and saved models are shared. The controller pauses when no browser requests live data, with a 90-second expiry for a closed browser. The capture depends on the source APIs and internet access, so always check the displayed observation timestamp.

Stop the application with **Ctrl+C** in its terminal.

## Next launches: commands to copy

After the initial installation:

```bash
cd "/path/to/spark-energy-analytics"
export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
unset SPARK_HOME HADOOP_HOME PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
uv run --locked streamlit run app/streamlit_app.py --server.port 8501
```

## Troubleshooting

- **`brew` or `uv` is not found:** finish the Homebrew installation/PATH instructions, reopen Terminal and check `brew --version` / `uv --version`.
- **Java is missing or has the wrong version:** repeat the Java exports above and check `"$JAVA_HOME/bin/java" -version`.
- **An import error appears after updating the project:** stop the existing server with Ctrl+C, run `uv sync --locked`, then relaunch. Refreshing the browser alone can leave old Python modules loaded in the server.
- **Port 8501 is occupied:** stop the earlier dashboard in its terminal. To identify the listener, run `lsof -nP -iTCP:8501 -sTCP:LISTEN`. Keep one dashboard instance per checkout so live checkpoint writers are not duplicated.
- **History or Models is empty:** run the preparation commands in step 4, and check that `data/models/demand/pipeline` and `data/models/demand/metrics.json` exist.
- **Real time is connecting or stale:** inspect the connection message and timestamp. Use **Retry connection** when offered; switching to Historical remains available.

## Optional checks

```bash
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
```

These steps were run natively on macOS 26 (Apple silicon) with Homebrew's OpenJDK 21 on 24 November 2026: the test suite, full-history model training and the dashboard.


## Optional: retrain the models

The trained models are included in `data/models/`. To retrain them on the full 2021–2026 history, with the same Java environment:

```bash
uv run --locked python scripts/download_history.py --start 2021-01-01 --end <yesterday>
uv run --locked python scripts/ingest.py
uv run --locked python scripts/train_models.py
```

The download is long: REE's peninsular forecast alone took 44 minutes (2,176 daily files). Training uses 6 GB of Spark driver memory by default; set `SPAIN_ELECTRICITY_DRIVER_MEMORY` (for example `8g`) to change it. The results appear in **Models**. See [the demand model](docs/02-demand-model.md) and [the Phase 2 models](docs/04-phase-2-models.md).
