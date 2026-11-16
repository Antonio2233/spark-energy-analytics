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
cd "/path/to/Trabajo_Big_Data/spark-energy-analytics"

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

## 4. Use the included history and trained model

The repository includes a small ready-to-run snapshot in `data/raw`, `data/processed` and `data/models`, together with the exact sample dates and provenance. **Skip downloading and training:** a fresh checkout already has the historical tables, saved model, evaluation metrics and predictions.

Continue directly to step 5. The snapshot occupies about 24 MB before Git compression; the processed tables and saved model/results account for about 3 MB. See [data/README.md](data/README.md) for its contents and training coverage.

### Optional: regenerate the snapshot

Only run these commands if you deliberately want to rebuild or replace the included snapshot:

```bash
uv run --locked python scripts/prepare_demand.py
uv run --locked python scripts/prepare_desk_market.py --month 2026-10 --forecast-day 2026-10-12
uv run --locked python scripts/train_demand.py
```

The first command downloads the compact seasonal sample and ingests it with Spark. The second adds the demonstration month's electricity prices, its year's gas reference and one forecast day. The third compares four small demand-model configurations and saves the selected pipeline. The default demand sample contains 96 days and 2,304 hours, rather than the full six-year history. Downloads are resumable.

Inspect **Data status** and **Models** before regenerating anything. Environments, tooling, live captures and streaming checkpoints remain local; only the curated historical snapshot and saved model are shared.

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
cd "/path/to/Trabajo_Big_Data/spark-energy-analytics"
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
- **History or Models is empty:** check that your checkout includes `data/processed`, `data/models/demand/pipeline` and `data/models/demand/metrics.json`. Update to the shared snapshot version first; regenerating it is optional.
- **Real time is connecting or stale:** inspect the connection message and timestamp. Use **Retry connection** when offered; switching to Historical remains available.

## Optional checks

```bash
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
```

The commands match the repository configuration and the official installation references above. Native macOS execution has not been tested from the current Windows/WSL workstation.
