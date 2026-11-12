# Electricity demand, generation mix and price in Spain with Spark

Final project for the Big Data course (UMA Master's). We combine Red Eléctrica's data (demand, generation by technology and price) with weather and the natural gas price. With Spark ML we explain the hourly demand, generation mix and price from the 2021–2026 history. With Structured Streaming we follow the system in real time and raise alerts when it departs from what is expected.

## Documentation

- [How phase 1 works](docs/01-how-it-works.md): what every script does, the datasets it writes, the streaming job and the UI tabs.
- [Project plan](docs/plan.md): questions, sources, models, streaming and schedule by iteration.
- [Phase 1 implementation plan](docs/superpowers/plans/2026-11-10-phase-1-data.md): data ingestion, first stream and UI.
- [AGENTS.md](AGENTS.md): instructions for AI-assisted development, data-source facts, Spark usage, Python guide and tests.

## How the pieces fit

There are two independent paths: the batch path processes the downloaded history, the streaming path processes what arrives in real time. Both end in the Streamlit UI.

```text
 HISTORY (batch)                                REAL TIME (streaming)
 download_history.py → data/raw/                replay_history.py   ─┐
 ingest.py (Spark)   → data/processed/          capture_realtime.py ─┴→ data/stream/landing/
          │                                                               │ run_stream.py (Spark)
          │                                                               ▼
          │                                                      data/stream/output/
          └───────────────► streamlit_app.py ◄────────────────────────────┘
        (Data status, History, Analysis)                 (Live)
```

- `download_history.py` fetches the raw files of a date range from REE, Open-Meteo and MIBGAS, as received.
- `ingest.py` turns them with Spark into Parquet datasets in UTC, plus `hourly_features`: one row per hour with demand, mix, prices, weather, gas and calendar, the input of the phase 2 models.
- `capture_realtime.py` lands today's REE readings and the current weather every 5 minutes, and catches up missed days after an outage.
- `replay_history.py` lands downloaded REE days one after another, as if they were arriving live.
- `run_stream.py` is the Spark Structured Streaming job. It reads each landed file, removes duplicates, and writes the 5-minute readings and 15-minute windows.
- `streamlit_app.py` only reads: it aggregates the Parquet output with Spark and draws it.

### What must be running

`download_history.py` and `ingest.py` run once and stop. `replay_history.py` stops once it has landed its days. `capture_realtime.py`, `run_stream.py` and the UI keep running, each in its own terminal, until Ctrl+C.

| To | Run | Notes |
|---|---|---|
| Browse the history | The UI | After downloading and ingesting once |
| Follow today's data | `capture_realtime.py`, `run_stream.py` and the UI | Start from an empty `data/stream`. Live shows readings a few minutes old; without both scripts it shows the last processed readings, with a notice |
| Demo with replayed history | `run_stream.py`, `replay_history.py` and the UI | Replay days the stream has not seen yet, or empty `data/stream` first |

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

The models use the full history, from 2021 to yesterday:

```bash
uv run python scripts/download_history.py --start 2021-01-01 --end 2026-11-09
uv run python scripts/ingest.py
```

On 2026-11-10 that was 2,642 files (about 490 MB): the download took 1 h 14 min with no failures, and the ingest 31 s.

### 2. Streaming

Start the streaming job in one terminal. It processes a new file every 30 seconds and keeps running until Ctrl+C:

```bash
uv run python scripts/run_stream.py
```

Feed it from another terminal, either with replayed history or with live data:

```bash
# Replay: one downloaded day every 5 seconds, as if it were arriving live
uv run python scripts/replay_history.py --start 2025-01-01 --end 2025-01-07 --delay 5
# Live: capture REE and Open-Meteo every 5 minutes (catches up missed days on start)
uv run python scripts/capture_realtime.py
```

In live mode the first capture lands today's file at once, and the stream processes it within 30 seconds. In http://localhost:4041 (or 4040 when the UI is not running) Spark's own UI shows every micro-batch.

The stream keeps a 2-hour watermark. Once it has seen a reading, anything more than 2 hours older is dropped as late. Two consequences:

- Replaying days that were already replayed adds nothing; replay later days, or delete `data/stream` to start over.
- Do not mix replay and live captures in the same `data/stream`. Use a separate folder (`SPAIN_ELECTRICITY_DATA_DIR=/path/to/other/data`) or delete `data/stream` between modes.

### 3. UI

```bash
uv run streamlit run app/streamlit_app.py
```

Opens at <http://localhost:8501>, with four tabs. All times are UTC. Under every chart, a folded section explains each series and links its source. Restart the UI after changing code in `src/`: Streamlit reruns the app file when it changes, but keeps the package modules it already loaded.

- **Data status:** rows and time range of every processed dataset, with its source; the attributes (columns) of each dataset; and what every variable and generation technology is, its unit, and where it comes from.
- **History:** actual demand vs REE's forecast (both from REData, so they measure the same demand), the generation mix (one line per technology), and day-ahead price vs PVPC.
- **Live:** the streamed 5-minute demand and the 15-minute windows. It shows today's data only while `capture_realtime.py` and `run_stream.py` are running; otherwise it shows the last replayed or captured readings, with a notice saying from which day and how to go live. Refreshes every 30 seconds, and warns in live mode when no new reading arrives for 20 minutes.
- **Analysis:** daily demand against temperature (working days apart from weekends and holidays), and the correlation between hourly variables with its value on every cell.

### Demo in three terminals

With the history already ingested:

```bash
rm -rf data/stream   # only the stream's output and checkpoints; the history is untouched
uv run streamlit run app/streamlit_app.py
uv run python scripts/run_stream.py
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 5
```

Open the Live tab: a new day lands every 5 seconds and the stream picks it up within 30 seconds.

### Tests

```bash
uv run pytest -m "not integration"   # unit tests
uv run pytest -m integration         # end-to-end Spark, streaming and UI tests
```

## Layout

- `src/spain_electricity/`: package code (sources, features, capture, streaming, views for the UI data, charts for the UI charts, catalog for what each series means).
- `scripts/`: command-line entry points.
- `app/`: Streamlit UI.
- `notebooks/`: thin analysis notebooks over the package.
- `docs/`: plan and documentation.
- `data/`: downloaded and generated data; not versioned.
