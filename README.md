# Electricity demand, generation mix and price in Spain with Spark

Final project for the Big Data course (UMA Master's). We combine Red Eléctrica's data (demand, generation by technology and price) with weather and the natural gas price. With Spark ML we explain the hourly demand, generation mix and price from the 2021–2026 history. With Structured Streaming we follow the system in real time and raise alerts when it departs from what is expected.

## Documentation

- [macOS setup and launch](README_MAC.md): native Mac installation, compact data/model preparation, one-click source selection and troubleshooting.
- [How phase 1 works](docs/01-how-it-works.md): what every script does, the datasets it writes, the streaming job and the UI tabs.
- [Demand model and hourly monitoring](docs/02-demand-model.md): full-history training against two baselines, measured results and streaming estimates.
- [Energy Desk](docs/03-energy-desk.md): live-first market dashboard, source alignment, bounded market preparation and real-time/replay behavior.
- [Project plan](docs/plan.md): questions, sources, models, streaming and schedule by iteration.
- [Phase 1 implementation plan](docs/superpowers/plans/2026-11-10-phase-1-data.md): data ingestion, first stream and UI.
- [Phase 2 implementation plan](docs/superpowers/plans/2026-11-12-phase-2-models.md): Spark ML models against REE's baselines, day types, expected demand in the stream and a Models tab.
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
- `streamlit_app.py` renders the desk and owns a background live-data controller when **Real time** is selected. The historical view reads the downloaded batch data directly.

### What must be running

For normal use, run **only the UI**. In the sidebar choose **Data source → Real time** to start capture and Structured Streaming automatically, or **Historical** to select a downloaded day. Select **Replay** to view the separate CLI replay stream. Startup and errors appear in the app; the replay demo requires the two scripts below. The UI live namespace is `data/stream_live`, so switching views preserves the legacy replay and all historical data.

| To | Run | Notes |
|---|---|---|
| Browse the history | The UI | After downloading and ingesting once |
| Follow today's data | The UI, then select **Real time** | Capture and streaming start automatically; the event timestamp and source age remain visible |
| Demo with replayed history | `run_stream.py`, `replay_history.py` and the UI | Replay days later than the newest reading the stream has processed, or empty `data/stream` first |

The standalone capture, replay and stream scripts below are advanced CLI tools; Replay requires the replay and stream scripts. The UI controller pauses when no viewer requests live data; closed browser sessions expire after 90 seconds. It stops only its own queries and keeps checkpoints for the next visit.

## Requirements

- [uv](https://docs.astral.sh/uv/) to manage Python and dependencies.
- Java 17 or 21, required by Spark 4.

## Getting started

```bash
uv sync --locked
uv run --locked streamlit run app/streamlit_app.py --server.port 8501
```

The repository includes the trained model and its evaluation results, but not raw or processed data. Download and ingest the data before using History and the full desk; retraining is not required. Install Java and uv first; native Mac instructions are in [README_MAC.md](README_MAC.md). Local environments and live/replay checkpoints remain excluded from Git.

The UI now opens on **Live**, an Energy Desk with demand, generation mix, electricity/gas references, weather, model estimates and alerts. Use **Data source → Historical / Replay / Real time** to choose downloaded history or start current capture automatically. Its mode badge and event timestamp distinguish the views.

### Quick demonstration on a seasonal sample

The trained models are already in Git, so a fresh checkout only needs data to show. This prepares a compact sample instead of the full history; retraining is optional and only reflects the sampled dates.

```bash
uv run python scripts/prepare_demand.py
uv run python scripts/prepare_desk_market.py --month 2026-10 --forecast-day 2026-10-12
# Optional: uv run python scripts/train_demand.py
uv run streamlit run app/streamlit_app.py
```

This downloads a bounded sample of 96 days (248 files, about 16 MB on the first run) and builds hourly features. **Models** shows the saved full-history results: candidate validation scores, metrics against the baselines and the worst errors. Price models and KMeans are described in the Phase 2 section below. Kafka is out of scope.

After training, restart `run_stream.py` to enable a third query with hourly demand estimates and validation-calibrated alerts in **Live**. Live mode uses captured weather; replay uses historical weather. Missing weather and incomplete hours never trigger alerts. See the [demand extension](docs/02-demand-model.md) for the measured accuracy and limitations.

### 1. History: download and ingest with Spark

```bash
# One sample month of every source (about 40 requests, one second apart)
uv run python scripts/download_history.py --start 2025-01-01 --end 2025-01-31
# Raw files → Parquet in data/processed, plus the hourly feature table
uv run python scripts/ingest.py
```

Downloads are resumable: a rerun skips files whose period has settled and refreshes those fetched while their day or month was still open.

To prepare the full history for final training:

```bash
uv run python scripts/download_history.py --start 2021-01-01 --end 2026-11-09
uv run python scripts/ingest.py
```

The default sources now also include the peninsular forecast, adding roughly 2,140 daily requests for 2021–2026. On 2026-11-10 the earlier source set was 2,642 files (about 490 MB): the download took 1 h 14 min with no failures, and the ingest 31 s. On 2026-11-24 the forecast added 2,176 files, downloaded in 44 min with no failures.

Local Spark sessions get a 6 GB driver heap by default, because full-history training ran out of Spark's own 1 GB. Change it with `SPAIN_ELECTRICITY_DRIVER_MEMORY` (for example `SPAIN_ELECTRICITY_DRIVER_MEMORY=4g`); it applies when a process starts its Spark session.

### 2. Streaming

Start the streaming job in one terminal. It processes a new file every 30 seconds and keeps running until Ctrl+C:

```bash
uv run python scripts/run_stream.py
```

Feed it from another terminal, either with replayed history or with live data:

```bash
# Replay: one downloaded day every 30 seconds, as if it were arriving live
uv run python scripts/replay_history.py --start 2025-01-01 --end 2025-01-07 --delay 30
# Live: capture REE and Open-Meteo every 5 minutes (catches up missed days on start)
uv run python scripts/capture_realtime.py
```

In live mode the first capture lands today's file at once, and the stream processes it within 30 seconds. Every 30-second micro-batch takes up to 20 landed files (`--max-files`), so after a restart the stream works through the captures it missed in a few batches. Spark's own UI shows every micro-batch at http://localhost:4040, or the next free port (4041, 4042…) when another Spark session already holds it.

The stream keeps a 2-hour watermark. Once it has seen a reading, anything more than 2 hours older is dropped as late. Two consequences:

- Replaying days that were already replayed adds nothing; replay later days, or delete `data/stream` to start over.
- Do not mix replay and live captures in the same `data/stream`: delete `data/stream` between modes. It holds only the stream's input, checkpoints and output; the history is untouched. (`SPAIN_ELECTRICITY_DATA_DIR` moves the whole data folder, history included, so it does not keep two streams apart.)

### 3. UI

```bash
uv run streamlit run app/streamlit_app.py
```

Opens at <http://localhost:8501>, with five tabs. All times are UTC. Chart descriptions explain each series and its source. Restart the UI after changing code in `src/`: Streamlit reruns the app file when it changes, but keeps the package modules it already loaded.

- **Data status:** rows and time range of every processed dataset, with its source; the attributes (columns) of each dataset; and what every variable and generation technology is, its unit, and where it comes from.
- **History:** actual demand vs REE's forecast (both from REData, so they measure the same demand), the generation mix (one line per technology), and day-ahead price vs PVPC.
- **Live:** the streamed 5-minute demand and the 15-minute windows. It shows today's data only while `capture_realtime.py` and `run_stream.py` are running; otherwise it shows the last replayed or captured readings, with a notice saying from which day and how to go live. Refreshes every 30 seconds, and warns in live mode when no new reading arrives for 20 minutes.
- **Analysis:** daily demand against temperature (working days apart from weekends and holidays), and the correlation between hourly variables with its value on every cell.
- **Models:** validation candidates, each model against its baselines (REE's peninsular forecast and the same hour the day before), held-out predictions, worst residuals, feature weights and KMeans day types. Live also shows hourly model estimates when the model-enabled stream is running.

### Demo in three terminals

With the history already ingested:

```bash
rm -rf data/stream   # only the stream's output and checkpoints; the history is untouched
uv run streamlit run app/streamlit_app.py
uv run python scripts/run_stream.py
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 30
```

Open the Live tab: a past day lands every 30 seconds and the stream processes it in its next micro-batch, so Live advances one day at a time; the 7 days take about 3.5 minutes.

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
- `data/`: only trained models are shared; raw and processed data, local tools and streaming state are ignored.


## Phase 2 models

The Models tab includes the demand, price and renewable-share regressions, feature weights and KMeans day types, all trained on the full 2021–2026 history and saved in Git. To retrain them after downloading and ingesting the full history:

```bash
uv run python scripts/train_models.py            # all four models; --models selects some
```

On 2026-11-24 demand took 225 s and the other three about 11 s each, on 12 cores. For a quick experiment on the seasonal sample instead, run `scripts/prepare_phase2.py` after `prepare_demand.py`; it requests only the 24 price months and six annual gas references the sample needs. See [demand model](docs/02-demand-model.md) and [Phase 2 models](docs/04-phase-2-models.md) for features, temporal evaluation, measured results, units, DST handling, limitations and `notebooks/02_models.ipynb`.
