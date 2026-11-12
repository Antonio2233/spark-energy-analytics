# How phase 1 works

Phase 1 gets the data in and makes it visible. It downloads the history of every source, turns it into clean Parquet tables with Spark, follows REE in real time with Spark Structured Streaming, and shows everything in a Streamlit UI. It trains no models yet: the demand and price regressions and the KMeans day types come in phase 2 (see the [project plan](plan.md)).

There are two independent paths. The batch path handles the history; the streaming path handles what arrives in real time. They share the same parsing code (`sources/ree_generation.py`, `features.py`) and meet again in the UI.

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

Every command runs through `uv run`, which uses the project's Python and libraries (the `.venv` that uv manages).

## What must be running

`download_history.py`, `ingest.py` and `replay_history.py` do their work and stop. `capture_realtime.py`, `run_stream.py` and the UI keep running until Ctrl+C, each in its own terminal.

| To | Run | What you see |
|---|---|---|
| Browse the history | The UI, after `download_history.py` and `ingest.py` once | Data status, History and Analysis |
| Follow today's data | `capture_realtime.py`, `run_stream.py` and the UI, from an empty `data/stream` | Live with readings a few minutes old, a new capture every 5 minutes |
| Demo with replayed history | `run_stream.py`, `replay_history.py` and the UI, replaying days later than the newest reading the stream has processed (or from an empty `data/stream`) | Live advancing one past day per micro-batch with `--delay 30` |

With none of the streaming scripts running, Live shows the last readings that were processed and a notice saying from which day they are.

## Batch path: the history

### Download: `scripts/download_history.py`

Fetches the raw files of a date range and stores them exactly as received in `data/raw/`. Nothing is parsed here.

```bash
uv run python scripts/download_history.py --start 2025-01-01 --end 2025-01-31
```

| Source | What it holds | Raw file |
|---|---|---|
| REE `demanda.ree.es` | Demand and generation by technology, every 5 minutes | one file per day, `ree_generation/YYYY/MM/` |
| REE REData | Actual, forecast and scheduled demand; day-ahead market price and PVPC | one file per month, `redata_demand/`, `redata_price/` |
| Open-Meteo archive | Hourly temperature, solar radiation and wind at 100 m in five cities | one file per city and month, `open_meteo/<city>/` |
| MIBGAS | Daily natural gas price (`GDAES_D+1`) | one workbook per year, `mibgas/` |

Options:

- `--sources` downloads only some sources (`ree_generation`, `redata_demand`, `redata_price`, `open_meteo`, `mibgas`).
- `--pause` sets the seconds between requests (1 by default).

Downloads are resumable. A rerun skips a file once its period has settled, and fetches it again if it was written while its day or month was still open. A failed request is retried, then reported, and the rest carry on.

### Ingest: `scripts/ingest.py`

Reads everything in `data/raw/` with Spark and writes one Parquet dataset per source to `data/processed/`. All timestamps are converted to UTC, including REE's daylight-saving labels.

```bash
uv run python scripts/ingest.py
```

| Dataset | Content | Rows for January 2025 |
|---|---|---|
| `generation` | 5-minute demand and generation mix | 8,928 |
| `demand_forecast` | 5-minute actual, forecast and scheduled demand (REData) | 8,928 |
| `market_price` | Day-ahead price (quarter-hourly) and PVPC (hourly), in long format | 3,720 |
| `weather` | Hourly weather per city | 3,720 |
| `gas_price` | Daily gas price per delivery day (the whole 2025 workbook) | 365 |
| `hourly_features` | All of the above joined by UTC hour | 744 |

`hourly_features` is the table the phase 2 models will read. It has one row per hour:

- demand, generation by technology and renewable share (wind, solar and hydro over demand);
- REData actual demand and REE's forecast;
- prices and the day's gas price;
- national weather (the population-weighted mean of the five cities);
- calendar: local date and hour, day of week, weekend and national holiday.

The 5-minute readings are averaged into the hour, never summed: they are power values in MW, so their mean is the hour's energy in MWh.

## Streaming path: real time

The streaming job reads files from `data/stream/landing/`. Two scripts can put files there; the job does not care which one did.

### Feed it with live data: `scripts/capture_realtime.py`

Every 5 minutes (`--interval`, in seconds), it downloads today's REE readings so far and the current Open-Meteo weather, and lands them in `landing/`, named after the UTC capture time. On start it catches up: if the last capture is older than the start of today's REE file, it lands the missed days first (up to 14), oldest first.

```bash
uv run python scripts/capture_realtime.py
```

### Feed it with replayed history: `scripts/replay_history.py`

Copies the downloaded REE days of a range into `landing/`, one every `--delay` seconds, as if they were arriving live. It is the way to demo the stream without depending on REE's real-time feed, which only changes every 5 minutes and sometimes stalls.

```bash
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 30
```

### Process it: `scripts/run_stream.py`

Starts two Spark Structured Streaming queries and keeps running until Ctrl+C:

1. Every 30 seconds (`--trigger`), it takes the new files in `landing/`, up to 20 per micro-batch (`--max-files`). After a restart it works through the captures it missed in a few batches.
2. It parses the 5-minute readings, converts them to UTC and adds the renewable share.
3. It drops duplicates within a 2-hour watermark, because captures repeat the day so far and replayed days overlap by about 6 hours.
4. The `readings` query appends each reading to `data/stream/output/readings/`.
5. The `windows` query computes 15-minute windows (mean and peak demand, mean renewable share, number of readings) and writes each micro-batch to `data/stream/output/windows/batch_id=N/`.

```bash
uv run python scripts/run_stream.py                  # keeps running
uv run python scripts/run_stream.py --available-now  # processes what has landed, then stops
```

Each query keeps a checkpoint in `data/stream/checkpoints/`, so a restart resumes where it stopped instead of reprocessing every file.

The watermark has a side effect: once the stream has seen a reading, anything more than 2 hours older is dropped as late. So replay only days later than the newest reading the stream has processed, and never mix replayed history and live captures in the same `data/stream`. To start over, delete `data/stream`: it holds only the stream's input, checkpoints and output, and the history is untouched. `SPAIN_ELECTRICITY_DATA_DIR` is not an alternative, because it moves the whole data folder, history included.

## The UI: `app/streamlit_app.py`

```bash
uv run streamlit run app/streamlit_app.py
```

Opens at <http://localhost:8501>. The app only reads: it aggregates the Parquet datasets with Spark (`views.py`) and draws them with Altair (`charts.py`). What every series means, its unit and its source come from `catalog.py`, shown under each chart ("What these series are and where they come from") and in the About the data section of Data status. Times are UTC, and the charts follow Streamlit's light or dark theme.

| Tab | What you see | Fed by |
|---|---|---|
| Data status | Rows and time range of every processed dataset, with its source and link; below, the attributes of each dataset (column, Spark type, unit and meaning), streaming outputs included; and the About the data section: every variable and generation technology, its unit and source, and the endpoint the project calls for each source | `data/processed/`, `data/stream/output/`, `catalog.py` |
| History | Actual demand vs REE forecast, the generation mix as one line per technology (click a name in the legend to highlight it; hover lists every technology at that hour), and day-ahead price vs PVPC, for a range of local dates; the hourly table underneath | `hourly_features` |
| Live | Latest reading, demand and renewable share; the last 6 hours of streamed 5-minute demand; the latest 15-minute windows. Refreshes every 30 seconds | `data/stream/output/` |
| Analysis | Daily demand against national temperature, working days apart from weekends and holidays, with a quadratic trend; the correlation between hourly variables, one value per pair | `hourly_features` |

In live mode the Live tab warns when the newest reading is more than 20 minutes old, and says which of three causes it is: the capture landed files the stream has not processed (start `run_stream.py`), nothing has been captured for 15 minutes (start `capture_realtime.py`), or both run and REE's feed is answering with old readings (nothing to restart). In replay mode it never warns, since replayed readings are old by design; instead, a notice says from which day the readings are and which scripts to run to follow today's data. The tab only shows today's data while `capture_realtime.py` and `run_stream.py` are running.

## Demo in three terminals

```bash
rm -rf data/stream   # only the stream's input, checkpoints and output; the history is untouched
uv run streamlit run app/streamlit_app.py
uv run python scripts/run_stream.py
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 30
```

Open the Live tab: a past day lands every 30 seconds and the stream processes it in its next micro-batch, so Live advances one day at a time; the 7 days take about 3.5 minutes. Without the first line, after a live session every replayed day would be older than the watermark and nothing would appear.

## Two definitions of demand

REE publishes demand in two places, and they do not match:

- The 5-minute feed (`demanda.ree.es`) gives `demand_mw`. It is what the streaming job sees.
- REData gives `actual_demand_mw` (series `Real`), the series REE's own forecast predicts.

On January 2025 (n = 8,928 five-minute instants), `actual_demand_mw` averages 30,660 MW and `demand_mw` 28,976 MW, about 5.5% lower. REE's forecast is only 59 MW below `actual_demand_mw` on average. They cover different systems: REData's series is national ("Demanda nacional en tiempo real"), while the 5-minute feed covers the Peninsula only. That the whole gap is the Balearic and Canary Islands, Ceuta and Melilla is an inference from its size.

The History tab therefore compares REData's forecast with `actual_demand_mw`. The phase 2 demand model predicts `demand_mw`, the demand the streaming job sees, and its baseline is REE's peninsular forecast, which comes from another endpoint of the 5-minute feed and matches `demand_mw` (on 15 January 2025, n = 288, it is 33 MW below on average). The decision and the endpoint are recorded in [AGENTS.md](../AGENTS.md).

The Iberian blackout of 28 April 2025 shows as the lowest working day in the Analysis tab (mean demand 15,512 MW). It stays in the data and the UI, but the phase 2 models exclude it from training and validation.
