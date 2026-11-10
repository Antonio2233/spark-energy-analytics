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
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 5
```

### Process it: `scripts/run_stream.py`

Starts two Spark Structured Streaming queries and keeps running until Ctrl+C:

1. Every 30 seconds (`--trigger`), it takes the next new file in `landing/`, one file per micro-batch.
2. It parses the 5-minute readings, converts them to UTC and adds the renewable share.
3. It drops duplicates within a 2-hour watermark, because captures repeat the day so far and replayed days overlap by about 6 hours.
4. The `readings` query appends each reading to `data/stream/output/readings/`.
5. The `windows` query computes 15-minute windows (mean and peak demand, mean renewable share, number of readings) and writes each micro-batch to `data/stream/output/windows/batch_id=N/`.

```bash
uv run python scripts/run_stream.py                  # keeps running
uv run python scripts/run_stream.py --available-now  # processes what has landed, then stops
```

Each query keeps a checkpoint in `data/stream/checkpoints/`, so a restart resumes where it stopped instead of reprocessing every file.

The watermark has a side effect: once the stream has seen a reading, anything more than 2 hours older is dropped as late. That is why the example above replays 4–10 January after 1–3 January had already been replayed, and why replayed history and live captures must not share the same `data/stream` folder. To start over, delete `data/stream`, or point `SPAIN_ELECTRICITY_DATA_DIR` to another folder.

## The UI: `app/streamlit_app.py`

```bash
uv run streamlit run app/streamlit_app.py
```

Opens at <http://localhost:8501>. The app only reads: it aggregates the Parquet datasets with Spark (`views.py`) and draws them with Altair (`charts.py`). What every series means, its unit and its source come from `catalog.py`, shown under each chart ("What these series are and where they come from") and in the About the data tab. Times are UTC, and the charts follow Streamlit's light or dark theme.

| Tab | What you see | Fed by |
|---|---|---|
| Data status | Rows and time range of every processed dataset, with its source and link; below, the attributes of each dataset (column, Spark type, unit and meaning), streaming outputs included | `data/processed/`, `data/stream/output/` |
| History | Actual demand vs REE forecast, the generation mix as one line per technology (click a name in the legend to highlight it; hover lists every technology at that hour), and day-ahead price vs PVPC, for a range of local dates; the hourly table underneath | `hourly_features` |
| Live | Latest reading, demand and renewable share; the last 6 hours of streamed 5-minute demand; the latest 15-minute windows. Refreshes every 30 seconds | `data/stream/output/` |
| Analysis | Daily demand against national temperature, working days apart from weekends and holidays, with a quadratic trend; the correlation between hourly variables, one value per pair | `hourly_features` |
| About the data | What every variable and generation technology is, its unit and its source, and the endpoint the project calls for each source | `catalog.py` |

In live mode the Live tab warns when the newest reading is more than 20 minutes old, which means REE's feed or the capture has stalled. In replay mode it never warns, since replayed readings are old by design.

## Demo in three terminals

```bash
uv run streamlit run app/streamlit_app.py
uv run python scripts/run_stream.py
uv run python scripts/replay_history.py --start 2025-01-04 --end 2025-01-10 --delay 5
```

Open the Live tab: a new day arrives every 5 seconds and the stream picks it up within 30 seconds.

## Two definitions of demand

REE publishes demand in two places, and they do not match:

- The 5-minute feed (`demanda.ree.es`) gives `demand_mw`. It is what the streaming job sees.
- REData gives `actual_demand_mw` (series `Real`), the series REE's own forecast predicts.

On January 2025 (n = 8,928 five-minute instants), `actual_demand_mw` averages 30,660 MW and `demand_mw` 28,976 MW, about 5.5% lower. REE's forecast is only 59 MW below `actual_demand_mw` on average. They cover different systems: REData's series is national ("Demanda nacional en tiempo real"), while the 5-minute feed covers the Peninsula only. That the whole gap is the Balearic and Canary Islands, Ceuta and Melilla is an inference from its size.

The History tab therefore compares the forecast with `actual_demand_mw`. Which of the two the phase 2 demand model predicts is an open decision, recorded in [AGENTS.md](../AGENTS.md).
