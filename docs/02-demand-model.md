# Small-sample demand model and hourly monitoring

The demand extension trains Spark ML pipelines and applies the selected pipeline to hourly Structured Streaming windows. It uses the existing file source; Kafka is not part of the project.

## Reproduce

With the dependencies and Java configured as described in the README:

```bash
uv run python scripts/prepare_demand.py
uv run python scripts/train_demand.py
uv run streamlit run app/streamlit_app.py
```

Preparation downloads the bounded sample and runs ingestion. Downloads are resumable and separated by a 0.5-second pause. `--download-only`, `--days-per-season` (1–7) and `--pause` are available. Training requires at least 100 valid training hours and 24 paired hours in each evaluation split. A one-day sample can be too small or unrepresentative; the four-day default is recommended for this demonstration.

Open **Models** to see selection, metrics, coverage, predictions and the largest errors. Restart an already running UI after changing package code. The trained model is shared in Git; raw and processed data must be downloaded and ingested locally.

## Bounded data and audit

The default selects four consecutive days in January, April, July and October of each year from 2021 to 2026. Starting dates rotate to cover all weekdays in each year. Dates newer than the archive's seven-day settling period are excluded. The manifest `data/demand_sample.json` records every requested date and download failure.

- REE generation: 96 daily files, covering 2,304 hours.
- Peninsular forecast: 32 daily files, only for the 2025 and 2026 evaluation periods.
- Weather: 120 monthly files, five cities for each selected month. Whole months preserve compatibility with the existing weather ingestion.
- No extra gas, price or national-demand downloads are needed for this demand model.

On the first run on 16 November 2026, all 248 files downloaded successfully and occupied 16,765,787 bytes (about 16 MB). The audit measures coverage by year, missing target/temperature/baseline and duplicate hours. Missing training baselines are intentional because training does not use REE's forecast as an input. The default sample yielded 1,536 training hours, 384 validation hours and 384 test hours. All timestamps stay UTC; calendar features use Europe/Madrid.

This is a seasonal teaching sample, not a statistically representative full-year benchmark. It misses other months and many holiday and extreme-event conditions. If other history already exists, ingestion and training use it as well; inspect the saved audit rather than assuming the default counts.

## Model selection without temporal leakage

The target is peninsular `demand_mw`. Inputs are contemporaneous five-city weighted temperature, heating/cooling degrees, local hour, weekday, month, weekend and national holiday. Cyclic features describe hour and month; a working-day/hour category permits different shapes on days off. These are estimates of demand during the observed hour, not genuine day-ahead forecasts.

Every candidate is a Spark `Pipeline`: training-only `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` and an estimator. The four fixed candidates are linear regression with regularization 0.1 or 10, and a 24-tree Random Forest with depth 5 or 8. The seed is fixed. This bounded comparison deliberately avoids a large search.

Fit on 2021–2024, choose the lowest 2025 MAE, then evaluate only that winner on 2026. No random split or test-based parameter selection is used. Nonpositive demand, missing/non-finite temperatures and the 28 April 2025 blackout are excluded. For evaluation, hours without a valid peninsular forecast are removed from both model and baseline. National REData demand/forecast are not substituted.

REE's baseline comes from `prevProgPeninsula`, field `pre`, joined by UTC hour. Those files contain retrospectively downloaded forecasts; their publication vintages are not reconstructed. REE and the contemporaneous ML estimate have different information sets, so a lower error does not establish a better operational forecasting service.

The selected `PipelineModel`, candidate validation scores, paired MAE/RMSE/R², audit and evaluation predictions are saved in `data/models/demand/`. Metrics are written last as the training completion marker.

## First measured results

The first default-sample run selected Random Forest, depth 8, in 43.1 seconds of training/evaluation and artifact saving (excluding JVM startup and data preparation).

| Split | Hours | ML MAE (MW) | ML RMSE (MW) | ML R² | REE MAE (MW) |
|---|---:|---:|---:|---:|---:|
| Validation 2025 | 384 | 1,271.1 | 1,604.9 | 0.887 | 266.9 |
| Test 2026 | 384 | 2,672.6 | 3,357.1 | 0.701 | 283.9 |

The sampled 2026 result is weaker than validation and substantially worse than REE. It is useful for demonstrating a reproducible baseline comparison and identifying generalization limits, not for claiming production forecasting accuracy. The Models panel lists the worst residuals so weather, holidays and changed demand patterns can be inspected; those factors are hypotheses, not established causes. No parameters were changed after observing these test results.

## Streaming estimates and alerts

Restart the streaming job after training:

```bash
uv run python scripts/run_stream.py
```

When a saved model exists, a third query aggregates unique 5-minute readings into 1-hour windows with the same two-hour watermark and its own `hourly_demand` checkpoint. Each micro-batch joins weather, derives the same calendar/features and applies the saved pipeline. The output is `data/stream/output/hourly/batch_id=N/`; readers keep the latest version of each hour.

Replay uses historical processed weather. Live mode reads the captured five-city current-weather array and keeps the newest capture of each city's observation. A national observation requires all five cities; live mode does not silently substitute historical weather. Weather is read again at each demand micro-batch, so a late weather capture is used on the next demand-window update.

States are explicit: `missing_weather` (null estimate), `provisional` (fewer than twelve unique readings), or `complete`. Only complete hours can alert. The threshold is the 99th percentile of absolute validation residuals, calibrated on 2025 only. It is a screening threshold, not a confidence interval or a guaranteed false-positive rate on unseen years. An unusual residual is not proof of an incident. Without a trained model, the original readings and quarter-hour queries continue unchanged.

Replay and live must use separate clean stream state, as in the README. Do not erase existing state casually; a fresh data root can be used for a standalone test/demo if its required history and model are copied there first.

## Tests

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Tests cover bounded sampling, date filtering and DST parsing, chronological splits, blackout/missing-data handling, matched baseline rows, persisted model results, full streaming prediction, missing/provisional/complete-hour alert behavior, live-weather deduplication/source isolation, and rendering the Models and monitoring panels. Test data is local and network-free.
