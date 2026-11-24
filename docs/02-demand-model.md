# Demand model and hourly monitoring

The demand model is a Spark ML pipeline trained on the full 2021–2026 history and applied to hourly Structured Streaming windows. It uses the existing file source; Kafka is not part of the project.

## Reproduce

With the dependencies and Java configured as described in the README:

```bash
uv run python scripts/download_history.py --start 2021-01-01 --end <yesterday>
uv run python scripts/ingest.py
uv run python scripts/train_models.py --models demand
uv run streamlit run app/streamlit_app.py
```

The download includes REE's peninsular forecast (`ree_forecast`), which is this model's baseline. On 2026-11-24 it added 2,176 daily files in 44 min, with no failures. Spark's driver gets 6 GB of heap by default (`SPAIN_ELECTRICITY_DRIVER_MEMORY`), because this training ran out of memory on Spark's own 1 GB.

For a quick demonstration without the full download, use the seasonal sample instead:

- `scripts/prepare_demand.py` downloads four days in each of January, April, July and October of every year;
- `scripts/train_demand.py` trains on it.

Its results describe the sampled dates only.

Open **Models** to see the selection, metrics, coverage, predictions and largest errors. Restart a running UI after changing package code. The trained model is shared in Git; raw and processed data must be downloaded and ingested locally.

## Model selection without temporal leakage

The target is peninsular `demand_mw`. These are estimates of demand during the observed hour, not day-ahead forecasts. The inputs are contemporaneous:

- five-city weighted temperature;
- heating and cooling degrees, with 18 and 22 °C bases;
- local hour, weekday and month, with cyclic encodings for hour and month;
- weekend and national holiday;
- a working-day/hour category, so days off can have a different shape.

Every candidate is a Spark `Pipeline`: `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` and an estimator, all fitted on training rows only. Hyperparameters come from `ParamGridBuilder` grids looped by hand, because Spark's `CrossValidator` and `TrainValidationSplit` split rows at random:

- linear regression: `regParam` 0.1, 1 or 10 × `elasticNetParam` 0 or 0.5 (6 candidates);
- Random Forest with 60 trees: depth 8 or 12 (2);
- GBT: 60 or 120 iterations × depth 4 or 6 (4).

The procedure, in order:

1. Fit each candidate on 2021–2024 and score it on 2025.
2. Pick the lowest 2025 MAE.
3. Take the alert threshold from that fit's 2025 residuals.
4. Refit the winner on every valid 2021–2025 hour.
5. Evaluate it once on 2026.

There is no random split and no selection on test data. Training excludes nonpositive demand, missing or non-finite temperatures, and the 28 April 2025 blackout.

The model is scored against two baselines on identical hours. An hour missing either baseline is removed from all three scores.

- **REE's peninsular forecast** (`prevProgPeninsula`, field `pre`), joined by UTC hour.
  - The files were downloaded retrospectively, so their publication vintages are not reconstructed.
  - REE uses recent demand and market information. It is a reference for a professional forecast, not an equally informed rival.
- **The same hour 24 hours earlier** (`demand_mw_24h_ago`), the naive forecast. It is computed after the blackout is excluded, so the hours of the following day have no baseline and are not scored.

The saved report gives MAE, RMSE, R² and the bias (actual minus estimate). `data/models/demand/` holds:

- the refit `PipelineModel`;
- candidate scores, paired metrics, feature weights and the audit;
- evaluation predictions. Only the test ones come from the saved model; the validation ones come from the 2021–2024 fit.

Metrics are written last, as the completion marker.

## Measured results (24 November 2026)

Full history:

| Set | Hours |
|---|---:|
| Training | 35,063 |
| Validation (paired) | 8,712 |
| Test (paired) | 7,848 |
| Refit | 43,799 |

Training, evaluation and saving took 225 s on 12 cores with a 6 GB driver.

Validation MAE of the candidates (2025, MW):

| Family | Candidates | Best MAE | Best R² |
|---|---:|---:|---:|
| Linear regression | 6 | 1,475 | 0.816 |
| Random Forest | 2 | 1,131 (depth 12) | 0.883 |
| GBT | 4 | **1,084** (120 iterations, depth 6) | 0.886 |

The selected GBT, against both baselines:

| Split | Hours | Source | MAE (MW) | RMSE (MW) | R² | Bias (MW) |
|---|---:|---|---:|---:|---:|---:|
| Validation 2025 | 8,712 | Model | 1,084 | 1,486 | 0.886 | +164 |
| | | REE forecast | 256 | 360 | 0.993 | −1 |
| | | Same hour yesterday | 1,803 | 2,687 | 0.627 | +2 |
| Test 2026 | 7,848 | Model (refit 2021–2025) | 2,473 | 3,091 | 0.664 | **+2,365** |
| | | REE forecast | 262 | 360 | 0.995 | −9 |
| | | Same hour yesterday | 1,862 | 2,786 | 0.727 | +8 |

In 2025 the model clearly beats the naive forecast. In 2026 it does not, and almost all of its error is bias: it underestimates demand by 2,365 MW on average.

Demand in 2026 is higher than in any training year at similar temperatures. July 2026 averages 33,651 MW, against 28,553–29,628 MW in July 2021–2025 (744 hours each), and REData's national series rises the same way.

The model learned the shape that temperature and calendar explain, but not the new level. The next step is to give it the recent level, for example the mean demand of the previous days, which is also available in real time. No parameters were changed after seeing these test results.

The alert threshold is 4,549 MW. It flags 1.0% of the 2025 hours and 15.3% of the 2026 hours. Most 2026 alerts reflect that bias, not incidents.

An earlier run on the seasonal sample (1,536 training hours, four 24-tree candidates) selected a Random Forest with a 2026 MAE of 2,673 MW. It also lost to the naive forecast, which is why we moved to the full history, GBT and the refit.

## Streaming estimates and alerts

Restart the streaming job after training:

```bash
uv run python scripts/run_stream.py
```

When a saved model exists, a third query starts. It aggregates unique 5-minute readings into 1-hour windows, with the same two-hour watermark and its own `hourly_demand` checkpoint. Each micro-batch joins weather, derives the same calendar features and applies the saved pipeline. The output goes to `data/stream/output/hourly/batch_id=N/`, and readers keep the latest version of each hour.

Weather sources:

- **Replay:** historical processed weather.
- **Live:** the captured five-city current-weather array, keeping the newest capture of each city's observation. A national value needs all five cities; live mode never falls back to historical weather.
- Weather is read again at each micro-batch, so a late weather capture is used at the next update of that hour.

Each hour has an explicit state:

- `missing_weather`: no estimate;
- `provisional`: fewer than twelve unique readings;
- `complete`: the only state that can alert.

The threshold is the 99th percentile of the absolute 2025 residuals of the 2021–2024 fit. It is a screening threshold, not a confidence interval or a guaranteed false-positive rate on unseen years, and an unusual residual does not prove an incident. Without a trained model, the readings and quarter-hour queries run unchanged.

Replay and live must use separate clean stream state, as described in the README. Do not erase existing state casually. For a standalone test or demo, use a fresh data root, after copying the history and model it needs into it.

## Tests

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

The tests use local data and no network. They cover:

- bounded sampling, date filtering and DST parsing;
- chronological splits and the handling of the blackout and missing data;
- matched rows for the model and both baselines;
- the hand-looped grid, the refit and the persisted results;
- full streaming prediction, and alert behavior for missing, provisional and complete hours;
- live-weather deduplication and source isolation;
- rendering the Models and monitoring panels.
