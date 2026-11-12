# Phase 2 · Spark ML Models, Expected Demand in the Stream and Models UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Train Spark ML models on the 2021–2026 hourly history to explain and predict demand, price and the renewable share; find day types with KMeans. Then apply the demand model in the stream, so live demand is compared with what weather and calendar predict, and show all of it in the UI.

**Architecture:**
- A new source, REE's peninsular forecast (`prevProgPeninsula`), gives the demand model a fair baseline.
- The hourly table gains that forecast, degree-hours, an anomaly flag and an Iberian-exception flag.
- `models.py` holds the model specifications, the temporal split, the hand-written hyperparameter loop, evaluation against each baseline and the saving of `PipelineModel`s with a `metrics.json`. `day_types.py` holds KMeans on daily curves.
- A training CLI writes everything to `data/models/`.
- The streaming job gains an hourly query whose `foreachBatch` joins the hour's temperature (history in replay, live captures otherwise) and applies the demand model.
- The UI gains a Models tab, and Live shows expected vs actual demand.

**Tech Stack:** Python 3.13, PySpark 4.2.x (`pyspark.ml`: `Pipeline`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler`, `LinearRegression`, `RandomForestRegressor`, `GBTRegressor`, `KMeans`, `RegressionEvaluator`, `ClusteringEvaluator`), Structured Streaming, pandas < 3 for plotting only, Streamlit with Altair, pytest, ruff, uv.

**Spec:** `docs/plan.md` (ML and streaming parts, iteration 2) and `AGENTS.md` (decisions, data-source facts, Spark ML rules, Python guide, tests). Phase 1 is described in `docs/01-how-it-works.md`.

## Global Constraints

- Everything in the repository is in English: identifiers, docstrings (PEP 257), comments, logs, column names, docs, UI and chart labels.
- All timestamps are UTC; calendar features come from `Europe/Madrid` wall time. Power and prices aggregate by mean.
- Temporal split by the year of `local_date`: train 2021–2024, validate 2025, test 2026. Never `randomSplit`, `CrossValidator` or `TrainValidationSplit`.
- 2025-04-28 (Iberian blackout) is excluded from training, validation and test. It stays in the data and in the UI.
- Demand target is `demand_mw` (peninsular, the 5-minute feed). Its baseline is REE's peninsular forecast `peninsular_forecast_mw`. REData's national `forecast_demand_mw` is never the demand baseline.
- Every model is a `Pipeline`. Transformers are fitted on training rows only, inside the pipeline.
- Hyperparameters are chosen on validation (2025) only. Test (2026) is evaluated once, after the choice.
- A model and its baseline are always scored on the same rows: rows where either is null are dropped from both.
- The model applied in streaming uses only features that arrive in real time. For demand that is temperature and calendar.
- Every new dataset column and every new series shown in the UI gets an entry in `catalog.py`; `test_every_ingested_column_is_described` enforces it for datasets.
- Tests never use the network. New real responses are stored trimmed in `tests/fixtures/`.
- Bulk downloads (the forecast history, about 2,140 daily files and about an hour) only with the team's permission.
- Never run `git commit`/`git push` unless the team asks in that same message. Each task ends with a proposed commit message.

## Review Focus

1. Leakage through the split. A scaler or encoder fitted on all rows, the blackout day left in validation, or test rows used for selection would make every metric optimistic. A reasonable person expects validation-only selection. Tests in Tasks 3 and 4.
2. Unfair baseline comparison. If the model is scored on hours where the forecast is missing (or the reverse), the comparison is between different rows. Test in Task 3.
3. Daylight-saving days in day-type profiles. A spring day has 23 local hours and an autumn day 25; a profile must still have 24 values. Test in Task 5.
4. The stream with no trained model, or with no temperature for an hour. The live readings and windows must keep working; the hour's expected value is null with a warning, never a crash. Test in Task 7.
5. Weather source per hour. In replay, temperature comes from `processed/weather`; live, from the landed current-weather files. An hour present in both must not be counted twice. Test in Task 7.

---

### Task 1: REE peninsular forecast source

**Files:**
- Create: `src/spain_electricity/sources/ree_forecast.py`, `tests/unit/test_ree_forecast.py`
- Create: `tests/fixtures/ree_forecast/2025/01/ree_forecast_2025-01-15.jsonp`. It is the real response of `prevProgPeninsula` for 2025-01-15 (one request, not a bulk download), trimmed like the `ree_generation` fixture.
- Modify: `src/spain_electricity/sources/ree_generation.py` (extract the owner-day filter), `src/spain_electricity/pipeline.py`, `src/spain_electricity/views.py` (`DATASETS`), `src/spain_electricity/catalog.py`, `tests/helpers.py` (`copy_raw_fixtures`), `tests/integration/test_batch_pipeline.py`

**Interfaces:**
- Consumes: `ree_generation.JSONP_PATTERN`, `ree_generation.with_utc(df, label_col="ts_label")`, `download.DownloadJob`.
- Produces in `ree_generation`: `keep_owner_day(readings: DataFrame) -> DataFrame`, which keeps rows whose `ts_label[:10]` equals column `file_day` and drops `file_day`. `ree_generation.read_raw` uses it, with no behavior change.
- Produces in `ree_forecast`:
  - `BASE_URL = "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/prevProgPeninsula"`
  - `RAW_FOLDER = "ree_forecast"`, `PROCESSED_FOLDER = "peninsular_forecast"`
  - `RAW_TO_COLUMN = {"pre": "peninsular_forecast_mw", "pro": "peninsular_scheduled_mw"}`
  - `build_url(day: date) -> str` (`?callback=cb&curva=DEMANDAAU&fecha=YYYY-MM-DD`)
  - `raw_path(paths, day) -> Path` (`raw/ree_forecast/YYYY/MM/ree_forecast_YYYY-MM-DD.jsonp`)
  - `is_valid_payload(content: bytes) -> bool` (starts with `cb(` and contains `valoresPrevistaProgramada`)
  - `jobs(start, end, paths) -> list[DownloadJob]`, with `settle_days=1` as for generation
  - `parse_payload(text_df, text_col="value") -> DataFrame`
  - `read_raw(spark, raw_root) -> DataFrame` with columns `ts_utc`, `peninsular_forecast_mw`, `peninsular_scheduled_mw`
  - `ingest(spark, paths) -> int`: writes `processed/peninsular_forecast` partitioned by UTC `year`, `month`
- Produces in `pipeline`: `SOURCES` gains `"ree_forecast"`, and `run_ingest` gains the step `("peninsular_forecast", "ree_forecast", "*.jsonp", …)` before the hourly table.

- [ ] **Step 1: Store the fixture and write the failing tests** in `tests/unit/test_ree_forecast.py`:
  - `test_build_url`: equals `BASE_URL + "?callback=cb&curva=DEMANDAAU&fecha=2025-01-15"`.
  - `test_is_valid_payload`: a fixture's bytes are True; `b"cb({\"valoresHorariosGeneracion\": []});"` and `b""` are False.
  - `test_parse_payload_maps_pre_and_pro`: the fixture parses to 361 rows; the first is `ts_label == "2025-01-14 21:00"` with `peninsular_forecast_mw == 38871` and `peninsular_scheduled_mw == 38696`.
  - `test_read_raw_keeps_the_owner_day`: `read_raw` over the fixture folder gives 288 rows, all on local 2025-01-15, the first at `2025-01-14 23:00` UTC.
  - `test_read_raw_handles_the_october_change`: a synthetic payload for 2025-10-26 with labels `02:00`, `2A:00`, `2B:00` gives three distinct UTC instants.
  - `test_ingested_forecast_is_described` in `tests/integration/test_batch_pipeline.py`: after `run_ingest` over the fixtures, `counts["peninsular_forecast"] == 288`. The existing column-description test then covers the new dataset.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_ree_forecast.py -q`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement** `keep_owner_day`, `ree_forecast`, the pipeline step, `views.DATASETS += ("peninsular_forecast",)`, and catalog entries: `Entry`s for both columns, with source `REE_FEED`, and `DATASETS["peninsular_forecast"]`.
- [ ] **Step 4: Run** `TZ=UTC uv run pytest -q`. Expected: all pass, the earlier `ree_generation` tests included.
- [ ] **Step 5: With the team's permission**, download the history with `uv run python scripts/download_history.py --sources ree_forecast --start 2021-01-01 --end <yesterday>`. Then run `uv run python scripts/ingest.py` and record the counts and failures in the task notes.
- [ ] **Step 6: Commit** `feat: ingest REE's peninsular demand forecast`

### Task 2: Hourly table additions

**Files:**
- Modify: `src/spain_electricity/config.py`, `src/spain_electricity/features.py`, `src/spain_electricity/catalog.py`, `AGENTS.md` (Iberian-exception dates and their source)
- Test: `tests/unit/test_features.py`

**Interfaces:**
- Produces in `config`:
  - `ANOMALY_DATES = (date(2025, 4, 28),)`
  - `IBERIAN_EXCEPTION = (date(2022, 6, 15), date(2023, 12, 31))`. Verify both dates against the BOE (Real Decreto-ley 10/2022 and the act that extended it) before coding, and cite the source in `AGENTS.md`'s decision row. If they differ, use the BOE dates.
  - `HEATING_BASE_C = 16.0`, `COOLING_BASE_C = 20.0`. Daily demand is lowest between about 16 and 20 °C in the Analysis scatter (2,139 days); that reading is visual, and it is recorded as such in AGENTS.md.
- Produces in `features`:
  - `add_degree_hours(df: DataFrame) -> DataFrame` adds `heating_degrees = greatest(0, HEATING_BASE_C − temperature_c)` and `cooling_degrees = greatest(0, temperature_c − COOLING_BASE_C)`, both null when the temperature is null. It has no actions, so it is usable on streams.
  - `add_flags(df: DataFrame) -> DataFrame` adds `is_anomaly` (`local_date` in `ANOMALY_DATES`) and `iberian_exception` (`local_date` between the two dates).
  - `add_lag_24h(df: DataFrame, column: str) -> DataFrame` adds `<column>_24h_ago`, joined on `hour_utc − 24 hours`; it is null when that hour is missing.
  - `build_hourly_features` gains `peninsular_forecast_mw` and `peninsular_scheduled_mw` (hourly mean, left join, nulls when the dataset is missing), `heating_degrees`, `cooling_degrees`, `is_anomaly`, `iberian_exception`, `spot_price_eur_mwh_24h_ago` and `renewable_share_24h_ago`.

- [ ] **Step 1: Write the failing tests:**
  - `test_add_degree_hours`: temperatures `[10.0, 18.0, 25.0, None]` give heating `[6.0, 0.0, 0.0, None]` and cooling `[0.0, 0.0, 5.0, None]`.
  - `test_add_flags`: local dates 2025-04-28, 2025-04-29, 2022-06-15, 2024-01-01 give `is_anomaly` `[True, False, False, False]` and `iberian_exception` `[False, False, True, False]`.
  - `test_add_lag_24h`: hours 10:00 on day 1, 10:00 on day 2 and 11:00 on day 2 with values 1, 2, 3 give `[None, 1.0, None]`.
  - Extend `test_build_hourly_features` with a `peninsular_forecast` dataset (two readings 100 and 300 at 10:00 and 10:05, giving 200) and assert the new columns. Extend `test_build_hourly_features_tolerates_missing_sources` to assert `peninsular_forecast_mw is None`.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_features.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement**, plus catalog entries for every new column.
- [ ] **Step 4: Run** `TZ=UTC uv run pytest -q`, then `uv run python scripts/ingest.py` on the real data. Expected: all pass, and `hourly_features` is still 51,336 rows (or more, if the history grew).
- [ ] **Step 5: Commit** `feat: add degree-hours, anomaly and regime flags and 24-hour lags to the hourly table`

### Task 3: Split and evaluation

**Files:**
- Create: `src/spain_electricity/models.py` (first part), `tests/unit/test_models.py`

**Interfaces:**
- Produces:
  - `SPLIT_YEARS = {"train": (2021, 2024), "validation": (2025, 2025), "test": (2026, 2026)}`
  - `split_by_year(hourly: DataFrame) -> dict[str, DataFrame]`, by `year(local_date)`, dropping `is_anomaly` rows.
  - `scored_rows(df: DataFrame, columns: Sequence[str]) -> DataFrame` drops rows with a null in any of `columns`.
  - `evaluate(predictions: DataFrame, label: str, prediction: str) -> dict[str, float]` returns keys `rmse`, `mae`, `r2` (`RegressionEvaluator`).

- [ ] **Step 1: Write the failing tests:**
  - `test_split_by_year_boundaries`: rows dated 2024-12-31, 2025-01-01, 2025-12-31 and 2026-01-01 land in train, validation, validation and test.
  - `test_split_drops_the_anomaly`: a row with `is_anomaly=True` in 2025 is in no split.
  - `test_evaluate`: labels `[1, 3]` against predictions `[2, 2]` give `rmse == 1.0`, `mae == 1.0` and `r2 == 0.0`.
  - `test_scored_rows_aligns_model_and_baseline`: three rows with the baseline null in one give two rows.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_models.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** it again. Expected: PASS.
- [ ] **Step 5: Commit** `feat: add the temporal split and regression metrics`

### Task 4: Regression pipelines, tuning and saving

**Files:**
- Modify: `src/spain_electricity/models.py`, `tests/unit/test_models.py`
- Create: `tests/integration/test_models.py`

**Interfaces:**
- Produces:
  - A `ModelSpec` frozen dataclass with fields `name: str`, `label: str`, `numeric: tuple[str, ...]`, `categorical: tuple[str, ...]`, `flags: tuple[str, ...]` and `baseline: str`.
  - `DEMAND = ModelSpec("demand", "demand_mw", ("temperature_c", "heating_degrees", "cooling_degrees"), ("local_hour", "day_of_week", "month"), ("is_holiday", "is_weekend"), "peninsular_forecast_mw")`
  - `PRICE = ModelSpec("price", "spot_price_eur_mwh", ("gas_price_eur_mwh", "renewable_share", "demand_mw"), ("local_hour",), ("iberian_exception",), "spot_price_eur_mwh_24h_ago")`
  - `RENEWABLES = ModelSpec("renewable_share", "renewable_share", ("wind_speed_100m_kmh", "shortwave_radiation_wm2"), ("local_hour", "month"), (), "renewable_share_24h_ago")`
  - `SPECS = (DEMAND, PRICE, RENEWABLES)`
  - `grids() -> dict[str, list[ParamMap]]`, keyed `linear`, `random_forest` and `gbt`, built with `ParamGridBuilder` on each family's estimator (the loop over them is by hand, as AGENTS.md requires), with these values:
    - linear: `regParam` in `[0.0, 0.1, 1.0]` × `elasticNetParam` in `[0.0, 0.5]`
    - random_forest: `numTrees` in `[50, 100]` × `maxDepth` in `[8, 12]`
    - gbt: `maxIter` in `[50, 100]` × `maxDepth` in `[5, 8]`
  - `build_pipeline(spec: ModelSpec, family: str) -> tuple[Pipeline, Estimator]` returns the pipeline and its estimator, which `grids()` uses. Flags are cast to double; categoricals go through `OneHotEncoder`, which takes the integer columns directly; the steps are `VectorAssembler`, `StandardScaler` (linear only) and the estimator, with `labelCol=spec.label`.
  - `tune(spec, splits, grids=None) -> dict` (defaults to `grids()`): per family, every grid point is fitted on train and scored on validation. It keeps the best per family, the overall best, and the baseline on the same validation rows.
  - `fit_final(spec, splits, best) -> tuple[PipelineModel, dict]`: refits the best on train plus validation and scores it and the baseline once on test.
  - `explain(model: PipelineModel, spec) -> list[dict]`: `{"feature", "weight"}` per expanded feature, using coefficients for linear and `featureImportances` for trees.
  - `save(paths, spec, model, report) -> Path`: writes `data/models/<name>/pipeline` (`PipelineModel.write().overwrite()`) and `data/models/<name>/metrics.json`, with keys `validation` (per family and baseline), `test` (best and baseline), `best` (family and params), `features`, `trained_at_utc` and `rows` (per split).
- Ruling: `RENEWABLES` is included because the description given to the lecturer says the mix is explained with Spark ML. Cost if the team decides otherwise: drop it from `SPECS`.
- Ruling: price and renewable-share baselines are persistence (same hour the day before), because REE publishes no forecast for them. Cost if wrong: one column per baseline.

- [ ] **Step 1: Write the failing tests.** Unit tests use a small synthetic hourly frame built in the test: two days per year in 2021–2026, with `demand_mw = 30000 + 200 * heating_degrees + 100 * local_hour`.
  - `test_build_pipeline_stages`: linear has a `StandardScaler` and trees do not, and the assembler inputs contain every numeric feature and flag.
  - `test_tune_selects_on_validation_only`: patch the test split to a frame whose labels make the second grid point win on validation and lose on test; `tune` picks the second.
  - `test_tune_scores_baseline_on_the_same_rows`: with one validation baseline null, the model and the baseline report the same `n`.
  - `test_explain_lists_one_weight_per_expanded_feature`.
  - In `tests/integration/test_models.py`, `test_save_writes_pipeline_and_metrics`: train `DEMAND` with a one-point grid per family on the synthetic frame. `save` writes a loadable `PipelineModel` and a `metrics.json` with the keys above, and the linear validation RMSE is below 1 MW on that noiseless frame.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_models.py tests/integration/test_models.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** them again, then the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat: train demand, price and renewable-share pipelines against their baselines`

### Task 5: Day types with KMeans

**Files:**
- Create: `src/spain_electricity/day_types.py`, `tests/unit/test_day_types.py`

**Interfaces:**
- Produces:
  - `daily_profiles(hourly: DataFrame) -> DataFrame`, with columns `local_date` and `profile`, a 24-value vector. It is the hourly demand divided by the day's mean, averaged by `local_hour`. On a 23-hour day, the missing hour 2 takes the mean of hours 1 and 3; on a 25-hour day, hour 2's two values are averaged. Anomaly days are excluded.
  - `fit_day_types(profiles: DataFrame, k_values: Sequence[int] = range(3, 9), seed: int = 42) -> tuple[KMeansModel, dict[int, float]]`: picks the k with the best silhouette (`ClusteringEvaluator`), and returns the model and the silhouette for each k.
  - `describe(assignments: DataFrame, hourly: DataFrame) -> DataFrame`: one row per cluster, with `day_type`, `days`, `mean_temperature_c`, `weekend_or_holiday_share`, `mean_renewable_share`, `mean_demand_mw`, and the mean profile (24 values). There are no automatic names; the UI shows the descriptors.
  - `save_day_types(paths, model, silhouettes, assignments) -> Path` writes `data/models/day_types/` (the model plus a `metrics.json` with silhouettes and the chosen k) and `processed/day_types` (`local_date`, `day_type`).

- [ ] **Step 1: Write the failing tests:**
  - `test_profile_has_24_values_with_mean_one`.
  - `test_profile_on_spring_change_fills_hour_two`, with local 2025-03-30 having 23 hours.
  - `test_profile_on_autumn_change_averages_hour_two`, with 2025-10-26.
  - `test_fit_day_types_separates_two_shapes`: ten synthetic days with a midday peak and ten with an evening peak, `k_values=[2]`, end in two clusters of ten.
  - `test_anomaly_days_have_no_profile`.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_day_types.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: find day types with KMeans on daily demand curves`

### Task 6: Training CLI

**Files:**
- Create: `scripts/train_models.py`, `tests/integration/test_train_models.py`
- Modify: `src/spain_electricity/models.py` (`train_all`)

**Interfaces:**
- Produces `models.train_all(spark, paths, specs=SPECS, grids=None, day_types=True) -> dict[str, dict]`. It reads `hourly_features` and, per spec, runs `tune`, `fit_final`, `explain` and `save`; then it fits and saves the day types. It returns each `metrics.json` content.
- CLI: `uv run python scripts/train_models.py [--models demand price renewable_share day_types]`. It logs per model the best family, the validation and test RMSE and the baseline RMSE, and calls `stop_spark()`.

- [ ] **Step 1: Write the failing test** `test_train_all_on_synthetic_history`: a temporary `hourly_features` with two days per year in 2021–2026, one-point grids and `k_values=[2]`. It writes `data/models/{demand,price,renewable_share,day_types}/metrics.json`.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/integration/test_train_models.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** it, then `uv run python scripts/train_models.py` on the real data. Record in the task notes, per model, validation and test RMSE against the baseline, the training time and the chosen k. Expected: all pass, and four model folders.
- [ ] **Step 5: Commit** `feat: add the training CLI`

### Task 7: Expected demand in the stream

**Files:**
- Modify: `src/spain_electricity/sources/open_meteo.py`, `src/spain_electricity/capture.py`, `src/spain_electricity/streaming.py`, `src/spain_electricity/catalog.py`
- Create: `tests/fixtures/open_meteo_current/20261112T154821Z.json` (a real capture, trimmed to the five cities), `tests/unit/test_expected_demand.py`
- Modify: `tests/integration/test_streaming.py`

**Interfaces:**
- Produces in `open_meteo`:
  - `read_current(spark, folder: Path) -> DataFrame` with columns `city`, `ts_utc`, `temperature_c`, `shortwave_radiation_wm2`, `wind_speed_100m_kmh`. The response is a list in `config.CITIES` order with no city names, so the list position maps to the city; `current.time` is UTC.
  - `is_valid_current_payload(content: bytes) -> bool`: a JSON list of `len(CITIES)` objects, each with `current.temperature_2m`. It closes the deferred minor about unvalidated live weather. `capture_once` uses it and skips (with a warning) invalid payloads.
- Produces in `capture`: `capture_once` also lands today's `prevProgPeninsula` file in `landing/ree_forecast/`, named like the generation captures and validated with `ree_forecast.is_valid_payload`. The generation stream does not read that folder.
- Produces in `streaming`:
  - `hourly_temperature(spark, paths, hours: DataFrame) -> DataFrame` (`hour_utc`, `temperature_c`). It reads national hourly weather from `processed/weather` for the hours it covers, and from `read_current` over the landing folder (mean of the captures in each hour) for the rest. An hour present in both takes the history value.
  - `hourly_forecast(spark, paths, hours: DataFrame) -> DataFrame` (`hour_utc`, `peninsular_forecast_mw`). It reads `processed/peninsular_forecast` for the hours it covers, and the landed forecast captures for the rest, with the same precedence as temperature.
  - `expected_demand(spark, paths, hourly_demand: DataFrame, model: PipelineModel | None) -> DataFrame` with columns `hour_utc`, `demand_mw`, `temperature_c`, `expected_demand_mw`, `peninsular_forecast_mw`, `gas_price_eur_mwh` (stream-static join on the local date with `processed/gas_price`), and `deviation_pct = (demand_mw − expected) / expected`. It applies `add_calendar`, `add_degree_hours` and the model. With no model or no temperature, `expected_demand_mw` is null; a missing forecast or gas price is null too.
  - `start_stream(...)` gains a third query, `expected`, when `data/models/demand/pipeline` exists. Readings go into `window(ts_utc, "1 hour")` with the existing watermark and the hourly mean demand, then `foreachBatch` → `expected_demand` → `stream/output/expected/batch_id=N`. Without a model it logs a warning once and starts only the two phase 1 queries.
- Catalog entries for `expected_demand_mw` and `deviation_pct`.

- [ ] **Step 1: Write the failing tests:**
  - `test_read_current_maps_cities_by_position`: the fixture gives five rows, and `madrid` has the first object's temperature.
  - `test_is_valid_current_payload`: the fixture is True; `b"[]"` and a four-city list are False.
  - `test_hourly_temperature_prefers_history`: an hour present in both sources takes the history value, with one row per hour.
  - `test_expected_demand_without_model_is_null`.
  - `test_expected_demand_without_temperature_is_null`.
  - `test_hourly_forecast_prefers_history`, the forecast counterpart of the temperature test.
  - `test_capture_lands_the_forecast`, with a `FakeSession` routing the three URLs: three files are landed.
  - In `tests/integration/test_streaming.py`, `test_stream_writes_expected_demand`: train a tiny `DEMAND` model on synthetic data into `paths.models`, then replay the 2025-01-15 fixture with `available_now=True`. The `expected` output has 24 hours with non-null `expected_demand_mw`.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest tests/unit/test_expected_demand.py tests/integration/test_streaming.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full suite. Then run a live check (capture, stream and UI for at least one hour), and record the hours written and their deviation in the task notes.
- [ ] **Step 5: Commit** `feat: compare live demand with the demand model's expectation`

### Task 8: Models in the UI

**Files:**
- Modify: `src/spain_electricity/views.py`, `src/spain_electricity/charts.py`, `src/spain_electricity/catalog.py`, `app/streamlit_app.py`
- Test: `tests/unit/test_views.py`, `tests/unit/test_charts.py`, `tests/integration/test_app.py`

**Interfaces:**
- Produces in `views`:
  - `model_reports(paths) -> dict[str, dict]` returns each `metrics.json`, empty when there are none.
  - `model_predictions(spark, paths, spec, start, end) -> pd.DataFrame` gives `hour_utc`, the actual value, `prediction` and the baseline over local dates.
  - `day_type_summary(spark, paths) -> pd.DataFrame` gives `describe` plus `days` per month.
  - `live_expected(spark, paths) -> pd.DataFrame` gives the latest version of each hour of `stream/output/expected`.
- Produces in `charts`:
  - `importance_chart(weights: pd.DataFrame, theme) -> alt.Chart`: horizontal bars for the top 15 by absolute weight, labeled.
  - `profiles_chart(summary: pd.DataFrame, theme) -> alt.LayerChart`: one line per day type over the local hour, with a legend.
- Produces in `app`:
  - A **Models** tab, after Analysis. It has a metrics table per model (each family on validation 2025, the best and the baseline on test 2026), actual vs prediction vs baseline for a chosen week, the feature-weight chart, and the day-type profiles with their descriptor table. With no `data/models`, it shows `st.info("No models yet: run scripts/train_models.py")`.
  - **Live** shows hourly actual demand against both the model's expectation and REE's peninsular forecast, and the latest deviation, when `stream/output/expected` exists.
  - Each chart has its "What these series are and where they come from" expander, fed by the catalog.

- [ ] **Step 1: Write the failing tests:**
  - `test_model_reports_empty_without_models`.
  - `test_model_reports_reads_metrics_json`.
  - `test_importance_chart_keeps_top_15_horizontal_labels`.
  - `test_profiles_chart_has_one_line_per_day_type`.
  - In `test_app.py`, `test_app_shows_models_tab`: the tabs are `["Data status", "History", "Live", "Analysis", "Models"]`; with no models the Models tab shows the info message; with the tiny models of Task 6 it shows no exception.
- [ ] **Step 2: Run** the three test files. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full suite, then the app on the real data. Render each new chart to PNG and look at it, as in phase 1, before calling the task done.
- [ ] **Step 5: Commit** `feat: show model results and day types in the UI`

### Task 9: Documentation and notebook

**Files:**
- Create: `docs/02-models.md`, `notebooks/02_models.ipynb`
- Modify: `README.md`, `AGENTS.md`, `docs/01-how-it-works.md` (link to 02)

**Content:**
- `docs/02-models.md` covers:
  - what each model predicts and from which features, and why those features (real-time availability);
  - the baselines and the split;
  - how to train;
  - the measured results of Task 6, against each baseline;
  - what the day types look like;
  - how the stream uses the demand model, and what must be running.
- README:
  - the new commands (forecast download with its measured time, `train_models.py`);
  - the Models tab;
  - "What must be running" updated: live expected demand needs a trained model;
  - the link to `docs/02-models.md`.
- AGENTS.md:
  - layout (`models.py`, `day_types.py`, `sources/ree_forecast.py`, `data/models/*`, `stream/output/expected`);
  - decisions (degree-hour bases, persistence baselines, the renewable-share model, the Iberian-exception dates with their BOE source);
  - the forecast's measured download facts;
  - remove the mix item from Open decisions.
- The notebook is thin: it loads `metrics.json` and draws the feature weights and the day-type profiles with the package's `charts`. It is qualitative, with few figures.

- [ ] **Step 1: Write the documents** from measured values only. Every figure carries its `n` or source.
- [ ] **Step 2: Run** `TZ=UTC uv run pytest -q` and `uv run ruff check . && uv run ruff format --check .`. Expected: all pass.
- [ ] **Step 3: Commit** `docs: explain the phase 2 models`
