# Phase 2: explanatory models and daily demand types

## Reproduce the compact experiment

Use the normal Java/uv setup in README or README_MAC. No Kafka or new service is required.

```bash
uv run python scripts/prepare_demand.py
uv run python scripts/prepare_phase2.py
uv run python scripts/train_models.py
uv run streamlit run app/streamlit_app.py
```

The existing seasonal demand sample covers 96 days, four days in each of January, April, July and October for 2021-2026. `prepare_phase2.py` fetches only the corresponding 24 monthly price responses and six annual MIBGAS workbooks. Downloads are resumable. Raw and processed data remain local. The default training command keeps the saved demand pipeline and trains price, renewable share and day types. Use `--models price`, `--models renewable_share` or `--models day_types` to retrain one part. `--models demand` explicitly retrains demand.

Saved models and small result artifacts live in `data/models/<name>/`. A checkout can inspect results in Models and run `notebooks/02_models.ipynb` without downloading history or retraining. Full History and historical Energy Desk views still require preparation. The new models are explanatory batch models; this phase does not add new price or renewable inference queries to the live controller. The existing demand streaming model remains in use.

## Hourly table

- `is_anomaly` marks the local date 2025-04-28. It stays in historical data, but model and clustering inputs exclude it.
- `iberian_exception` marks delivery dates from 2022-06-15 to 2023-12-31 inclusive. It identifies a legal regime, not whether the cap actually activated on a particular day or the amount of the consumer adjustment.
- `spot_price_eur_mwh_24h_ago` and `renewable_share_24h_ago` join the observation exactly 24 elapsed UTC hours earlier. They are not row offsets or local-clock-day matches. Missing hours remain null, including sample-block boundaries and dates around DST changes.

The start uses the first delivery day specified by [Order TED/517/2022](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2022-9435): the mechanism starts operating on 14 June for the auction delivering on 15 June. [Royal Decree-law 3/2023](https://www.boe.es/buscar/doc.php?id=BOE-A-2023-7937) extends the mechanism through 31 December 2023.

## Price and renewable-share estimation

Price uses daily gas, contemporaneous renewable share and demand, local hour and the legal-regime flag. Renewable share uses wind at 100 m, solar radiation, local hour and month. Both compare linear regression and Random Forest in Spark ML Pipelines. Linear uses one-hot categories and training-fitted standardization; trees use the unscaled assembled features. A small ParamGridBuilder search has two points per family. This keeps local runs short rather than using the larger original plan grid or adding GBT.

Fit candidates on 2021-2024 and select by MAE on 2025 only. Refit the selected configuration on train plus the usable paired validation rows and evaluate once on 2026. All encoders and scalers are fitted inside each pipeline. Test rows never select parameters. Training rows need complete explanatory inputs and target; validation and test also need a finite persistence baseline, so the model and reference scores use identical hours. Duplicate UTC hours stop training.

These are contemporaneous explanatory estimates. They do not demonstrate day-ahead forecasting skill because contemporaneous weather, renewable share and demand are not known a day ahead. The persistence baselines describe exact UTC t-24h. Negative prices are retained. Renewable share is renewable MW divided by demand MW and may exceed one during exports; predictions are not clipped to [0,1].

MAE and RMSE use EUR/MWh for price and fractions for renewable share (multiply by 100 for percentage points). R-squared may be negative. The small seasonal sample does not support full-year conclusions, and full-history training/evaluation remains required for final delivery.

## KMeans day types

Only complete local days with finite positive demand enter clustering. A day must contain all 23, 24 or 25 distinct UTC hours expected for Europe/Madrid. Duplicate hours, arbitrary gaps and the blackout are rejected. The repeated autumn local hour is averaged; only the missing spring hour 2 is interpolated from hours 1 and 3. Each resulting 24-value vector is divided by its mean, so clustering compares shapes rather than total demand.

Fit KMeans for k=3 through 8 using complete 2021-2024 profiles. Select the best training silhouette, and assign other complete days to those learned centers. This silhouette describes the fitted training partition, not held-out generalization. IDs are arbitrary, without automatic labels such as cold or sunny. The descriptor table summarizes temperature, renewable share, nonworking-day frequency and mean demand, with equal weight per day.

## Models tab and notebook

Models shows the saved demand results, validation candidates and paired test metrics for the new targets, up to 168 saved test hours per target, feature-weight charts, KMeans silhouette scores, mean profiles and cluster descriptors. Tree importance is nonnegative and normalized; linear coefficients are for standardized numerical/encoded inputs. Their magnitudes are not directly comparable across model families and cannot establish causality. Correlated variables may divide importance between them.

The thin notebook calls package functions and draws the saved reports. It makes no downloads and does not fit models. Training artifacts carry input row counts and measured runtime. Cluster summaries refer only to complete days present in the saved input.


## Measured compact-sample results (24 November 2026)

Both new regressions used 1,536 training hours and 288 paired validation/test hours each. The 24-hour baseline is missing on the first day of each seasonal block, removing 96 of the 384 annual evaluation hours. These are seasonal sample results, not full-year evaluations.

| Target | Selected family | Test MAE, model | Test MAE, t-24h | Test RMSE, model | Test RMSE, t-24h | Model R-squared |
|---|---|---:|---:|---:|---:|---:|
| Price (EUR/MWh) | Linear, regParam=0.1 | 28.756 | 15.493 | 34.314 | 21.383 | 0.730 |
| Renewable share (fraction) | Random Forest, depth=7 | 0.15861 | 0.06852 | 0.18702 | 0.08651 | 0.050 |

The renewable MAEs correspond to 15.861 and 6.852 percentage points. Persistence beats the learned models on this held-out sample. This is a limitation to report, not a reason to select or tune a different model on the test. Five-city wind/radiation is only a proxy for conditions across generating assets; it does not cover every wind/solar installation. Train-and-evaluate runtimes recorded in the reports were 49.42 seconds for price and 38.39 seconds for renewable share on the local run, excluding downloads and feature preparation.

KMeans selected k=3 with a training silhouette of 0.4642, using 64 training days and assigning 96 days. The saved types have 44, 23 and 29 days respectively. Type 0 contains no weekend/holiday days in this sample, type 1 has an 82.6% weekend/holiday share, and type 2 has the highest mean temperature (21.7 C). These are descriptive associations in the sample, not fixed labels for future dates.

The saved prediction tables include the explanatory inputs as well as the target, baseline and model prediction, allowing offline inference checks from a checkout with no raw/processed history.
