# Phase 2: explanatory models and daily demand types

## Reproduce

Use the normal Java/uv setup in README or README_MAC. No Kafka or new service is required.

```bash
uv run python scripts/download_history.py --start 2021-01-01 --end <yesterday>
uv run python scripts/ingest.py
uv run python scripts/train_models.py
uv run streamlit run app/streamlit_app.py
```

`train_models.py` trains the four models by default: demand, price, renewable share and day types. Use `--models price`, `--models renewable_share`, `--models day_types` or `--models demand` to retrain one part; each run writes only the folders of the models it trains.

For a quick experiment without the full download, `scripts/prepare_demand.py` and `scripts/prepare_phase2.py` fetch the seasonal sample. It covers 96 days (four in each of January, April, July and October for 2021–2026), 24 monthly price responses and six annual MIBGAS workbooks. Its results describe the sampled dates only.

Saved models and small result artifacts live in `data/models/<name>/` and are versioned. A checkout can inspect results in Models and run `notebooks/02_models.ipynb` without downloading history or retraining. Historical views and the Energy Desk still need the downloaded and ingested data.

The price and renewable-share models are explanatory batch models: this phase adds no live inference for them. The demand model remains the one used in streaming.

## Hourly table

- `is_anomaly` marks the local date 2025-04-28. It stays in historical data, but model and clustering inputs exclude it.
- `iberian_exception` marks delivery dates from 2022-06-15 to 2023-12-31 inclusive. It identifies a legal regime, not whether the cap actually activated on a particular day or the amount of the consumer adjustment.
- `spot_price_eur_mwh_24h_ago` and `renewable_share_24h_ago` join the observation exactly 24 elapsed UTC hours earlier. They are not row offsets or local-clock-day matches. Missing hours remain null, including sample-block boundaries and dates around DST changes.

The start uses the first delivery day specified by [Order TED/517/2022](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2022-9435): the mechanism starts operating on 14 June for the auction delivering on 15 June. [Royal Decree-law 3/2023](https://www.boe.es/buscar/doc.php?id=BOE-A-2023-7937) extends the mechanism through 31 December 2023.

## Price and renewable-share estimation

Price uses daily gas, contemporaneous renewable share and demand, local hour and the legal-regime flag. Renewable share uses wind at 100 m, solar radiation, local hour and month. Both compare linear regression and Random Forest in Spark ML Pipelines. Linear uses one-hot categories and training-fitted standardization; trees use the unscaled assembled features. A small ParamGridBuilder search has two points per family. This keeps local runs short rather than using the larger original plan grid or adding GBT.

Fit candidates on 2021-2024 and select by MAE on 2025 only. Refit the selected configuration on train plus the usable paired validation rows and evaluate once on 2026. All encoders and scalers are fitted inside each pipeline. Test rows never select parameters. Training rows need complete explanatory inputs and target; validation and test also need a finite persistence baseline, so the model and reference scores use identical hours. Duplicate UTC hours stop training.

These are contemporaneous explanatory estimates. They do not demonstrate day-ahead forecasting skill because contemporaneous weather, renewable share and demand are not known a day ahead. The persistence baselines describe exact UTC t-24h. Negative prices are retained. Renewable share is renewable MW divided by demand MW and may exceed one during exports; predictions are not clipped to [0,1].

MAE and RMSE use EUR/MWh for price and fractions for renewable share (multiply by 100 for percentage points). R-squared may be negative.

## KMeans day types

Only complete local days with finite positive demand enter clustering. A day must contain all 23, 24 or 25 distinct UTC hours expected for Europe/Madrid. Duplicate hours, arbitrary gaps and the blackout are rejected. The repeated autumn local hour is averaged; only the missing spring hour 2 is interpolated from hours 1 and 3. Each resulting 24-value vector is divided by its mean, so clustering compares shapes rather than total demand.

Fit KMeans for k=3 through 8 using complete 2021-2024 profiles. Select the best training silhouette, and assign other complete days to those learned centers. This silhouette describes the fitted training partition, not held-out generalization. IDs are arbitrary, without automatic labels such as cold or sunny. The descriptor table summarizes temperature, renewable share, nonworking-day frequency and mean demand, with equal weight per day.

## Models tab and notebook

Models shows the saved demand results, validation candidates and paired test metrics for the new targets, up to 168 saved test hours per target, feature-weight charts, KMeans silhouette scores, mean profiles and cluster descriptors. Tree importance is nonnegative and normalized; linear coefficients are for standardized numerical/encoded inputs. Their magnitudes are not directly comparable across model families and cannot establish causality. Correlated variables may divide importance between them.

The thin notebook calls package functions and draws the saved reports. It makes no downloads and does not fit models. Training artifacts carry input row counts and measured runtime. Cluster summaries refer only to complete days present in the saved input.


## Measured full-history results (24 November 2026)

The models use the full 2021–2026 history:

| Target | Training hours | Paired validation hours | Paired test hours |
|---|---:|---:|---:|
| Price | 35,040 | 8,736 | 7,848 |
| Renewable share | 35,063 | 8,736 | 7,848 |

Training, evaluation and saving took 12.7 s for price, 10.1 s for renewable share and 10.7 s for day types, on 12 cores.

| Target | Selected | Split | MAE, model | MAE, t-24h | RMSE, model | RMSE, t-24h | R², model | R², t-24h | Bias, model |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Price (EUR/MWh) | Random Forest, depth 7 | Validation 2025 | 25.40 | 19.92 | 30.61 | 28.85 | 0.587 | 0.633 | |
| | | Test 2026 | 39.61 | 22.66 | 50.78 | 36.84 | 0.573 | 0.775 | −17.56 |
| Renewable share (fraction) | Random Forest, depth 7 | Validation 2025 | 0.1049 | 0.0702 | 0.1279 | 0.0916 | 0.278 | 0.630 | |
| | | Test 2026 | 0.1173 | 0.0632 | 0.1392 | 0.0876 | 0.417 | 0.769 | +0.0885 |

The bias is the mean of actual minus model, measured on the saved 2026 predictions. The reports do not yet store it for validation.

**Price.** With contemporaneous inputs only, the model explains 57% of the 2026 variation in price. In the Random Forest, gas has 59% of the importance, renewable share 25%, the Iberian-exception regime 8% and demand 6%. In 2026 it overestimates the price by 17.6 EUR/MWh on average.

**Renewable share.** The model explains 42% of the variation, with radiation (56% of the importance) and wind (25%). It underestimates 2026 by 8.9 percentage points on average. Its inputs carry no trend, while the share rises from 0.456 in 2021 to 0.608 in 2026 (hourly means; 0.446, 0.537, 0.589 and 0.577 in between).

**Persistence wins.** For both targets, the same hour 24 hours earlier is more accurate than the model. These are explanatory models, which say how much each driver accounts for, not forecasts. Two follow-ups would improve them: a capacity or recent-level input for the renewable share, and a gas × regime interaction for price.

**Day types.** KMeans tried k = 3 to 8 on 1,461 complete training days (2021–2024); k = 3 had the best silhouette, 0.427. The other values were 0.407, 0.397, 0.382, 0.384 and 0.396. All 2,152 complete days were assigned. The IDs are arbitrary; the descriptors suggest these readings:

| Type | Days | Mean temperature | Weekend or holiday | Mean demand (MW) | Profile peak (local) |
|---|---:|---:|---:|---:|---:|
| 0 · warm working day | 594 | 23.6 °C | 13% | 29,097 | 13:00 |
| 1 · cool working day | 960 | 13.2 °C | 0% | 28,133 | 20:00 |
| 2 · day off | 598 | 16.8 °C | 97% | 24,228 | 21:00 |

These are descriptive associations, not fixed labels for future dates.

An earlier run on the compact seasonal sample used 1,536 training hours and 288 paired evaluation hours per target. Persistence beat both regressions there too: price test MAE was 28.8 against 15.5 EUR/MWh, and renewable share 0.159 against 0.069.

The saved prediction tables include the explanatory inputs as well as the target, baseline and model prediction, allowing offline inference checks from a checkout with no raw/processed history.
