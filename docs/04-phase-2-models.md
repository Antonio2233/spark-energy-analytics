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

The inputs:

- **Price:** daily gas, contemporaneous renewable share and demand, local hour, the legal-regime flag, and gas × regime, so the regime can change how gas sets the price.
- **Renewable share:** wind at 100 m, solar radiation, demand (the share's denominator), local hour and month.

Both follow the demand model's procedure:

1. Search the same `ParamGridBuilder` grids, looped by hand: linear regression (6 candidates), Random Forest (2) and GBT (4). Linear uses one-hot categories and training-fitted standardization; trees use the unscaled assembled features.
2. Fit each candidate on 2021–2024 and select by MAE on 2025 only.
3. Refit the winner on every valid 2021–2025 hour and evaluate it once on 2026.
4. Refit the same configuration on every valid hour through October 2026 (the update) and score it on November 2026, beside the 2021–2025 model.

All encoders and scalers are fitted inside each pipeline, and test rows never select parameters. Training rows need complete explanatory inputs and target. Evaluation rows also need a finite persistence baseline, so the model and reference scores use identical hours. Duplicate UTC hours stop training.

These are contemporaneous explanatory estimates. They do not demonstrate day-ahead forecasting skill because contemporaneous weather, renewable share and demand are not known a day ahead. The persistence baselines describe exact UTC t-24h. Negative prices are retained. Renewable share is renewable MW divided by demand MW and may exceed one during exports; predictions are not clipped to [0,1].

MAE and RMSE use EUR/MWh for price and fractions for renewable share (multiply by 100 for percentage points). R-squared may be negative.

## KMeans day types

Only complete local days with finite positive demand enter clustering. A day must contain all 23, 24 or 25 distinct UTC hours expected for Europe/Madrid. Duplicate hours, arbitrary gaps and the blackout are rejected. The repeated autumn local hour is averaged; only the missing spring hour 2 is interpolated from hours 1 and 3. Each resulting 24-value vector is divided by its mean, so clustering compares shapes rather than total demand.

Fit KMeans for k=3 through 8 using complete 2021-2024 profiles, as a one-stage Spark ML `Pipeline`. Select the best training silhouette, and assign other complete days to those learned centers. This silhouette describes the fitted training partition, not held-out generalization. The descriptor table summarizes temperature, renewable share, nonworking-day frequency, mean demand and the profile's peak hour, with equal weight per day.

KMeans numbers the groups arbitrarily. Each group is then named from its descriptors, which is an inference:

- a group whose days are mostly weekends or holidays is a **weekend or holiday**; the others are **working days**;
- within each kind, the warmest group is **warm** and the coolest **cool**.

KMeans itself only sees the normalized shape.

## Models tab and notebook

Models shows the three regressions the same way: the selected configuration, the paired scores against the baselines, one month of saved predictions with its largest errors, and the feature weights. A **Model version** selector at the top switches all three between the 2021–2025 model (validation 2025 and test 2026) and the update (November 2026, beside the 2021–2025 model on the same hours). Below them come the KMeans silhouettes, the mean profile of each named day type and the descriptors. Tree importance is nonnegative and normalized; linear coefficients are for standardized numerical/encoded inputs. Their magnitudes are not directly comparable across model families and cannot establish causality. Correlated variables may divide importance between them.

The thin notebook calls package functions and draws the saved reports. It makes no downloads and does not fit models. Training artifacts carry input row counts and measured runtime. Cluster summaries refer only to complete days present in the saved input.


## Measured full-history results (24 November 2026)

The models use the full 2021–2026 history:

| Target | Training hours | Paired validation hours | Paired test hours | Refit hours (2021–2025) |
|---|---:|---:|---:|---:|
| Price | 35,040 | 8,712 | 7,848 | 43,776 |
| Renewable share | 35,063 | 8,712 | 7,848 | 43,799 |

Training, evaluation, the update and saving took 154 s for price and 99 s for renewable share, on 12 cores. All four models together took about 7 minutes.

Best validation MAE of each family (2025):

| Target | Linear regression (6) | Random Forest (2) | GBT (4) | Selected |
|---|---:|---:|---:|---|
| Price (EUR/MWh) | 21.25 | 21.83 | **20.85** | GBT, 120 iterations, depth 6 |
| Renewable share (fraction) | 0.1089 | **0.1043** | 0.1085 | Random Forest, 60 trees, depth 12 |

The selected model against the same hour 24 hours earlier:

| Target | Split | MAE, model | MAE, t-24h | RMSE, model | RMSE, t-24h | R², model | R², t-24h | Bias, model |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Price (EUR/MWh) | Validation 2025 | 20.85 | 19.94 | 26.82 | 28.87 | 0.682 | 0.632 | −2.43 |
| | Test 2026 | 34.62 | 22.66 | 45.07 | 36.84 | 0.664 | 0.775 | −17.21 |
| Renewable share (fraction) | Validation 2025 | 0.1043 | 0.0698 | 0.1265 | 0.0905 | 0.294 | 0.639 | +0.0866 |
| | Test 2026 | 0.1326 | 0.0632 | 0.1625 | 0.0876 | 0.205 | 0.769 | +0.1102 |

The bias is the mean of actual minus estimate. Validation scores come from the 2021–2024 fit that was selected, and test scores from the refit.

**Price.** With contemporaneous inputs only, the model explains 66% of the 2026 variation in price. In the GBT, gas has 48% of the importance, renewable share 23%, demand 13%, gas × Iberian exception 7% and the regime flag 2%. In 2026 it overestimates the price by 17.2 EUR/MWh on average. The earlier compact search (Random Forest, depth 7, no interaction) reached R² 0.573 and MAE 39.6 on the same test.

**Renewable share.** The model explains 21% of the 2026 variation, with radiation (46% of the importance), wind (25%) and demand (8%). It underestimates 2026 by 11.0 percentage points on average. Its inputs carry no trend, while the share rises from 0.456 in 2021 to 0.608 in 2026 (hourly means; 0.446, 0.537, 0.589 and 0.577 in between).

This model is worse on 2026 than the earlier compact one (MAE 0.117, R² 0.417, bias +8.9 points), although its 2025 score is slightly better (0.1043 against 0.1049). The likely cause has not been tested: demand is now an input, and 2026 demand runs above every training year. A model that learned "more demand, lower share" then expects a lower share in 2026, while the share actually rose. Following the protocol, no input was changed after seeing the test.

**Persistence wins on 2026.** For both targets, the same hour 24 hours earlier is more accurate than the model. These are explanatory models, which say how much each driver accounts for, not forecasts.

### Does training on 2026 help?

The update refits the selected configuration on every valid hour through 31 October 2026: 51,072 hours for price and 51,095 for renewable share. It is scored on the 552 paired hours of 1–23 November 2026, beside the 2021–2025 model on the same hours.

| Target | Source | MAE | RMSE | R² | Bias |
|---|---|---:|---:|---:|---:|
| Price (EUR/MWh) | Update, trained through October 2026 | **37.86** | 50.89 | 0.656 | −3.02 |
| | Model trained on 2021–2025 | 44.25 | 59.91 | 0.523 | −18.12 |
| | Same hour 24 h earlier | 52.33 | 72.60 | 0.299 | −0.16 |
| Renewable share (fraction) | Update, trained through October 2026 | 0.1154 | 0.1357 | 0.436 | +0.0641 |
| | Model trained on 2021–2025 | 0.1217 | 0.1433 | 0.372 | +0.0719 |
| | Same hour 24 h earlier | **0.0974** | 0.1291 | 0.490 | +0.0038 |

- **Price:** the 2026 data removes most of the bias (−18.1 to −3.0 EUR/MWh) and cuts the MAE by 14%. In November both models beat persistence, which explains only 30% of a volatile month.
- **Renewable share:** the update improves a little (MAE −5%) but still underestimates by 6.4 points, and persistence remains better. Ten months of 2026 among 51,095 hours do not give the model the trend.

These are 552 hours of a single month, so they show the direction of the effect, not its size over a year.

### Day types

KMeans tried k = 3 to 8 on 1,461 complete training days (2021–2024); k = 3 had the best silhouette, 0.426. The other values were 0.406, 0.379, 0.383, 0.389 and 0.396. All 2,152 complete days were assigned. The names come from the rule above:

| Type (KMeans number) | Days | Mean temperature | Weekend or holiday | Mean demand (MW) | Profile peak (local) |
|---|---:|---:|---:|---:|---:|
| Weekend or holiday (0) | 597 | 16.8 °C | 97% | 24,225 | 21:00 |
| Cool working day (1) | 962 | 13.2 °C | 0% | 28,148 | 20:00 |
| Warm working day (2) | 593 | 23.6 °C | 14% | 29,071 | 13:00 |

KMeans numbered the groups differently in the previous run (warm 0, cool 1, day off 2), which is why the UI shows the names. These are descriptive associations, not fixed labels for future dates.

An earlier run on the compact seasonal sample used 1,536 training hours and 288 paired evaluation hours per target. Persistence beat both regressions there too: price test MAE was 28.8 against 15.5 EUR/MWh, and renewable share 0.159 against 0.069.

The saved prediction tables include the explanatory inputs as well as the target, baseline and estimate, allowing offline inference checks from a checkout with no raw/processed history.
