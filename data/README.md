# Ready-to-run teaching snapshot

This curated snapshot is included so a fresh checkout can open Historical, Models and the Energy Desk without downloading the history or fitting a new model.

| Folder/file | Content |
|---|---|
| `raw/` | Original REE generation/forecast JSONP, Open-Meteo and REData JSON, and the MIBGAS workbook plus its CSV conversion |
| `processed/` | Spark Parquet datasets and the joined hourly feature table |
| `models/demand/pipeline/` | Saved Spark ML `PipelineModel`, including fitted preprocessing and Random Forest |
| `models/demand/predictions/` | Saved validation/test predictions for the Models panel |
| `models/demand/metrics.json` | Measured metrics, selected parameters, training audit and sample provenance |
| `demand_sample.json` | Exact requested dates and download outcome |
| `snapshot_manifest.json` | Byte sizes and SHA-256 hashes of the snapshot artifacts |

The model was fitted on 1,536 hourly observations from 64 sampled days in 2021–2024, selected on 384 validation hours from 2025, and evaluated on 384 test hours from 2026. The 96 sampled days are four-day blocks in January, April, July and October of each year. These are real observations, not synthetic training data. The selected Random Forest has 24 trees and depth 8. See [demand modeling](../docs/02-demand-model.md) for accuracy and limitations.

Sources: [REE peninsular demand/generation and forecast](https://demanda.ree.es/visiona/home), [REE REData](https://www.ree.es/en/datos/apidata), [Open-Meteo archive](https://open-meteo.com/en/docs/historical-weather-api), [MIBGAS](https://www.mibgas.es/es/file-access), and the national calendar computed in the package. Variable meanings, units and source mappings are in `src/spain_electricity/catalog.py`.

## Launch

Install Java and the dependencies, then start the app as described in [README_MAC.md](../README_MAC.md) or the [main README](../README.md). Use the sidebar's Historical / Real time selector. No training step is needed for this snapshot.

`data/.tools`, `data/stream`, `data/stream_live`, temporary files and Spark CRC sidecars stay excluded from Git. The live controller writes only to its isolated namespace; selecting Real time does not overwrite this snapshot. The dependency lock pins the PySpark version used to save the model. Local virtual environments are created separately on each machine.

The preparation and training scripts remain available for deliberate experiments. Running them can change tracked snapshot artifacts; review those changes before sharing a new snapshot. Streaming checkpoints and live captures should not be committed.
