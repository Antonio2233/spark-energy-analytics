# Energy Desk: market context over the live stream

Live is the opening tab of the application. The dark Energy Desk combines the physical electricity system, reference prices, weather and the saved demand model. History, Analysis, Models and Data status remain available as supporting views. History uses peninsular demand rather than requiring the optional national REData series; missing price series are omitted instead of plotted empty. Analysis selects nonconstant variables with at least 24 non-null observations, so absent gas/price datasets do not disable all correlations.

## What the desk shows

- Six headline cards: demand, hourly day-ahead spot, renewable output/demand, combined-cycle output/demand, daily MIBGAS gas and an illustrative fuel-only spread. Changes use an observation exactly one hour earlier; a missing comparison stays unavailable.
- Demand against the hourly Spark ML estimate and peninsular REE forecast.
- Generation stack and current shares of eight tracked technologies. The stack is not a complete system balance, and its shares have a different denominator from renewable output/demand.
- Residual demand, window peak, wind, nuclear and signed interchange. REE's interchange sign is retained without an unverified import/export label.
- The displayed day's published day-ahead delivery curve, including remaining delivery hours, plus a fuel-cost scenario. These are delivery prices, not live transaction ticks.
- Hourly price/residual-demand co-movement and five-city temperature, wind at 100 m and radiation. Co-movement does not establish causation.
- Complete/provisional/missing-weather model states, residual alerts, source-period coverage and quarter-hour stream windows.

The sidebar selects a 6/12/24/48-hour observation window and a 40–65% conversion-efficiency scenario. All display times are UTC. The horizon ends at the newest processed event, not at the wall clock when replaying history. Results sent to the browser are bounded to at most 576 five-minute readings and 49 hourly buckets.

## Replay versus real time

The header prominently distinguishes HISTORICAL VIEW, legacy HISTORICAL REPLAY, LIVE CAPTURE, stale live data and saved output with unknown capture mode. The exact event date/time is always shown. Historical data never presents an old gas or electricity price as today's market.

For normal operation, start the application once:

```bash
uv run streamlit run app/streamlit_app.py
```

In the sidebar choose **Data source → Real time**. A background controller starts the first capture and the Structured Streaming queries automatically. The page stays responsive while connecting and offers a retry action on failure. Choose **Historical** to select a downloaded local day; this view reads batch generation and saved evaluation estimates without requiring replay commands. Model evaluation estimates are available for sampled 2025/2026 dates and remain unavailable for other dates.

Capture polls generation and weather every five minutes. Market context adds daily REData price and peninsular forecast snapshots, throttled to at most once per 15 minutes. Files land atomically. Failed market refreshes retain the previous snapshot, and source matching/freshness remains visible. A missing same-day gas reference is refreshed in an isolated, committed daily snapshot; failures leave other sources running and retry after 30 minutes. A matching existing gas reference is reused, so gas workbooks are not polled every five minutes.

The controller is cached once per data root and shares the app's Spark session. Browser sessions send a heartbeat every ten seconds while Real time is selected. Capture and the controller's queries pause when the last viewer switches away or its 90-second lease expires. Multiple viewers do not create duplicate captures or stop each other's connection. Errors are shown with an explicit retry action. Application shutdown stops owned work without stopping another job's queries.

The generation readings, deduplication, quarter-hour windows and hourly model monitoring remain genuine Structured Streaming queries. The desk additionally uses Spark to join the processed stream output with static reference/capture snapshots at serving time; prices and gas are not falsely described as extra streaming queries. The connection/desk fragment refreshes every ten seconds and caches each bounded desk snapshot for 20 seconds. In-progress foreachBatch outputs are ignored until their `_SUCCESS` marker exists.

The UI uses `DataPaths.with_stream("live")`: landing, checkpoints, outputs and optional live gas snapshots are under `data/stream_live`, while raw history, processed history and models stay shared. Select Replay to show the CLI replay output; replay state remains under `data/stream`. Cache keys include source namespace and historical date, so switching views cannot return a cached result from the other source. No cleanup or state deletion is required when using the selector. Advanced CLI workflows must still avoid mixing replay and live in their legacy namespace.

On 16 November 2026, the real-browser verification switched from 11 October 2026 at 21:55 UTC to live observations from 16 November 2026 at 18:10 UTC, then back to the chosen historical day. The existing replay fingerprint remained unchanged. Offline tests additionally cover multi-viewer leases, automatic pause, partial-startup cleanup, explicit retry and view isolation.

## Aligning the sources

In replay mode, prices, forecasts and weather come from processed historical datasets. In live mode, prices/forecasts/weather come from their live landing snapshots, with no silent historical fallback. Weather requires a matching observation across all five cities for the national value. Electricity prices and forecasts join on UTC hour; gas joins on the matching Europe/Madrid delivery date and is never forward-filled from another day. Missing values stay unavailable rather than becoming zero.

Source health reports whether a matching observation/delivery period exists. It does not assert that a retrospectively downloaded revision was available at that historical publication time. Gas is a reference price and PVPC a regulated consumer price; neither is a tradable real-time order-book quote.

## Small market preparation

For the existing October 2026 replay:

```bash
uv run python scripts/prepare_desk_market.py --month 2026-10 --forecast-day 2026-10-12
```

This downloads one month of electricity prices, one annual gas workbook and an optional single forecast day, then reuses the existing ingestion pipeline. The extra forecast covers the next-day overlap in REE's final replay file. It neither downloads another full demand history nor retrains the demand model. The first enriched preparation yielded 3,725 price observations, 320 gas delivery days and 9,504 five-minute peninsular forecasts; the hourly sample remained 2,304 rows. Other delivery months can be selected explicitly.

## Fuel-only scenario

With electricity and gas quoted in €/MWh and conversion efficiency η on the same energy basis as the gas quote:

```text
fuel_cost_equivalent = daily_gas_reference / η
fuel_only_spread = hourly_day_ahead_spot − fuel_cost_equivalent
```

The efficiency slider is a scenario assumption, not a calibrated plant specification. CO₂, operating, network, finance and fixed costs are excluded. The result is neither net profit nor a clean spark spread. The general electricity-minus-required-fuel-cost construction follows the [EIA's introduction to spark spreads](https://www.eia.gov/todayinenergy/detail.php?id=9911).

## Verification

Tests cover exact observation-hour comparisons, daily gas alignment, future price delivery periods, negative prices, unavailable live context, capture throttling/error retention, adaptive correlation selection and rendering with sparse streamed data. Only the trained model is shared in Git; market/history data and live/replay state remain local. Browser screenshots and the temporary visual-test server are local verification tools, not shared environment instructions.

## UI refresh and chart themes

Window and hourly-monitoring views cache bounded results by stream folder and committed batch versions for at most 60 seconds. A new committed batch changes the cache key immediately; empty or unfinished batches are not cached as results. Window results are limited to 192 quarter-hour windows (48 hours), and hourly monitoring to 48 hours. The cache avoids repeated Spark reads on unchanged refreshes, but a new batch still recomputes the latest state over committed batches. This is a UI cache, not incremental stream state.

All Energy Desk charts receive the selected light or dark theme, including titles, the supply-mix center label and delivery-price scenarios.
