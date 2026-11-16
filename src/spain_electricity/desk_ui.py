"""Live-first Energy Desk presentation over bounded, timestamp-aligned Spark results."""

from datetime import UTC, date, datetime
from html import escape

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from spain_electricity import catalog, charts, desk, views
from spain_electricity.config import DataPaths

STYLE = """
<style>
.block-container { max-width: 1600px; padding: 2rem 2.6rem 3rem; }
[data-testid="stSidebar"] { border-right: 1px solid #26354b; }
[data-testid="stMetricValue"] { font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
 font-size: 1.65rem; letter-spacing: -.06em; }
[data-testid="stMetricLabel"] { color: #9caec6; font-size: .8rem; }
[data-testid="stMetricDelta"] { font-size: .76rem; }
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p { color: #9bacc3 !important; }
[data-baseweb="tab-list"] { gap: 1.8rem; border-bottom: 1px solid #26354b; padding-bottom: .4rem; }
button[data-baseweb="tab"] { font-size: .86rem; }
h1 { font-size: 2.1rem !important; font-weight: 650 !important; letter-spacing: -.045em; }
h2, h3 { letter-spacing: -.025em; }
.desk-eyebrow { color: #7592b7; text-transform: uppercase; letter-spacing: .18em; font-size: .67rem;
 font-weight: 700; margin-bottom: .45rem; }
.desk-head { display: flex; justify-content: space-between; align-items: center; gap: 1rem;
 padding: 1.2rem 0 1.3rem; }
.desk-head h1 { margin: 0; padding: 0 !important; }
.desk-sub { color: #95a9c5; font-size: .82rem; margin-top: .55rem; }
.desk-badge { display: inline-block; border: 1px solid #35516b; color: #7ec6ff; background: #12253b;
 padding: .4rem .65rem; font: 650 .68rem Consolas, monospace; letter-spacing: .11em; border-radius: 5px; }
.desk-badge.live { color: #49dcb1; background: #102d2a; border-color: #245b4f; }
.desk-badge.stale { color: #ffbc7a; background: #35291e; border-color: #70502d; }
.desk-asof { color: #9bacc3; font: .72rem Consolas, monospace; margin-top: .5rem; text-align: right; }
.desk-brand { font-size: 1.6rem; letter-spacing: .17em; font-weight: 750; margin-bottom: .1rem; }
.desk-brand span { color: #53b4ff; }
.desk-rule { border-top: 1px solid #29374d; margin: 1rem 0; }
@media (max-width: 900px) { .block-container { padding: 1rem; } .desk-head { align-items: flex-start; }
 [data-testid="stMetricValue"] { font-size: 1.3rem; } }
@media (max-width: 650px) { .desk-head { flex-direction: column; } .desk-asof { text-align: left; } }
</style>
"""


def apply_style() -> None:
    """Apply a portable dark desk theme without changing the user's local launch environment."""
    st.html(STYLE)


@st.cache_data(ttl=20, show_spinner=False)
def _snapshot(
    _spark: SparkSession,
    root: str,
    hours: int,
    efficiency: float,
    stream_name: str | None = None,
    historical_day: date | None = None,
) -> desk.DeskSnapshot:
    from pathlib import Path

    return desk.snapshot(_spark, DataPaths(Path(root), stream_name), hours, efficiency, historical_day)


def _number(value, digits: int = 1, suffix: str = "", scale: float = 1) -> str:
    return "—" if value is None or pd.isna(value) else f"{value / scale:,.{digits}f}{suffix}"


def _card(column, label: str, value: str, detail: str, delta: str | None = None) -> None:
    with column.container(border=True, height=180):
        st.metric(label, value, delta=delta, delta_color="off")
        st.caption(detail)


def _delta(value, suffix: str, scale: float = 1, digits: int = 1) -> str | None:
    if value is None or pd.isna(value):
        return None
    return f"{value / scale:+,.{digits}f}{suffix} · 1h"


def render_live(
    spark: SparkSession, paths: DataPaths, hours: int, efficiency: float, historical_day: date | None = None
) -> None:
    """Render the market dashboard, with prominent event time, mode and source coverage."""
    data = _snapshot(spark, str(paths.root), hours, efficiency, paths.stream_name, historical_day)
    now = datetime.now(UTC)
    age = views.minutes_since(data.asof, now)
    label = {"historical": "HISTORICAL VIEW", "replay": "HISTORICAL REPLAY"}.get(data.mode, "LIVE CAPTURE")
    badge = "" if data.mode in ("replay", "historical") else "live"
    if data.asof is None:
        label, badge = "AWAITING STREAM", "stale"
    elif data.mode == "live" and age is not None and age > views.STALE_MINUTES:
        label, badge = "LIVE · STALE DATA", "stale"
    elif data.mode == "none":
        label, badge = "SAVED OUTPUT · CAPTURE UNKNOWN", "stale"
    stamp = "No processed readings" if data.asof is None else f"{data.asof:%d %b %Y · %H:%M} UTC"
    st.html(
        f'<div class="desk-head"><div><div class="desk-eyebrow">Iberian power / system intelligence</div>'
        "<h1>Spain Energy Desk</h1>"
        '<div class="desk-sub">Market context, physical supply and model risk.</div>'
        f'</div><div><span class="desk-badge {badge}">{escape(label)}</span>'
        f'<div class="desk-asof">{escape(stamp)}</div></div></div>'
    )
    if data.asof is None:
        st.info(
            "Waiting for the first live readings. You can switch to Historical while the connection starts."
            if paths.stream_name == "live"
            else "No observations available for the selected period."
        )
        return
    if data.mode in ("replay", "historical"):
        st.caption(
            f"HISTORICAL · Observations from {data.asof:%d %B %Y}. "
            "Prices, gas and weather are matched to those dates. This is not today's market."
        )
    else:
        warning = views.stale_warning(paths, data.timeline, now)
        if warning:
            if paths.stream_name == "live":
                st.warning(
                    f"The newest reading is {age:.0f} min old. "
                    "The source may be delayed; check the live connection status above."
                )
            else:
                st.warning(warning)
        st.caption(
            f"Observation age: {age:.0f} min · capture every 5 min · desk refresh every 10 s"
            if age is not None
            else "Capture status unavailable"
        )
    s = data.summary
    columns = st.columns(6)
    _card(
        columns[0],
        "DEMAND",
        _number(s.get("demand_mw"), 2, " GW", 1000),
        "REE · 5-minute observation",
        _delta(s.get("demand_mw_change_1h"), " GW", 1000, 2),
    )
    _card(
        columns[1],
        "DAY-AHEAD SPOT",
        _number(s.get("spot_price_eur_mwh"), 2, " €"),
        "€/MWh · delivery-hour mean",
        _delta(s.get("spot_price_eur_mwh_change_1h"), " €"),
    )
    _card(
        columns[2],
        "RENEWABLES / LOAD",
        _number(s.get("renewable_share"), 1, "%", 0.01),
        "Wind + PV + solar thermal + hydro",
        _delta(s.get("renewable_share_change_1h"), " pp", 0.01),
    )
    _card(
        columns[3],
        "COMBINED CYCLE / LOAD",
        _number(s.get("combined_cycle_share"), 1, "%", 0.01),
        "Gas-fired CCGT · not all gas generation",
        _delta(s.get("combined_cycle_share_change_1h"), " pp", 0.01),
    )
    _card(
        columns[4],
        "MIBGAS D+1",
        _number(s.get("gas_price_eur_mwh"), 2, " €"),
        "€/MWh gas · matching delivery day",
    )
    _card(
        columns[5],
        "FUEL-ONLY SPREAD",
        _number(s.get("fuel_spread_eur_mwh"), 2, " €"),
        f"Spot − gas / {efficiency:.0%} · €/MWh",
    )
    main, side = st.columns([2.2, 1])
    with main.container(border=True):
        st.altair_chart(charts.desk_demand_chart(data.timeline), width="stretch")
    with side.container(border=True):
        if data.mix.empty:
            st.info("Generation technology detail has not been streamed yet.")
        else:
            st.altair_chart(charts.desk_mix_ring(data.mix), width="stretch")
    main, side = st.columns([2.2, 1])
    with main.container(border=True):
        st.altair_chart(charts.desk_mix_area(data.timeline), width="stretch")
        st.caption(
            "Stack = tracked positive generation. Renewable share above uses demand as denominator, "
            "so it can exceed 100% during exports. The stack is not a system balance."
        )
    with side.container(border=True):
        st.markdown("**System pulse**")
        first, second = st.columns(2)
        first.metric("Residual demand", _number(s.get("residual_demand_mw"), 2, " GW", 1000))
        second.metric("Window peak", _number(s.get("peak_demand_mw"), 2, " GW", 1000))
        first.metric("Wind output", _number(s.get("wind_mw"), 2, " GW", 1000))
        second.metric("Nuclear output", _number(s.get("nuclear_mw"), 2, " GW", 1000))
        st.metric("Cross-border interchange", _number(s.get("net_interchange_mw"), 2, " GW", 1000))
        st.caption(
            f"Peak over the selected {hours} h · interchange retains REE's sign; "
            "import/export direction has not been verified."
        )
    main, side = st.columns([2.2, 1])
    with main.container(border=True):
        if data.prices.empty or data.prices.spot_price_eur_mwh.notna().sum() == 0:
            st.info(
                "No day-ahead price curve for the displayed delivery day. Missing prices remain unavailable."
            )
        else:
            cost = s.get("fuel_cost_eur_mwh")
            st.altair_chart(
                charts.desk_price_profile(data.prices, None if pd.isna(cost) else float(cost)),
                width="stretch",
            )
    with side.container(border=True):
        st.markdown("**Gas-to-power scenario**")
        st.metric("Fuel cost equivalent", _number(s.get("fuel_cost_eur_mwh"), 2, " €/MWh"))
        st.metric("PVPC", _number(s.get("pvpc_eur_mwh"), 2, " €/MWh"))
        st.caption(
            f"Assumed conversion efficiency: {efficiency:.0%}, on the gas quote's energy basis. "
            "The spread subtracts fuel only; CO₂, operating, network and fixed costs are excluded. "
            "PVPC is a regulated consumer price, not a wholesale trading quote."
        )
        st.markdown("[Spread methodology · EIA](https://www.eia.gov/todayinenergy/detail.php?id=9911)")
    left, right = st.columns(2)
    with left.container(border=True):
        usable = data.hourly.dropna(subset=["residual_demand_mw", "spot_price_eur_mwh", "renewable_share"])
        if len(usable) >= 3:
            st.altair_chart(charts.desk_price_scatter(data.hourly), width="stretch")
        else:
            st.info("Price-driver comparison needs at least three matching hourly observations.")
    with right.container(border=True):
        st.markdown("**Weather across the system**")
        st.metric("Population-weighted temperature", _number(s.get("temperature_c"), 1, " °C"))
        if not data.cities.empty:
            st.dataframe(
                data.cities.rename(
                    columns={
                        "city": "City",
                        "temperature_c": "°C",
                        "wind_speed_100m_kmh": "Wind 100 m · km/h",
                        "shortwave_radiation_wm2": "Solar · W/m²",
                    }
                ).round(1),
                hide_index=True,
                width="stretch",
            )
        else:
            st.info("No weather observations match the displayed hour.")
        st.caption(
            "Five-city weather is aligned to the observation hour. Wind and radiation describe "
            "conditions at the cities, not at every generation asset."
        )
    with st.container(border=True):
        st.markdown("**Model risk & operational alerts**")
        predictions = data.monitoring if data.mode == "historical" else views.live_predictions(spark, paths)
        if predictions.empty:
            st.info("Hourly estimates will appear when the first matching observations have been processed.")
        else:
            latest = predictions.iloc[0]
            alerts = predictions[predictions.demand_alert.fillna(False)]
            if latest.prediction_status == "unavailable":
                st.info(
                    "No saved model evaluation estimate for this date. Evaluation predictions cover sampled "
                    "2025/2026 dates; missing estimates remain unavailable."
                )
            elif latest.prediction_status == "missing_weather":
                st.warning("Newest hour: weather unavailable. Estimate and alert are withheld.")
            elif latest.prediction_status == "provisional":
                st.info(
                    f"Newest hour: {int(latest.n_readings)}/12 readings. "
                    "Provisional estimate; alerts withheld."
                )
            elif not alerts.empty:
                st.warning(
                    f"{len(alerts)} of the last {len(predictions)} monitored hours exceed the "
                    "validation-calibrated error threshold. Inspect the residuals before interpretation."
                )
            else:
                st.success("No threshold breaches in the displayed monitored hours.")
            st.dataframe(
                predictions[
                    [
                        "hour_utc",
                        "avg_demand_mw",
                        "expected_demand_mw",
                        "demand_error_mw",
                        "alert_threshold_mw",
                        "n_readings",
                        "prediction_status",
                        "demand_alert",
                    ]
                ].head(12),
                hide_index=True,
                width="stretch",
            )
            st.caption(
                "Alerts require 12 unique readings per hour. Threshold = 99th percentile of 2025 "
                "validation errors. A residual alert is neither an incident diagnosis nor a trading signal."
            )
    with st.expander("Source health, timing & definitions", expanded=False):
        st.dataframe(data.sources, hide_index=True, width="stretch")
        st.caption(
            "Matched means a value exists for the displayed observation/delivery period. "
            "It does not establish publication-time availability of historical price revisions. "
            "Gas is a daily reference and is never forward-filled from another delivery day."
        )
        st.markdown(
            catalog.markdown(
                [
                    catalog.entry(key)
                    for key in [
                        "demand_mw",
                        "renewable_share",
                        "spot_price_eur_mwh",
                        "gas_price_eur_mwh",
                        "expected_demand_mw",
                    ]
                ]
            )
        )
        st.dataframe(
            data.hourly if data.mode == "historical" else views.live_windows(spark, paths).head(12),
            hide_index=True,
            width="stretch",
        )
