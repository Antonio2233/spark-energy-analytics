"""What every series shown in the UI is, its unit, and the source it comes from.

The charts take their series names from here, so a name in a legend and its explanation never drift.
"""

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    """Where a series comes from: a readable name, a public page and the endpoint the project calls."""

    name: str
    url: str | None
    endpoint: str


@dataclass(frozen=True)
class Entry:
    """One series: its column (or technology) key, display name, unit, meaning and source."""

    key: str
    label: str
    unit: str
    description: str
    source: Source


REE_FEED = Source(
    "REE · real-time demand and generation (Peninsula)",
    "https://demanda.ree.es/visiona/home",
    "demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/demandaGeneracionPeninsula, every 5 minutes",
)
REDATA = Source(
    "REE · REData API",
    "https://www.ree.es/en/datos/apidata",
    "apidatos.ree.es/es/datos: demanda/demanda-tiempo-real and mercados/precios-mercados-tiempo-real",
)
OPEN_METEO_ARCHIVE = Source(
    "Open-Meteo · Historical Weather API",
    "https://open-meteo.com/en/docs/historical-weather-api",
    "archive-api.open-meteo.com/v1/archive, hourly, one request per city and month",
)
OPEN_METEO_CURRENT = Source(
    "Open-Meteo · Forecast API (current conditions)",
    "https://open-meteo.com/en/docs",
    "api.open-meteo.com/v1/forecast?current=…, captured every 5 minutes with the live feed",
)
MIBGAS = Source(
    "MIBGAS · yearly data files",
    "https://www.mibgas.es/en/file-access",
    "www.mibgas.es/es/file-access/MIBGAS_Data_YYYY.xlsx, product GDAES_D+1",
)
PROJECT = Source(
    "Computed in this project",
    None,
    "spain_electricity.features and spain_electricity.national_holidays",
)
SOURCES = (REE_FEED, REDATA, OPEN_METEO_ARCHIVE, OPEN_METEO_CURRENT, MIBGAS, PROJECT)

WEIGHTED_CITIES = (
    "National value: the population-weighted mean of Madrid, Barcelona, Valencia, Seville and Bilbao."
)

VARIABLES = (
    Entry(
        "demand_mw",
        "Demand (5-min feed)",
        "MW",
        "Electricity demand of the peninsular system, published every 5 minutes. It is the demand the "
        "streaming job sees; an hourly value is the mean of its twelve readings.",
        REE_FEED,
    ),
    Entry(
        "actual_demand_mw",
        "Actual demand (REData)",
        "MW",
        "National demand as REData publishes it ('Demanda nacional en tiempo real', series Real). It is the "
        "series REE's forecast predicts. It runs about 5% above the peninsular feed; the gap matches the "
        "demand of the Balearic and Canary Islands, Ceuta and Melilla.",
        REDATA,
    ),
    Entry(
        "forecast_demand_mw",
        "REE forecast",
        "MW",
        "REE's own forecast of national demand (series Prevista), at the same 5-minute grain. It is "
        "compared with the national actual demand; the demand model's baseline is REE's peninsular "
        "forecast instead.",
        REDATA,
    ),
    Entry(
        "renewable_share",
        "Renewable share",
        "%",
        "Share of demand covered by wind, solar and hydro in the same hour: (wind + solar + hydro) / demand. "
        "Computed in this project from the 5-minute feed.",
        REE_FEED,
    ),
    Entry(
        "spot_price_eur_mwh",
        "Day-ahead price",
        "€/MWh",
        "Wholesale price of the day-ahead electricity market in Spain (REData 'Precio mercado spot'). "
        "Hourly until 2025 and quarter-hourly after it; an hourly value is the mean.",
        REDATA,
    ),
    Entry(
        "pvpc_eur_mwh",
        "PVPC",
        "€/MWh",
        "Hourly energy price of the regulated tariff for households and small consumers (Precio Voluntario "
        "para el Pequeño Consumidor).",
        REDATA,
    ),
    Entry(
        "temperature_c",
        "Temperature",
        "°C",
        f"Air temperature at 2 m. {WEIGHTED_CITIES}",
        OPEN_METEO_ARCHIVE,
    ),
    Entry(
        "shortwave_radiation_wm2",
        "Solar radiation",
        "W/m²",
        f"Sunlight reaching the ground (shortwave radiation); it drives solar output. {WEIGHTED_CITIES}",
        OPEN_METEO_ARCHIVE,
    ),
    Entry(
        "wind_speed_100m_kmh",
        "Wind at 100 m",
        "km/h",
        f"Wind speed at 100 m above ground, around the height of a wind turbine's hub. {WEIGHTED_CITIES}",
        OPEN_METEO_ARCHIVE,
    ),
    Entry(
        "gas_price_eur_mwh",
        "Gas price",
        "€/MWh",
        "Spanish natural gas price for delivery the next day (MIBGAS product GDAES_D+1), assigned to its "
        "delivery day and repeated over its 24 hours. Gas sets the cost of combined-cycle plants.",
        MIBGAS,
    ),
    Entry(
        "day_type",
        "Weekend or holiday",
        "",
        "Saturdays, Sundays and the ten national holidays (nine fixed dates plus Good Friday). Demand on "
        "those days looks like a Sunday's.",
        PROJECT,
    ),
)

# Color slot order of the mix chart: adjacent slots are the validated color pairs.
TECHNOLOGIES = (
    Entry(
        "hydro",
        "Hydro",
        "MW",
        "Electricity from water held in reservoirs or flowing in rivers. It is net of pumped storage: when "
        "plants pump water uphill to store energy they consume, so the value can drop below zero.",
        REE_FEED,
    ),
    Entry(
        "solar_pv",
        "Solar PV",
        "MW",
        "Photovoltaic panels, which turn sunlight directly into electricity: zero at night, highest around "
        "midday.",
        REE_FEED,
    ),
    Entry(
        "wind",
        "Wind",
        "MW",
        "Wind turbines. Output follows the weather, by day and by night.",
        REE_FEED,
    ),
    Entry(
        "solar_thermal",
        "Solar thermal",
        "MW",
        "Concentrated solar power: mirrors heat a fluid that drives a steam turbine. Heat storage lets some "
        "plants keep running after sunset.",
        REE_FEED,
    ),
    Entry(
        "nuclear",
        "Nuclear",
        "MW",
        "Nuclear power plants. Output is almost flat and changes only when a reactor stops for refuelling or "
        "maintenance.",
        REE_FEED,
    ),
    Entry(
        "combined_cycle",
        "Combined cycle",
        "MW",
        "Natural gas plants with a gas turbine and a steam turbine. They are flexible and cover peaks and "
        "hours without wind or sun; their cost follows the gas price.",
        REE_FEED,
    ),
    Entry(
        "coal",
        "Coal",
        "MW",
        "Coal-fired power plants, almost phased out in Spain.",
        REE_FEED,
    ),
    Entry(
        "cogeneration_other",
        "Cogeneration and other",
        "MW",
        "Industrial plants that produce heat and electricity together, mostly with gas, plus waste and other "
        "minor sources.",
        REE_FEED,
    ),
)

DATASETS = {
    "generation": ("Demand and generation mix, every 5 minutes", REE_FEED),
    "demand_forecast": ("Actual, forecast and scheduled national demand, every 5 minutes", REDATA),
    "market_price": ("Day-ahead market price and PVPC", REDATA),
    "weather": ("Hourly weather in five cities", OPEN_METEO_ARCHIVE),
    "gas_price": ("Daily natural gas price (day-ahead product)", MIBGAS),
    "hourly_features": ("All of the above joined by hour: the input of the models", PROJECT),
}

# Dataset columns that are not series of their own: keys, calendar fields and stream bookkeeping.
ATTRIBUTES = {
    "ts_utc": ("", "Time of the reading, in UTC."),
    "hour_utc": ("", "Start of the hour, in UTC."),
    "local_date": ("", "Date in Spanish local time (Europe/Madrid)."),
    "local_hour": ("", "Hour of the day in Spanish local time, 0 to 23."),
    "day_of_week": ("", "Day of the week in local time: 1 is Monday, 7 is Sunday."),
    "is_weekend": ("", "True on Saturdays and Sundays, in local time."),
    "is_holiday": ("", "True on the ten national holidays."),
    "year": (
        "",
        "Partition column: year of the timestamp (UTC in the source datasets, local in hourly_features).",
    ),
    "month": (
        "",
        "Partition column: month of the timestamp (UTC in the source datasets, local in hourly_features).",
    ),
    "series": ("", "Which price the row holds: spot (day-ahead market) or pvpc."),
    "price_eur_mwh": ("€/MWh", "The price of that series at that time."),
    "city": ("", "City of the weather point: madrid, barcelona, valencia, seville or bilbao."),
    "delivery_date": ("", "Day the gas is delivered, which is the day its price applies to."),
    "scheduled_demand_mw": ("MW", "Demand scheduled in the market programmes (REData series Programada)."),
    "solar_mw": ("MW", "Total solar generation: solar PV plus solar thermal."),
    "net_interchange_mw": (
        "MW",
        "Net balance of the exchanges with neighbouring systems. Its sign convention has not been checked.",
    ),
    "window_start": ("", "Start of the 15-minute window, in UTC."),
    "window_end": ("", "End of the 15-minute window, in UTC."),
    "avg_demand_mw": ("MW", "Mean demand of the readings in the window."),
    "max_demand_mw": ("MW", "Highest demand reading in the window."),
    "avg_renewable_share": ("%", "Mean renewable share of the readings in the window."),
    "n_readings": ("", "Readings that fell in the window: 3 when it is complete."),
    "batch_id": ("", "Micro-batch that wrote this version of the window; the UI keeps the latest one."),
}

_BY_KEY = {entry.key: entry for entry in VARIABLES + TECHNOLOGIES}
_TECHNOLOGY_COLUMNS = {f"{entry.key}_mw": entry for entry in TECHNOLOGIES}


def entry(key: str) -> Entry:
    """Return the entry of a column or technology key."""
    return _BY_KEY[key]


def label(key: str) -> str:
    """Return the display name of a column or technology key."""
    return _BY_KEY[key].label


def attribute(column: str) -> tuple[str, str]:
    """Return the unit and the meaning of a dataset column; blank strings when it is unknown."""
    item = _BY_KEY.get(column) or _TECHNOLOGY_COLUMNS.get(column)
    if item is not None:
        return item.unit, item.description
    return ATTRIBUTES.get(column, ("", ""))


def markdown(entries: Iterable[Entry]) -> str:
    """Return a Markdown list with each entry's name, unit, meaning and linked source."""
    lines = []
    for item in entries:
        unit = f" ({item.unit})" if item.unit else ""
        source = f"[{item.source.name}]({item.source.url})" if item.source.url else item.source.name
        lines.append(f"- **{item.label}**{unit}: {item.description} Source: {source}.")
    return "\n".join(lines)
