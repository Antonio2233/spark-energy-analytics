"""Transformations shared by batch and streaming, and the hourly feature table.

Every function except ``build_hourly_features`` and ``write_hourly_features`` is a pure
DataFrame transformation with no actions, so it works the same on streaming DataFrames.
"""

from collections.abc import Mapping, Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spain_electricity.config import CITIES, LOCAL_TIMEZONE, City, DataPaths
from spain_electricity.national_holidays import holidays_frame
from spain_electricity.sources.ree_generation import RAW_TO_COLUMN

GENERATION_COLUMNS = tuple(RAW_TO_COLUMN.values())
RENEWABLE_COLUMNS = ("wind_mw", "solar_mw", "hydro_mw")
WEATHER_COLUMNS = ("temperature_c", "shortwave_radiation_wm2", "wind_speed_100m_kmh")
PRICE_COLUMNS = {"spot": "spot_price_eur_mwh", "pvpc": "pvpc_eur_mwh"}
HOLIDAY_YEARS = range(2012, 2036)


def hourly_mean(df: DataFrame, ts_col: str, value_cols: Sequence[str]) -> DataFrame:
    """Average values per UTC hour (power and prices are averaged, never summed)."""
    return df.groupBy(F.date_trunc("hour", F.col(ts_col)).alias("hour_utc")).agg(
        *[F.avg(column).alias(column) for column in value_cols]
    )


def add_renewable_share(df: DataFrame) -> DataFrame:
    """Add ``renewable_share`` = (wind + solar + hydro) / demand."""
    renewables = sum((F.col(column) for column in RENEWABLE_COLUMNS[1:]), F.col(RENEWABLE_COLUMNS[0]))
    return df.withColumn("renewable_share", renewables / F.col("demand_mw"))


def national_weather(weather: DataFrame, cities: Mapping[str, City] = CITIES) -> DataFrame:
    """Return the weighted mean of every weather column per timestamp across cities."""
    weight = F.create_map(*[F.lit(item) for city in cities.values() for item in (city.name, city.weight)])[
        F.col("city")
    ]
    averages = []
    for column in WEATHER_COLUMNS:
        present = F.col(column).isNotNull() & weight.isNotNull()
        averages.append(
            (F.sum(F.when(present, F.col(column) * weight)) / F.sum(F.when(present, weight))).alias(column)
        )
    return weather.groupBy("ts_utc").agg(*averages)


def add_calendar(df: DataFrame, holidays: DataFrame, ts_col: str = "hour_utc") -> DataFrame:
    """Add Madrid wall-time calendar columns and the national holiday flag."""
    local = F.from_utc_timestamp(F.col(ts_col), LOCAL_TIMEZONE)
    day_of_week = (F.dayofweek(local) + 5) % 7 + 1  # 1 = Monday ... 7 = Sunday
    with_calendar = (
        df.withColumn("local_date", F.to_date(local))
        .withColumn("local_hour", F.hour(local))
        .withColumn("day_of_week", day_of_week)
        .withColumn("month", F.month(local))
        .withColumn("is_weekend", day_of_week >= 6)
    )
    return with_calendar.join(F.broadcast(holidays), "local_date", "left").withColumn(
        "is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False))
    )


def _read_optional(spark: SparkSession, path: Path) -> DataFrame | None:
    return spark.read.parquet(str(path)) if path.exists() else None


def _with_nulls(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    for column in columns:
        df = df.withColumn(column, F.lit(None).cast("double"))
    return df


def build_hourly_features(spark: SparkSession, paths: DataPaths) -> DataFrame:
    """Return one row per UTC hour joining generation, forecast, prices, weather, gas and calendar."""
    generation_path = paths.processed / "generation"
    if not generation_path.exists():
        raise FileNotFoundError(f"{generation_path} is missing: run the ingestion first")
    table = add_renewable_share(
        hourly_mean(spark.read.parquet(str(generation_path)), "ts_utc", GENERATION_COLUMNS)
    )

    forecast = _read_optional(spark, paths.processed / "demand_forecast")
    if forecast is None:
        table = _with_nulls(table, ["forecast_demand_mw"])
    else:
        table = table.join(hourly_mean(forecast, "ts_utc", ["forecast_demand_mw"]), "hour_utc", "left")

    prices = _read_optional(spark, paths.processed / "market_price")
    if prices is None:
        table = _with_nulls(table, list(PRICE_COLUMNS.values()))
    else:
        hourly_prices = (
            prices.groupBy(F.date_trunc("hour", "ts_utc").alias("hour_utc"))
            .pivot("series", list(PRICE_COLUMNS))
            .agg(F.avg("price_eur_mwh"))
        )
        for series, column in PRICE_COLUMNS.items():
            hourly_prices = hourly_prices.withColumnRenamed(series, column)
        table = table.join(hourly_prices, "hour_utc", "left")

    weather = _read_optional(spark, paths.processed / "weather")
    if weather is None:
        table = _with_nulls(table, WEATHER_COLUMNS)
    else:
        table = table.join(
            hourly_mean(national_weather(weather), "ts_utc", WEATHER_COLUMNS), "hour_utc", "left"
        )

    table = add_calendar(table, holidays_frame(spark, HOLIDAY_YEARS))
    gas = _read_optional(spark, paths.processed / "gas_price")
    if gas is None:
        table = _with_nulls(table, ["gas_price_eur_mwh"])
    else:
        table = table.join(gas.withColumnRenamed("delivery_date", "local_date"), "local_date", "left")
    return table


def write_hourly_features(spark: SparkSession, paths: DataPaths) -> int:
    """Write ``processed/hourly_features`` partitioned by local year and month; return its row count."""
    target = paths.processed / "hourly_features"
    (
        build_hourly_features(spark, paths)
        .withColumn("year", F.year("local_date"))
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(target))
    )
    return spark.read.parquet(str(target)).count()
