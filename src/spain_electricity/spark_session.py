"""The only way to create (and stop) the project's SparkSession."""

import os

from pyspark.sql import SparkSession

from spain_electricity.config import DEFAULT_DRIVER_MEMORY, DRIVER_MEMORY_ENV

LOOPBACK = "127.0.0.1"

_session: SparkSession | None = None


def get_spark(
    app_name: str = "spain-electricity", master: str = "local[*]", shuffle_partitions: int = 8
) -> SparkSession:
    """Return the process's local SparkSession, creating it in UTC with a small shuffle.

    The session is created once per process and then returned unchanged, from any thread: a second
    caller (the Streamlit app inside a test run, for example) must not rewrite the first caller's
    settings. Stop it with ``stop_spark``.

    A local session binds the driver to the loopback address. Task results above 1 MiB, broadcasts
    and shuffle blocks are fetched from the block manager's address, and the address Spark picks by
    default (a LAN or VPN interface) can hang silently behind a firewall in stealth mode. Its heap is
    ``SPAIN_ELECTRICITY_DRIVER_MEMORY`` (6 GB by default): Spark's own 1 GB is too small for the
    full-history training. It applies only when the JVM starts, that is, to the process's first session.
    """
    global _session
    if _session is not None:
        return _session
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master.startswith("local"):
        builder = (
            builder.config("spark.driver.host", LOOPBACK)
            .config("spark.driver.bindAddress", LOOPBACK)
            .config("spark.driver.memory", os.environ.get(DRIVER_MEMORY_ENV, DEFAULT_DRIVER_MEMORY))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    _session = spark
    return spark


def stop_spark() -> None:
    """Stop the process's SparkSession, if any, so that the next ``get_spark`` creates a new one."""
    global _session
    if _session is not None:
        _session.stop()
        _session = None
