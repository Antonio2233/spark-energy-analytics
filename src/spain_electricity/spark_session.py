"""The only way to create (and stop) the project's SparkSession."""

from pyspark.sql import SparkSession

_session: SparkSession | None = None


def get_spark(
    app_name: str = "spain-electricity", master: str = "local[*]", shuffle_partitions: int = 8
) -> SparkSession:
    """Return the process's local SparkSession, creating it in UTC with a small shuffle.

    The session is created once per process and then returned unchanged, from any thread: a second
    caller (the Streamlit app inside a test run, for example) must not rewrite the first caller's
    settings. Stop it with ``stop_spark``.
    """
    global _session
    if _session is not None:
        return _session
    spark = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    _session = spark
    return spark


def stop_spark() -> None:
    """Stop the process's SparkSession, if any, so that the next ``get_spark`` creates a new one."""
    global _session
    if _session is not None:
        _session.stop()
        _session = None
