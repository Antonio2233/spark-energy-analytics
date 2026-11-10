"""The only way to create the project's SparkSession."""

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "spain-electricity", master: str = "local[*]", shuffle_partitions: int = 8
) -> SparkSession:
    """Return a local SparkSession working in UTC with a small shuffle."""
    spark = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
