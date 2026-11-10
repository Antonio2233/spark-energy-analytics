def test_session_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_session_shuffle_partitions(spark):
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"


def test_get_spark_keeps_existing_session_settings(spark):
    from spain_electricity.spark_session import get_spark

    again = get_spark("another-caller", "local[2]", 9)
    assert again is spark
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"


def test_session_uses_arrow_for_pandas(spark):
    assert spark.conf.get("spark.sql.execution.arrow.pyspark.enabled") == "true"


def test_to_pandas_keeps_utc_wall_time(spark):
    frame = spark.sql("SELECT timestamp'2025-07-01 22:00:00' AS t").toPandas()
    assert str(frame.t[0]) == "2025-07-01 22:00:00"
