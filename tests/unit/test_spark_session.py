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


def test_local_session_talks_to_itself_over_loopback(spark):
    # Results above 1 MiB, broadcasts and shuffles travel through the block manager's address; on a
    # laptop with a firewall in stealth mode or a VPN, an interface address can silently hang.
    assert spark.conf.get("spark.driver.host") == "127.0.0.1"
    assert spark.conf.get("spark.driver.bindAddress") == "127.0.0.1"


def test_task_result_above_one_mib_reaches_the_driver(spark):
    blobs = spark.sparkContext.parallelize([0], 1).map(lambda _: b"x" * 2_000_000).collect()
    assert len(blobs[0]) == 2_000_000
