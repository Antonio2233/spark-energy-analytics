def test_session_is_utc(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_session_shuffle_partitions(spark):
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
