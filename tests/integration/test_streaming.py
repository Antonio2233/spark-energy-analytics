import os
import shutil

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from spain_electricity import streaming


@pytest.mark.integration
def test_stream_dedupes_and_windows(spark, paths, fixtures_dir):
    landing = paths.landing / "ree_generation"
    landing.mkdir(parents=True)
    for offset, day in enumerate(("2025-01-15", "2025-01-16")):
        target = landing / f"2025011{5 + offset}T000000Z.jsonp"
        shutil.copy(fixtures_dir / "ree_generation" / "2025" / "01" / f"ree_generation_{day}.jsonp", target)
        os.utime(target, (1_700_000_000 + offset * 60, 1_700_000_000 + offset * 60))

    for query in streaming.start_stream(spark, paths, available_now=True):
        query.awaitTermination()

    readings = spark.read.parquet(str(paths.stream_output / "readings"))
    assert readings.count() == 649
    assert readings.select("ts_utc").distinct().count() == 649
    assert "renewable_share" in readings.columns

    windows = spark.read.parquet(str(paths.stream_output / "windows"))
    latest = windows.withColumn(
        "rank", F.row_number().over(Window.partitionBy("window_start").orderBy(F.desc("batch_id")))
    ).where("rank = 1")
    assert latest.count() == 217
    assert {row.n_readings for row in latest.select("n_readings").distinct().collect()} <= {1, 2, 3}
    assert latest.agg(F.sum("n_readings")).first()[0] == 649
