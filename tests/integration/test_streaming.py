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


@pytest.mark.integration
@pytest.mark.parametrize(("max_files", "files_per_batch"), [(1, [1, 1]), (10, [2])])
def test_stream_takes_several_files_per_micro_batch(spark, paths, fixtures_dir, max_files, files_per_batch):
    # After a restart the backlog of captures must drain in a few batches, not one file per trigger.
    # Each landed file is one input row of the text source.
    landing = paths.landing / "ree_generation"
    landing.mkdir(parents=True)
    for offset, day in enumerate(("2025-01-15", "2025-01-16")):
        target = landing / f"2025011{5 + offset}T000000Z.jsonp"
        shutil.copy(fixtures_dir / "ree_generation" / "2025" / "01" / f"ree_generation_{day}.jsonp", target)

    queries = streaming.start_stream(spark, paths, available_now=True, max_files_per_trigger=max_files)
    for query in queries:
        query.awaitTermination()

    progress = queries[0].recentProgress
    rows = [entry["numInputRows"] if isinstance(entry, dict) else entry.numInputRows for entry in progress]
    assert [count for count in rows if count] == files_per_batch
