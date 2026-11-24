import os
import subprocess
import sys

import pytest

from spain_electricity.config import DEFAULT_DRIVER_MEMORY, DRIVER_MEMORY_ENV

CHECK = (
    "from spain_electricity.spark_session import get_spark, stop_spark\n"
    "spark = get_spark('memory-check', 'local[1]', 1)\n"
    "print(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory())\n"
    "stop_spark()\n"
)


@pytest.mark.integration
def test_local_session_takes_driver_memory_from_the_environment():
    # Full-history demand training ran out of Spark's default 1 GB heap; 6 GB was enough.
    env = {**os.environ, DRIVER_MEMORY_ENV: "2g"}
    result = subprocess.run(
        [sys.executable, "-c", CHECK], env=env, capture_output=True, text=True, timeout=180
    )
    max_heap = int(result.stdout.strip().splitlines()[-1])
    assert max_heap > 1.5 * 1024**3


def test_default_driver_memory_is_the_measured_one():
    assert DEFAULT_DRIVER_MEMORY == "6g"
