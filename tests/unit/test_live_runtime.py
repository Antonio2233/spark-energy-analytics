import threading
import time
from types import SimpleNamespace

from spain_electricity import live_runtime


class Query:
    def __init__(self, name):
        self.name = name
        self.isActive = True
        self.stopped = threading.Event()

    def stop(self):
        self.isActive = False
        self.stopped.set()

    def exception(self):
        return None


def setup_service(monkeypatch, paths, *, fail_first=False, lease_seconds=10):
    spark = SimpleNamespace(streams=SimpleNamespace(active=[]))
    started = threading.Event()
    attempts = []
    captured = []

    def capture(paths, now, *, session):
        captured.append(paths)
        return [paths.landing / "ree_generation" / "capture.jsonp"]

    def start(spark, paths, **kwargs):
        query = Query(kwargs["query_prefix"] + "readings")
        spark.streams.active.append(query)
        attempts.append(query)
        if fail_first and len(attempts) == 1:
            raise RuntimeError("simulated startup failure")
        output = paths.stream_output / "readings"
        (output / "_spark_metadata").mkdir(parents=True, exist_ok=True)
        (output / "_spark_metadata" / "0").write_text("committed")
        (output / "part-0.parquet").write_text("present")
        started.set()
        return [query]

    monkeypatch.setattr(live_runtime.capture, "capture_once", capture)
    monkeypatch.setattr(live_runtime.capture, "capture_market_once", lambda *args, **kwargs: [])
    monkeypatch.setattr(live_runtime.streaming, "start_stream", start)
    monkeypatch.setattr(live_runtime.LiveService, "_refresh_gas", lambda self: None)
    service = live_runtime.LiveService(spark, paths, lease_seconds=lease_seconds, poll_seconds=0.01)
    return service, spark, started, attempts, captured


def wait_for(predicate):
    end = time.monotonic() + 3
    while not predicate() and time.monotonic() < end:
        threading.Event().wait(0.01)
    assert predicate()


def test_selection_is_nonblocking_shared_and_preserves_other_queries(monkeypatch, paths):
    service, spark, started, attempts, captured = setup_service(monkeypatch, paths)
    unrelated = Query("unrelated_job")
    spark.streams.active.append(unrelated)
    legacy = paths.checkpoints / "marker"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("preserve replay")
    try:
        assert service.request("a", False).state == "paused"
        assert service._thread is None and not captured
        service.request("a", True)
        assert started.wait(3)
        wait_for(lambda: service.status().ready)
        service.request("a", True)
        service.request("b", True)
        assert len(attempts) == 1
        assert captured[0].stream_name == "live" and captured[0].models == paths.models
        service.request("a", False)
        assert not attempts[0].stopped.wait(0.05)  # Another viewer still needs the stream.
        service.request("b", False)
        assert attempts[0].stopped.wait(3)
        assert unrelated.isActive and legacy.read_text() == "preserve replay"
    finally:
        service.close()


def test_closed_browser_lease_expires_without_a_terminal_action(monkeypatch, paths):
    service, _, started, attempts, _ = setup_service(monkeypatch, paths, lease_seconds=0.2)
    try:
        service.request("browser", True)
        assert started.wait(3)
        assert attempts[0].stopped.wait(3)
        wait_for(lambda: service.status().state == "paused")
    finally:
        service.close()


def test_partial_startup_is_cleaned_up_and_retry_is_explicit(monkeypatch, paths):
    service, _, started, attempts, _ = setup_service(monkeypatch, paths, fail_first=True)
    try:
        service.request("browser", True)
        wait_for(lambda: service.status().error is not None)
        assert attempts[0].stopped.wait(3)
        service.request("browser", True)
        assert len(attempts) == 1
        service.request("browser", True, retry=True)
        assert started.wait(3)
        wait_for(lambda: service.status().ready)
        assert service.status().error is None and len(attempts) == 2
    finally:
        service.close()
