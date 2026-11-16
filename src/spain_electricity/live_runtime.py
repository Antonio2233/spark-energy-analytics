"""Application-owned live capture and Structured Streaming, with expiring viewer leases."""

import atexit
import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from spain_electricity import capture, streaming
from spain_electricity.config import LOCAL_TIMEZONE, DataPaths
from spain_electricity.download import USER_AGENT, download_all
from spain_electricity.sources import mibgas
from spain_electricity.storage import has_parquet

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveStatus:
    """A nonblocking UI status; ready means the readings sink has committed actual data."""

    state: str
    ready: bool
    last_capture: datetime | None
    error: str | None
    reference_warning: str | None


class LiveService:
    """Own one live stream per data root, shared safely by the app's browser sessions.

    A viewer sends a heartbeat while Real time is selected. Capture and this service's queries
    pause when the last viewer switches away or its lease expires. No shell or OS-specific
    process launch is used, and the app's SparkSession is never stopped by this service.
    """

    def __init__(
        self,
        spark: SparkSession,
        paths: DataPaths,
        *,
        capture_seconds: float = 300,
        lease_seconds: float = 90,
        poll_seconds: float = 1,
    ) -> None:
        """Prepare an idle controller; construction performs no network or Spark actions."""
        self.spark = spark
        self.paths = paths.with_stream("live")
        self.capture_seconds = capture_seconds
        self.lease_seconds = lease_seconds
        self.poll_seconds = poll_seconds
        suffix = hashlib.sha256(str(paths.root.resolve()).encode()).hexdigest()[:10]
        self.prefix = f"ui_live_{suffix}_"
        self._condition = threading.Condition(threading.RLock())
        self._clients: dict[str, float] = {}
        self._thread: threading.Thread | None = None
        self._queries: list[StreamingQuery] = []
        self._closed = False
        self._state = "paused"
        self._ready = False
        self._error: str | None = None
        self._reference_warning: str | None = None
        self._last_capture: datetime | None = None
        self._gas_day = None
        self._gas_retry = 0.0
        atexit.register(self.close)

    def _wanted(self) -> bool:
        with self._condition:
            cutoff = time.monotonic() - self.lease_seconds
            self._clients = {key: stamp for key, stamp in self._clients.items() if stamp > cutoff}
            return bool(self._clients) and not self._closed

    def request(self, client_id: str, enabled: bool, *, retry: bool = False) -> LiveStatus:
        """Apply one viewer's selection/heartbeat without waiting for HTTP or Spark startup."""
        with self._condition:
            new_viewer = client_id not in self._clients
            if enabled and not self._closed:
                self._clients[client_id] = time.monotonic()
                if retry or new_viewer:
                    self._error = None
                if self._thread is None:
                    self._thread = threading.Thread(target=self._run, name=self.prefix, daemon=True)
                    self._thread.start()
            else:
                self._clients.pop(client_id, None)
            self._condition.notify_all()
            return self.status()

    def status(self) -> LiveStatus:
        """Return a thread-safe snapshot without touching the JVM or the network."""
        with self._condition:
            return LiveStatus(
                self._state, self._ready, self._last_capture, self._error, self._reference_warning
            )

    def _set_state(self, state: str) -> None:
        with self._condition:
            self._state = state

    def _capture(self, session: requests.Session) -> bool:
        now = datetime.now(UTC)
        files = capture.capture_once(self.paths, now, session=session)
        received = any(path.parent.name == capture.REE_LANDING for path in files)
        if received:
            with self._condition:
                self._last_capture = now
        if self._wanted():
            capture.capture_market_once(self.paths, now, session=session)
        return received

    def _refresh_gas(self) -> None:
        """Refresh a missing daily reference into a committed, isolated snapshot at most daily."""
        from zoneinfo import ZoneInfo

        day = datetime.now(ZoneInfo(LOCAL_TIMEZONE)).date()
        if day == self._gas_day or time.monotonic() < self._gas_retry:
            return
        self._gas_retry = time.monotonic() + 1800
        shared = self.paths.processed / "gas_price"
        if (
            has_parquet(shared)
            and self.spark.read.parquet(str(shared))
            .where(F.col("delivery_date") == F.lit(day))
            .limit(1)
            .count()
        ):
            self._gas_day = day
            self._gas_retry = 0.0
            return
        reference = DataPaths(self.paths.stream_root / "reference")
        target = reference.root / "gas" / f"asof={day}"
        if not (target / "_SUCCESS").exists():
            report = download_all(mibgas.jobs([day.year], reference), pause_seconds=0)
            if report.failed:
                raise RuntimeError("The daily gas reference could not be refreshed")
            raw = reference.raw / "mibgas" / f"MIBGAS_Data_{day.year}.xlsx"
            csv = reference.raw / "mibgas" / "csv" / f"MIBGAS_Data_{day.year}.csv"
            mibgas.xlsx_to_csv(raw, csv)
            mibgas.read_gas_price(self.spark, csv.parent).write.mode("overwrite").parquet(str(target))
        self._gas_day = day
        self._gas_retry = 0.0

    def _observe(self) -> None:
        for query in self._queries:
            if not query.isActive:
                problem = query.exception()
                raise RuntimeError(f"The live stream stopped: {problem or query.name}")
        folder = self.paths.stream_output / "readings"
        # Only expose files after the Parquet streaming sink's metadata commit exists.
        metadata = folder / "_spark_metadata"
        ready = (
            has_parquet(folder)
            and metadata.exists()
            and any(path.is_file() and not path.name.startswith(".") for path in metadata.iterdir())
        )
        with self._condition:
            self._ready = ready
            self._state = "running" if ready else "waiting"

    def _stop_queries(self) -> None:
        owned = list(self._queries)
        try:
            owned.extend(
                query
                for query in self.spark.streams.active
                if query.name and query.name.startswith(self.prefix)
            )
        except Exception:
            logger.debug("Spark is already unavailable during live-service shutdown", exc_info=True)
        for query in owned:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                logger.warning("A live query could not be stopped cleanly", exc_info=True)
        self._queries = []

    def _run(self) -> None:
        session = requests.Session()
        session.headers["User-Agent"] = USER_AGENT
        try:
            while True:
                with self._condition:
                    while not self._closed and (not self._wanted() or self._error is not None):
                        self._condition.wait(timeout=self.poll_seconds)
                    if self._closed:
                        return
                    self._state = "starting"
                try:
                    received = self._capture(session)
                    if not self._wanted():
                        continue
                    self._queries = streaming.start_stream(
                        self.spark, self.paths, trigger_seconds=10, query_prefix=self.prefix
                    )
                    next_capture = time.monotonic() + (self.capture_seconds if received else 30)
                    while self._wanted():
                        self._observe()
                        if time.monotonic() >= next_capture:
                            received = self._capture(session)
                            next_capture = time.monotonic() + (self.capture_seconds if received else 30)
                        try:
                            self._refresh_gas()
                            with self._condition:
                                self._reference_warning = None
                        except Exception as error:
                            logger.warning("Optional gas reference update failed: %s", error)
                            with self._condition:
                                self._reference_warning = str(error)
                        with self._condition:
                            self._condition.wait(timeout=self.poll_seconds)
                except Exception as error:
                    logger.exception("Live data service failed")
                    with self._condition:
                        self._error = str(error)
                        self._state = "error"
                finally:
                    self._stop_queries()
                    with self._condition:
                        if self._error is None:
                            self._state = "paused"
        finally:
            session.close()

    def close(self) -> None:
        """Request shutdown of owned work only; leave Spark and historical state intact."""
        with self._condition:
            self._closed = True
            self._clients.clear()
            self._condition.notify_all()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
