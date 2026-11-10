"""Resumable, atomic HTTP downloads into ``data/raw``.

Only fetching happens here: files are stored exactly as received.
"""

import logging
import os
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

USER_AGENT = "spain-electricity-student-project/0.1"


class DownloadError(Exception):
    """Raised when a URL could not be fetched after every retry."""


@dataclass(frozen=True)
class DownloadJob:
    """One file to fetch and the last date its content covers."""

    url: str
    destination: Path
    covers_until: date
    validator: Callable[[bytes], bool] | None = None


@dataclass
class DownloadReport:
    """Outcome of a batch of downloads."""

    fetched: int = 0
    skipped: int = 0
    failed: list[str] = field(default_factory=list)


def should_fetch(destination: Path, covers_until: date, today: date) -> bool:
    """Return True when the file is missing or may still be incomplete (period ends yesterday or later)."""
    return not destination.exists() or covers_until >= today - timedelta(days=1)


def fetch(
    url: str,
    destination: Path,
    *,
    session: requests.Session,
    validator: Callable[[bytes], bool] | None = None,
    retries: int = 3,
    backoff_seconds: float = 2.0,
    timeout: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Download ``url`` to ``destination`` through a ``.part`` file, retrying on any failure."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    last_problem = ""
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code >= 400:
                last_problem = f"HTTP {response.status_code}"
            elif validator is not None and not validator(response.content):
                last_problem = "invalid payload"
            else:
                partial.write_bytes(response.content)
                os.replace(partial, destination)
                return
        except requests.RequestException as error:
            last_problem = f"{type(error).__name__}: {error}"
        logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, last_problem)
        if attempt < retries:
            sleep(backoff_seconds * attempt)
    partial.unlink(missing_ok=True)
    raise DownloadError(f"{url}: {last_problem}")


def download_all(
    jobs: Iterable[DownloadJob],
    *,
    session: requests.Session | None = None,
    pause_seconds: float = 1.0,
    today: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> DownloadReport:
    """Run every job, skipping complete files and carrying on after failures."""
    session = session or _default_session()
    today = today or date.today()
    report = DownloadReport()
    for job in jobs:
        if not should_fetch(job.destination, job.covers_until, today):
            report.skipped += 1
            continue
        try:
            fetch(job.url, job.destination, session=session, validator=job.validator, sleep=sleep)
        except DownloadError as error:
            logger.error("Giving up on %s", error)
            report.failed.append(job.url)
        else:
            report.fetched += 1
            logger.info("Fetched %s", job.destination.name)
        if pause_seconds:
            sleep(pause_seconds)
    return report


def _default_session() -> requests.Session:
    """Return a session with a descriptive, generic User-Agent."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session
