"""Real-time capture, catch-up after an outage, and replay of history into the landing folder.

Every file lands atomically: it is written as a hidden ``.<name>.part`` file (Spark's file source
ignores names starting with a dot) and renamed when complete. File names carry the UTC capture
time, so ``last_capture_time`` can tell how long the capture was down.
"""

import json
import logging
import os
import re
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from datetime import time as clock_time
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from spain_electricity.config import CITIES, LOCAL_TIMEZONE, DataPaths
from spain_electricity.download import USER_AGENT
from spain_electricity.sources import open_meteo, ree_generation

logger = logging.getLogger(__name__)

REE_LANDING = "ree_generation"
WEATHER_LANDING = "open_meteo_current"
CAPTURE_NAME = re.compile(r"^(\d{8}T\d{6}Z)\.jsonp$")
STAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def _stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime(STAMP_FORMAT)


def _write_atomically(directory: Path, name: str, content: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    partial, final = directory / f".{name}.part", directory / name
    partial.write_bytes(content)
    os.replace(partial, final)
    return final


def _get(session: requests.Session, url: str) -> bytes | None:
    try:
        response = session.get(url, timeout=60)
    except requests.RequestException as error:
        logger.warning("Capture request failed for %s: %s; will retry on the next cycle", url, error)
        return None
    if response.status_code >= 400:
        logger.warning("Capture got HTTP %s for %s; will retry on the next cycle", response.status_code, url)
        return None
    return response.content


def _has_readings(content: bytes) -> bool:
    """Return True when a REE JSONP payload holds at least one reading."""
    match = re.match(rb"(?s)^\s*\w+\((.*)\)\s*;?\s*$", content)
    if not match:
        return False
    try:
        return bool(json.loads(match.group(1)).get("valoresHorariosGeneracion"))
    except (ValueError, AttributeError):
        return False


def _land_ree_day(paths: DataPaths, session: requests.Session, day, name: str) -> Path | None:
    content = _get(session, ree_generation.build_url(day))
    if content is None:
        return None
    if not (ree_generation.is_valid_payload(content) and _has_readings(content)):
        logger.warning("REE returned no readings for %s; the feed may be stalled", day)
        return None
    return _write_atomically(paths.landing / REE_LANDING, name, content)


def capture_once(paths: DataPaths, now: datetime, *, session: requests.Session) -> list[Path]:
    """Land today's REE readings and the current weather; never raises on network problems."""
    stamp = _stamp(now)
    local_today = now.astimezone(ZoneInfo(LOCAL_TIMEZONE)).date()
    written = []
    ree_file = _land_ree_day(paths, session, local_today, f"{stamp}.jsonp")
    if ree_file is not None:
        written.append(ree_file)
    weather = _get(session, open_meteo.build_current_url(CITIES.values()))
    if weather is not None:
        written.append(_write_atomically(paths.landing / WEATHER_LANDING, f"{stamp}.json", weather))
    return written


def last_capture_time(landing_dir: Path) -> datetime | None:
    """Return the UTC time of the newest live capture, ignoring replayed or backfilled files."""
    folder = landing_dir / REE_LANDING
    if not folder.exists():
        return None
    stamps = [match.group(1) for path in folder.iterdir() if (match := CAPTURE_NAME.match(path.name))]
    if not stamps:
        return None
    return datetime.strptime(max(stamps), STAMP_FORMAT).replace(tzinfo=UTC)


def catch_up(paths: DataPaths, now: datetime, *, session: requests.Session, max_days: int = 14) -> list[Path]:
    """Land every local day missed since the last capture, oldest first, up to yesterday."""
    last = last_capture_time(paths.landing)
    if last is None:
        return []
    zone = ZoneInfo(LOCAL_TIMEZONE)
    today = now.astimezone(zone).date()
    yesterday = today - timedelta(days=1)
    # Today's REE file starts at 21:00 local of the previous day, so a later capture leaves no gap.
    if last >= datetime.combine(yesterday, clock_time(21, 0), zone):
        return []
    first_day = max(last.astimezone(zone).date(), today - timedelta(days=max_days))
    logger.info("Catching up REE readings from %s to %s after an outage", first_day, yesterday)
    written = []
    day = first_day
    while day <= yesterday:
        landed = _land_ree_day(paths, session, day, f"{_stamp(now)}_backfill_{day.isoformat()}.jsonp")
        if landed is not None:
            written.append(landed)
        day += timedelta(days=1)
    return written


def run_capture(paths: DataPaths, interval_seconds: int = 300) -> None:
    """Catch up once, then capture every ``interval_seconds`` until interrupted."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    catch_up(paths, datetime.now(UTC), session=session)
    while True:
        written = capture_once(paths, datetime.now(UTC), session=session)
        logger.info("Captured %d file(s) into %s", len(written), paths.landing)
        time.sleep(interval_seconds)


def replay_files(
    files: Sequence[Path],
    landing_dir: Path,
    delay_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Copy history files into ``landing_dir`` in name order, pausing between them; return the count."""
    ordered = sorted(files, key=lambda path: path.name)
    for index, source in enumerate(ordered):
        if index:
            sleep(delay_seconds)
        _write_atomically(landing_dir, source.name, source.read_bytes())
        logger.info("Replayed %s (%d/%d)", source.name, index + 1, len(ordered))
    return len(ordered)
