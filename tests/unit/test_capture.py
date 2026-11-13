import logging
import os
from datetime import UTC, datetime, timedelta

from helpers import FakeResponse, FakeSession

from spain_electricity import capture

REE_PAYLOAD = b'cb({"valoresHorariosGeneracion":[{"ts":"2026-11-10 18:00","dem":30000}]});'
EMPTY_REE_PAYLOAD = b'cb({"valoresHorariosGeneracion":[]});'
WEATHER_PAYLOAD = b'[{"current":{"time":"2026-11-10T17:00","temperature_2m":15.9}}]'
NOW = datetime(2026, 11, 10, 17, 58, tzinfo=UTC)


def routed(ree=REE_PAYLOAD, weather=WEATHER_PAYLOAD, ree_status=200):
    def route(url):
        if "demanda.ree.es" in url:
            return FakeResponse(ree_status, ree)
        return FakeResponse(200, weather)

    return route


def landed(paths, folder):
    directory = paths.landing / folder
    return sorted(path.name for path in directory.iterdir()) if directory.exists() else []


def test_capture_writes_atomically(paths):
    written = capture.capture_once(paths, NOW, session=FakeSession(routed()))
    assert len(written) == 2
    assert landed(paths, "ree_generation") == ["20261110T175800Z.jsonp"]
    assert landed(paths, "open_meteo_current") == ["20261110T175800Z.json"]
    assert not list(paths.landing.rglob("*.part"))


def test_capture_requests_local_today(paths):
    session = FakeSession(routed())
    capture.capture_once(paths, datetime(2026, 11, 10, 23, 30, tzinfo=UTC), session=session)
    assert any("fecha=2026-11-11" in url for url in session.urls)


def test_capture_survives_http_error(paths, caplog):
    with caplog.at_level(logging.WARNING):
        written = capture.capture_once(paths, NOW, session=FakeSession(routed(ree_status=503)))
    assert landed(paths, "ree_generation") == []
    assert all(path.suffix == ".json" for path in written)
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_capture_survives_connection_error(paths):
    def route(url):
        import requests

        return requests.ConnectionError("network down")

    assert capture.capture_once(paths, NOW, session=FakeSession(route)) == []


def test_capture_skips_empty_payload(paths):
    capture.capture_once(paths, NOW, session=FakeSession(routed(ree=EMPTY_REE_PAYLOAD)))
    assert landed(paths, "ree_generation") == []


def test_replay_order_and_sleep(tmp_path):
    sources = []
    for name in (
        "ree_generation_2025-01-03.jsonp",
        "ree_generation_2025-01-01.jsonp",
        "ree_generation_2025-01-02.jsonp",
    ):
        path = tmp_path / "raw" / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(name.encode())
        sources.append(path)
    sleeps = []
    landing = tmp_path / "landing"
    assert capture.replay_files(sources, landing, 5.0, sleep=sleeps.append) == 3
    assert sleeps == [5.0, 5.0]
    by_mtime = sorted(landing.iterdir(), key=lambda path: os.stat(path).st_mtime_ns)
    assert [path.name for path in by_mtime] == sorted(path.name for path in sources)


def test_last_capture_time(paths):
    assert capture.last_capture_time(paths.landing) is None
    folder = paths.landing / "ree_generation"
    folder.mkdir(parents=True)
    for name in ("20261107T100000Z.jsonp", "20261108T093000Z.jsonp", "ree_generation_2025-01-01.jsonp"):
        (folder / name).write_bytes(b"x")
    assert capture.last_capture_time(paths.landing) == datetime(2026, 11, 8, 9, 30, tzinfo=UTC)


def seed_last_capture(paths, stamp):
    folder = paths.landing / "ree_generation"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{stamp}.jsonp").write_bytes(REE_PAYLOAD)


def requested_days(session):
    return [url.split("fecha=")[1] for url in session.urls]


def test_catch_up_fetches_missing_days_in_order(paths):
    seed_last_capture(paths, "20261107T100000Z")
    session = FakeSession(routed())
    written = capture.catch_up(paths, datetime(2026, 11, 10, 18, 0, tzinfo=UTC), session=session)
    assert requested_days(session) == ["2026-11-07", "2026-11-08", "2026-11-09"]
    assert len(written) == 3
    names = [path.name for path in sorted(written, key=lambda path: os.stat(path).st_mtime_ns)]
    assert names == sorted(names)


def test_catch_up_noop_when_recent(paths):
    seed_last_capture(paths, "20261109T210000Z")
    session = FakeSession(routed())
    assert capture.catch_up(paths, datetime(2026, 11, 10, 18, 0, tzinfo=UTC), session=session) == []
    assert session.urls == []


def test_catch_up_noop_without_previous_capture(paths):
    session = FakeSession(routed())
    assert capture.catch_up(paths, NOW, session=session) == []
    assert session.urls == []


def test_catch_up_respects_max_days(paths):
    now = datetime(2026, 11, 10, 18, 0, tzinfo=UTC)
    seed_last_capture(paths, (now - timedelta(days=30)).strftime("%Y%m%dT%H%M%SZ"))
    session = FakeSession(routed())
    capture.catch_up(paths, now, session=session, max_days=14)
    days = requested_days(session)
    assert len(days) == 14
    assert days[0] == "2026-10-27" and days[-1] == "2026-11-09"
