import os
from datetime import date, datetime

import pytest
import requests
from helpers import FakeResponse, FakeSession

from spain_electricity.download import DownloadError, DownloadJob, download_all, fetch, should_fetch


def no_sleep(_seconds):
    return None


def written_on(path, day):
    path.write_text("{}")
    stamp = datetime(day.year, day.month, day.day, 12).timestamp()
    os.utime(path, (stamp, stamp))
    return path


def test_should_fetch_missing_file(tmp_path):
    assert should_fetch(tmp_path / "missing.json", date(2025, 1, 31))


def test_should_fetch_skips_settled_file(tmp_path):
    assert not should_fetch(written_on(tmp_path / "f", date(2025, 2, 5)), date(2025, 1, 31))
    assert not should_fetch(written_on(tmp_path / "g", date(2025, 2, 2)), date(2025, 1, 31))


def test_should_fetch_refreshes_file_written_before_period_settled(tmp_path):
    assert should_fetch(written_on(tmp_path / "f", date(2025, 1, 31)), date(2025, 1, 31))
    assert should_fetch(written_on(tmp_path / "g", date(2025, 2, 1)), date(2025, 1, 31))


def test_should_fetch_respects_settle_days(tmp_path):
    path = written_on(tmp_path / "f", date(2025, 2, 3))
    assert should_fetch(path, date(2025, 1, 31), settle_days=7)
    assert not should_fetch(path, date(2025, 1, 31), settle_days=1)


def test_fetch_is_atomic(tmp_path):
    session = FakeSession([FakeResponse(200, b"cb({});")])
    fetch("u", tmp_path / "sub" / "a.jsonp", session=session, sleep=no_sleep)
    assert (tmp_path / "sub" / "a.jsonp").read_bytes() == b"cb({});"
    assert not list(tmp_path.rglob("*.part"))


def test_fetch_retries_then_fails(tmp_path):
    session = FakeSession([FakeResponse(400, b"{}")] * 3)
    with pytest.raises(DownloadError):
        fetch("u", tmp_path / "a.json", session=session, retries=3, sleep=no_sleep)
    assert not list(tmp_path.iterdir())
    assert len(session.urls) == 3


def test_failed_fetch_creates_no_folder(tmp_path):
    session = FakeSession([FakeResponse(400, b"{}")] * 3)
    with pytest.raises(DownloadError):
        fetch("u", tmp_path / "redata_demand" / "2025" / "a.json", session=session, retries=3, sleep=no_sleep)
    assert not (tmp_path / "redata_demand").exists()


def test_fetch_retries_on_request_exception(tmp_path):
    session = FakeSession([requests.ConnectionError("down"), FakeResponse(200, b"ok")])
    fetch("u", tmp_path / "a.json", session=session, sleep=no_sleep)
    assert (tmp_path / "a.json").read_bytes() == b"ok"


def test_validator_rejects_error_body(tmp_path):
    body = b'{"errors":[{"status":"400"}]}'
    session = FakeSession([FakeResponse(200, body), FakeResponse(200, b'{"included":[]}')])
    fetch(
        "u",
        tmp_path / "a.json",
        session=session,
        validator=lambda content: b'"errors"' not in content,
        sleep=no_sleep,
    )
    assert b"included" in (tmp_path / "a.json").read_bytes()


def test_download_all_continues_after_failure(tmp_path):
    session = FakeSession([FakeResponse(500, b"")] * 3 + [FakeResponse(200, b"ok")])
    jobs = [
        DownloadJob("bad", tmp_path / "1", date(2025, 1, 1)),
        DownloadJob("good", tmp_path / "2", date(2025, 1, 2)),
    ]
    report = download_all(jobs, session=session, pause_seconds=0, sleep=no_sleep)
    assert report.failed == ["bad"]
    assert report.fetched == 1
    assert (tmp_path / "2").read_bytes() == b"ok"


def test_download_all_skips_complete_files(tmp_path):
    (tmp_path / "1").write_bytes(b"old")
    session = FakeSession([FakeResponse(200, b"new")])
    jobs = [
        DownloadJob("old-period", tmp_path / "1", date(2025, 1, 1)),
        DownloadJob("recent-period", tmp_path / "2", date(2026, 11, 10)),
    ]
    report = download_all(jobs, session=session, pause_seconds=0, sleep=no_sleep)
    assert report.skipped == 1 and report.fetched == 1
    assert session.urls == ["recent-period"]
    assert (tmp_path / "1").read_bytes() == b"old"
