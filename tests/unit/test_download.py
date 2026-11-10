from dataclasses import dataclass
from datetime import date

import pytest
import requests

from spain_electricity.download import DownloadError, DownloadJob, download_all, fetch, should_fetch


@dataclass
class FakeResponse:
    status_code: int
    content: bytes


class FakeSession:
    """Returns queued responses (or raises queued exceptions) and records requested URLs."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, timeout):
        self.urls.append(url)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def no_sleep(_seconds):
    return None


def test_should_fetch_rules(tmp_path):
    existing = tmp_path / "f.json"
    existing.write_text("{}")
    today = date(2026, 11, 10)
    assert should_fetch(tmp_path / "missing.json", date(2025, 1, 31), today)
    assert not should_fetch(existing, date(2025, 1, 31), today)
    assert should_fetch(existing, date(2026, 11, 9), today)
    assert should_fetch(existing, date(2026, 11, 10), today)


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
    report = download_all(jobs, session=session, pause_seconds=0, today=date(2026, 11, 10), sleep=no_sleep)
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
    report = download_all(jobs, session=session, pause_seconds=0, today=date(2026, 11, 10), sleep=no_sleep)
    assert report.skipped == 1 and report.fetched == 1
    assert session.urls == ["recent-period"]
    assert (tmp_path / "1").read_bytes() == b"old"
