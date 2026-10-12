from unittest.mock import MagicMock

import pytest
import yadisk

import downloader


@pytest.mark.parametrize("exc, expected", [
    (yadisk.exceptions.UnauthorizedError(), False),
    (yadisk.exceptions.ForbiddenError(), False),
    (yadisk.exceptions.PathNotFoundError(), False),
    (yadisk.exceptions.TooManyRequestsError(), True),
    (yadisk.exceptions.InternalServerError(), True),
    (ConnectionError("reset"), True),
    (OSError("disk"), True),
    (ValueError("bug"), False),
])
def test_should_classify_exception_when_deciding_retry(exc, expected):
    assert downloader._is_retryable(exc) is expected


def test_should_not_retry_listing_when_path_not_found():
    client = MagicMock()
    client.listdir.side_effect = yadisk.exceptions.PathNotFoundError()

    with pytest.raises(yadisk.exceptions.PathNotFoundError):
        downloader._list_dir(client, "/gone")

    assert client.listdir.call_count == 1


def test_should_wait_a_minute_when_rate_limited():
    state = MagicMock()
    state.outcome.exception.return_value = yadisk.exceptions.TooManyRequestsError()

    assert downloader._wait_before_retry(state) == 60.0


def test_should_retry_listing_when_server_error_is_transient(monkeypatch):
    monkeypatch.setattr(downloader._list_dir.retry, "sleep", lambda _: None)
    client = MagicMock()
    client.listdir.side_effect = [yadisk.exceptions.InternalServerError(), ["entry"]]

    result = downloader._list_dir(client, "/")

    assert result == ["entry"]
