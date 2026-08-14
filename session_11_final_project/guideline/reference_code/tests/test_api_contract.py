from __future__ import annotations

from unittest.mock import Mock

import pytest

from jobs.api_bronze import RequestRateLimiter, fetch_batch


def response(status_code: int, payload: dict | None = None) -> Mock:
    item = Mock()
    item.status_code = status_code
    item.headers = {}
    item.text = "response"
    item.json.return_value = payload or {}
    return item


def limiter() -> RequestRateLimiter:
    item = RequestRateLimiter(0)
    item.wait = Mock()
    return item


def test_batch_size_above_fifty_is_rejected() -> None:
    with pytest.raises(ValueError):
        fetch_batch(Mock(), limiter(), "http://api:8000", "key", [str(i) for i in range(51)])


def test_404_is_not_retried() -> None:
    session = Mock()
    session.get.return_value = response(404)
    shipments, not_found, error = fetch_batch(
        session, limiter(), "http://api:8000", "key", ["ORD-1"]
    )
    assert shipments == []
    assert not_found == ["ORD-1"]
    assert error is None
    session.get.assert_called_once()


def test_500_is_retried_once(monkeypatch) -> None:
    monkeypatch.setattr("jobs.api_bronze.time.sleep", lambda _: None)
    session = Mock()
    session.get.side_effect = [response(500), response(500)]
    shipments, not_found, error = fetch_batch(
        session, limiter(), "http://api:8000", "key", ["ORD-1"]
    )
    assert shipments == []
    assert not_found == []
    assert error is not None
    assert error.error_type == "http_500_exhausted"
    assert session.get.call_count == 2


def test_429_uses_retry_after_then_succeeds(monkeypatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr("jobs.api_bronze.time.sleep", delays.append)
    session = Mock()
    session.get.side_effect = [
        response(429, {"retry_after": 2}),
        response(200, {"shipments": [{"order_id": "ORD-1"}], "not_found": []}),
    ]
    shipments, not_found, error = fetch_batch(
        session, limiter(), "http://api:8000", "key", ["ORD-1"]
    )
    assert shipments == [{"order_id": "ORD-1"}]
    assert not_found == []
    assert error is None
    assert delays == [2.0]
