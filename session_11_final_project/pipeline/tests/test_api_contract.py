from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests
from common.config import ApiSourceConfig
from ingestion.api_bronze import RequestRateLimiter, fetch_batch, validate_success_payload

CONFIG = ApiSourceConfig(
    base_url="http://service.invalid",
    api_key="test-only-key",
    connect_timeout_seconds=1,
    read_timeout_seconds=1,
    requests_per_minute=100,
)


def response(status_code: int, payload: dict | None = None) -> Mock:
    item = Mock()
    item.status_code = status_code
    item.headers = {}
    item.text = "test response"
    item.json.return_value = payload or {}
    return item


def limiter() -> RequestRateLimiter:
    item = RequestRateLimiter(100)
    item.wait = Mock()
    return item


def test_batch_size_above_fifty_is_rejected() -> None:
    with pytest.raises(ValueError):
        fetch_batch(Mock(), limiter(), CONFIG, [str(i) for i in range(51)])


def test_404_is_not_retried() -> None:
    session = Mock()
    session.get.return_value = response(404)
    shipments, not_found, event = fetch_batch(session, limiter(), CONFIG, ["ORD-1"])
    assert shipments == []
    assert not_found == ["ORD-1"]
    assert event is None
    session.get.assert_called_once()


def test_500_is_retried_once(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.api_bronze.time.sleep", lambda _: None)
    session = Mock()
    session.get.side_effect = [response(500), response(500)]
    _, _, event = fetch_batch(session, limiter(), CONFIG, ["ORD-1"])
    assert event is not None
    assert event.event_type == "server_error_exhausted"
    assert session.get.call_count == 2


def test_timeout_retries_one_two_four_then_blocks(monkeypatch) -> None:
    delays: list[int] = []
    monkeypatch.setattr("ingestion.api_bronze.time.sleep", delays.append)
    session = Mock()
    session.get.side_effect = [requests.Timeout()] * 4
    _, _, event = fetch_batch(session, limiter(), CONFIG, ["ORD-1"])
    assert event is not None
    assert event.event_type == "timeout_exhausted"
    assert delays == [1, 2, 4]


def test_429_uses_retry_after_then_succeeds(monkeypatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr("ingestion.api_bronze.time.sleep", delays.append)
    session = Mock()
    session.get.side_effect = [
        response(429, {"retry_after": 2}),
        response(200, {"shipments": [{"order_id": "ORD-1"}], "not_found": []}),
    ]
    shipments, not_found, event = fetch_batch(session, limiter(), CONFIG, ["ORD-1"])
    assert shipments == [{"order_id": "ORD-1"}]
    assert not_found == []
    assert event is None
    assert delays == [2.0]


def test_invalid_success_shape_is_contract_failure() -> None:
    with pytest.raises(RuntimeError, match="contract failure"):
        validate_success_payload({"shipments": "not-a-list", "not_found": []})


def test_all_found_success_shape_may_omit_not_found() -> None:
    shipments, not_found = validate_success_payload(
        {"shipments": [{"order_id": "ORD-1"}], "count": 1}
    )
    assert shipments == [{"order_id": "ORD-1"}]
    assert not_found == []


def test_response_count_must_match_shipments() -> None:
    with pytest.raises(RuntimeError, match="count"):
        validate_success_payload({"shipments": [{"order_id": "ORD-1"}], "count": 0})
