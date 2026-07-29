"""Unit tests for `AnakinCredentials`.

These test the Block <-> SDK wiring only (that `get_client()` constructs
`anakin.Anakin` with the right kwargs) — no live HTTP, no live Prefect
server/API connection (`Block.save`/`.load` are not exercised here; see
SUBMIT.md for what that would require).
"""

from __future__ import annotations

from prefect_anakin import AnakinCredentials


def test_get_client_passes_api_key() -> None:
    creds = AnakinCredentials(api_key="ask_test_dummy")

    client = creds.get_client()

    assert client is not None


def test_get_client_passes_base_url_override() -> None:
    creds = AnakinCredentials(
        api_key="ask_test_dummy", base_url="https://staging.anakin.io/v1"
    )

    client = creds.get_client()

    assert client is not None


def test_get_client_omits_base_url_when_unset() -> None:
    creds = AnakinCredentials(api_key="ask_test_dummy")

    assert creds.base_url is None
    client = creds.get_client()

    assert client is not None


def test_get_http_client_uses_default_base_url_and_api_key_header() -> None:
    creds = AnakinCredentials(api_key="ask_test_dummy")

    with creds.get_http_client() as http_client:
        assert str(http_client.base_url) == "https://api.anakin.io/v1"
        assert http_client.headers["x-api-key"] == "ask_test_dummy"


def test_get_http_client_honors_base_url_override() -> None:
    creds = AnakinCredentials(api_key="ask_test_dummy", base_url="https://staging.anakin.io/v1")

    with creds.get_http_client() as http_client:
        assert str(http_client.base_url) == "https://staging.anakin.io/v1"
