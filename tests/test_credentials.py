"""Tests for `AnakinCredentials`."""

from __future__ import annotations

import pytest
from anakin import Anakin

from prefect_anakin import AnakinCredentials

KEY = "ask_" + "0" * 64


def test_get_client_returns_sdk_client_with_key() -> None:
    client = AnakinCredentials(api_key=KEY).get_client()

    assert isinstance(client, Anakin)
    assert client.api_key_configured


def test_get_client_without_key_is_keyless(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)

    client = AnakinCredentials().get_client()

    assert not client.api_key_configured


def test_api_key_is_not_exposed_in_repr() -> None:
    creds = AnakinCredentials(api_key=KEY)

    assert KEY not in repr(creds)
    assert KEY not in str(creds.model_dump())


def test_http_client_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    with AnakinCredentials(api_key=KEY).get_http_client() as http:
        assert str(http.base_url).rstrip("/") == "https://api.anakin.io/v1"
        assert http.headers["x-api-key"] == KEY


def test_http_client_base_url_override() -> None:
    creds = AnakinCredentials(api_key=KEY, base_url="https://staging.example/v1")

    with creds.get_http_client() as http:
        assert str(http.base_url).rstrip("/") == "https://staging.example/v1"


def test_http_client_falls_back_to_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANAKIN_API_KEY", KEY)

    with AnakinCredentials().get_http_client() as http:
        assert http.headers["x-api-key"] == KEY


def test_http_client_requires_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)

    with pytest.raises(ValueError, match="needs an Anakin API key"):
        AnakinCredentials().get_http_client()
