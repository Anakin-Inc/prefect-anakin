"""Unit tests for the Anakin Prefect tasks.

These test the task <-> SDK wiring (arguments passed through, dict shape
returned) with the Anakin client's methods mocked — not live HTTP and not
a live Prefect server. Run via `.fn(...)` to call the undecorated function
body directly and avoid needing a Prefect orchestration context; see
SUBMIT.md for why a real `@flow`-orchestrated run was not exercised here.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from anakin.models import (
    AgenticSearchData,
    AgenticSearchResult,
    Document,
    SearchResult,
    SearchResultItem,
)

from prefect_anakin import AnakinCredentials, agentic_search, scrape_url, search


@pytest.fixture
def credentials() -> AnakinCredentials:
    return AnakinCredentials(api_key="ask_test_dummy")


def test_scrape_url_returns_document_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.scrape.return_value = Document(
        id="job_1",
        url="https://example.com",
        status="completed",
        cached=False,
        duration_ms=100,
        markdown="# Hello",
    )
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = scrape_url.fn(
        "https://example.com",
        credentials,
        country="gb",
        use_browser=True,
    )

    assert result["markdown"] == "# Hello"
    assert result["id"] == "job_1"
    mock_client.scrape.assert_called_once_with(
        "https://example.com",
        country="gb",
        use_browser=True,
        generate_json=False,
        force_fresh=False,
        session_id=None,
        session_name=None,
    )


def test_search_returns_results_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.search.return_value = SearchResult(
        id="search_1",
        results=[
            SearchResultItem(url="https://example.com", title="Example", snippet="...")
        ],
    )
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = search.fn("what is anakin", credentials, limit=3)

    assert result["id"] == "search_1"
    assert result["results"][0]["url"] == "https://example.com"
    mock_client.search.assert_called_once_with("what is anakin", limit=3)


def test_agentic_search_returns_structured_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.agentic_search.return_value = AgenticSearchResult(
        id="job_2",
        status="completed",
        generatedJson=AgenticSearchData(summary="Findings.", structured_data={"a": 1}),
    )
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = agentic_search.fn("research this", credentials, schema={"type": "object"})

    assert result["id"] == "job_2"
    assert result["generated_json"]["summary"] == "Findings."
    mock_client.agentic_search.assert_called_once_with(
        "research this", use_browser=True, schema={"type": "object"}
    )
