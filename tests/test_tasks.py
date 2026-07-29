"""Unit tests for the Anakin Prefect tasks.

These test the task <-> SDK wiring (arguments passed through, dict shape
returned) with the Anakin client's methods mocked — not live HTTP and not
a live Prefect server. Run via `.fn(...)` to call the undecorated function
body directly and avoid needing a Prefect orchestration context; see
SUBMIT.md for why a real `@flow`-orchestrated run was not exercised here.

For the raw-HTTP tasks (no `anakin-sdk` coverage — see `tasks.py`'s module
docstring), `AnakinCredentials.get_http_client()` is monkeypatched to a fake
context-managed client whose `.request(...)` replays canned JSON bodies, so
these test the task <-> `prefect_anakin._http` wiring the same way the
SDK-backed tests test task <-> `anakin.Anakin` wiring — not live HTTP.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from anakin.models import (
    AgenticSearchData,
    AgenticSearchResult,
    BrowserSession,
    CrawlPage,
    CrawlResult,
    Document,
    MapResult,
    SearchResult,
    SearchResultItem,
    WireResult,
)

from prefect_anakin import (
    AnakinCredentials,
    agentic_search,
    ai_visibility_search,
    ai_visibility_sources,
    browser_task,
    crawl,
    map,
    monitor_changes,
    monitor_control,
    monitor_create,
    monitor_list,
    scrape_url,
    search,
    session_delete,
    session_list,
    wire_build,
    wire_catalog,
    wire_discover,
    wire_identities,
    wire_login,
    wire_read_action,
    wire_write_action,
)


@pytest.fixture
def credentials() -> AnakinCredentials:
    return AnakinCredentials(api_key="ask_test_dummy")


def _fake_response(body: Any) -> MagicMock:
    response = MagicMock()
    response.is_error = False
    response.content = b"x"
    response.json.return_value = body
    return response


def _mock_http_client(*bodies: Any) -> MagicMock:
    """A fake `httpx.Client` context manager replaying `bodies` across
    successive `.request(...)` calls, standing in for
    `AnakinCredentials.get_http_client()`."""
    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = False
    mock_client.request.side_effect = [_fake_response(body) for body in bodies]
    return mock_client


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


# ─── map / crawl (SDK-backed) ──────────────────────────────────────────────


def test_map_returns_map_result_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.map.return_value = MapResult(
        id="map_1", url="https://example.com", links=["https://example.com/a"], totalLinks=1
    )
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = map.fn("https://example.com", credentials, depth=3)

    assert result["total_links"] == 1
    mock_client.map.assert_called_once_with(
        "https://example.com",
        limit=100,
        depth=3,
        limit_per_level=100,
        include_subdomains=False,
        include_external_links=False,
        use_browser=False,
        search=None,
        session_id=None,
    )


def test_crawl_returns_crawl_result_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.crawl.return_value = CrawlResult(
        id="crawl_1",
        url="https://example.com",
        totalPages=1,
        completedPages=1,
        results=[CrawlPage(url="https://example.com", status="completed", markdown="hi")],
    )
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = crawl.fn("https://example.com", credentials, max_pages=5)

    assert result["completed_pages"] == 1
    assert result["pages"][0]["markdown"] == "hi"
    mock_client.crawl.assert_called_once_with(
        "https://example.com",
        max_pages=5,
        depth=1,
        country="us",
        use_browser=False,
        include_patterns=(),
        exclude_patterns=(),
        session_id=None,
    )


# ─── Wire: execution (SDK-backed) ──────────────────────────────────────────


def test_wire_read_action_returns_wire_result_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.wire.return_value = WireResult(job_id="job_1", status="completed", data={"price": 10})
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = wire_read_action.fn("amazon.get_product", credentials, params={"asin": "B1"})

    assert result["data"] == {"price": 10}
    mock_client.wire.assert_called_once_with("amazon.get_product", {"asin": "B1"})


def test_wire_read_action_defaults_params_to_empty_dict(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.wire.return_value = WireResult(job_id="job_1", status="completed")
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    wire_read_action.fn("some.action", credentials)

    mock_client.wire.assert_called_once_with("some.action", {})


def test_wire_write_action_runs_when_benign(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.wire.return_value = WireResult(job_id="job_2", status="completed")
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = wire_write_action.fn(
        "linkedin.submit_contact_form", credentials, params={"name": "a"}
    )

    assert result["status"] == "completed"
    mock_client.wire.assert_called_once_with("linkedin.submit_contact_form", {"name": "a"})


def test_wire_write_action_refuses_financial_action(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    with pytest.raises(ValueError, match="financial transactions"):
        wire_write_action.fn("amazon.checkout", credentials, params={})

    mock_client.wire.assert_not_called()


# ─── Sessions (SDK-backed) ─────────────────────────────────────────────────


def test_session_list_returns_list_of_dicts(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    mock_client.sessions.list.return_value = [
        BrowserSession(sessionId="sess_1", name="amazon-login", websiteDomain="amazon.com")
    ]
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    result = session_list.fn(credentials, domain="amazon.com")

    assert result[0]["id"] == "sess_1"
    mock_client.sessions.list.assert_called_once_with(domain="amazon.com")


def test_session_delete_calls_sdk_delete(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_client = MagicMock()
    monkeypatch.setattr(credentials, "get_client", lambda: mock_client)

    session_delete.fn("sess_1", credentials)

    mock_client.sessions.delete.assert_called_once_with("sess_1")


# ─── Wire: discovery, identities, login, build (raw HTTP) ─────────────────


def test_wire_discover_calls_wire_resolve(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"results": [{"action_id": "amazon.search"}]})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = wire_discover.fn("top phones on walmart", credentials, limit=3)

    assert result["results"][0]["action_id"] == "amazon.search"
    mock_http_client.request.assert_called_once_with(
        "GET", "/wire/resolve", json=None, params={"q": "top phones on walmart", "limit": 3}
    )


def test_wire_catalog_with_slug(monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials) -> None:
    mock_http_client = _mock_http_client({"actions": []})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    wire_catalog.fn(credentials, slug="walmart")

    mock_http_client.request.assert_called_once_with(
        "GET", "/wire/catalog/walmart", json=None, params=None
    )


def test_wire_catalog_without_slug_lists_all(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"catalogs": []})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    wire_catalog.fn(credentials)

    mock_http_client.request.assert_called_once_with("GET", "/wire/catalog", json=None, params=None)


def test_wire_identities_filters_by_catalog(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"identities": []})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    wire_identities.fn(credentials, catalog_id="cat_1")

    mock_http_client.request.assert_called_once_with(
        "GET", "/wire/identities", json=None, params={"catalog_id": "cat_1"}
    )


def test_wire_login_posts_catalog_and_params(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"id": "cred_1"})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = wire_login.fn(
        "neb", credentials, params={"email": "a@b.com", "password": "x"}
    )

    assert result["id"] == "cred_1"
    mock_http_client.request.assert_called_once_with(
        "POST",
        "/wire/login",
        json={"catalog_slug": "neb", "params": {"email": "a@b.com", "password": "x"}},
        params=None,
    )


def test_wire_build_runs_when_benign(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"status": "pending"})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = wire_build.fn("https://example.com", "Extract product prices", credentials)

    assert result["status"] == "pending"


def test_wire_build_refuses_financial_goal(credentials: AnakinCredentials) -> None:
    # The guard runs before any HTTP client is built, so no monkeypatch needed.
    with pytest.raises(ValueError, match="financial transactions"):
        wire_build.fn("https://example.com", "checkout and buy now", credentials)


# ─── Website Monitoring (raw HTTP) ─────────────────────────────────────────


def test_monitor_create_redacts_webhook_secret(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client(
        {"id": "mon_1", "url": "https://example.com", "alertWebhookSecret": "whsec_x"}
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = monitor_create.fn("https://example.com", 30, credentials)

    assert result["id"] == "mon_1"
    assert result["alertWebhookSecret"] == "[redacted — view in the Anakin dashboard]"
    call_kwargs = mock_http_client.request.call_args
    assert call_kwargs.args == ("POST", "/monitors")
    assert call_kwargs.kwargs["json"]["intervalMinutes"] == 30


def test_monitor_list_fetches_single_monitor_by_id(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"id": "mon_1"})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    monitor_list.fn(credentials, monitor_id="mon_1")

    mock_http_client.request.assert_called_once_with("GET", "/monitors/mon_1", json=None, params=None)


def test_monitor_changes_fetches_change_history(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"changes": []})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    monitor_changes.fn("mon_1", credentials)

    mock_http_client.request.assert_called_once_with(
        "GET", "/monitors/mon_1/changes", json=None, params=None
    )


@pytest.mark.parametrize(
    ("action", "method", "path"),
    [
        ("pause", "POST", "/monitors/mon_1/pause"),
        ("resume", "POST", "/monitors/mon_1/resume"),
        ("run_now", "POST", "/monitors/mon_1/run"),
        ("delete", "DELETE", "/monitors/mon_1"),
    ],
)
def test_monitor_control_dispatches_verb(
    monkeypatch: pytest.MonkeyPatch,
    credentials: AnakinCredentials,
    action: str,
    method: str,
    path: str,
) -> None:
    mock_http_client = _mock_http_client({"id": "mon_1"})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    monitor_control.fn("mon_1", action, credentials)

    mock_http_client.request.assert_called_once_with(method, path, json=None, params=None)


def test_monitor_control_rejects_unknown_action(credentials: AnakinCredentials) -> None:
    with pytest.raises(ValueError, match="Unknown monitor action"):
        monitor_control.fn("mon_1", "cancel", credentials)


# ─── AI Visibility (raw HTTP) ───────────────────────────────────────────────


def test_ai_visibility_sources_returns_raw_body(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client({"sources": [{"slug": "chatgpt", "label": "ChatGPT"}]})
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = ai_visibility_sources.fn(credentials)

    assert result["sources"][0]["slug"] == "chatgpt"


def test_ai_visibility_search_polls_and_strips_full_content(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client(
        {"search_id": "s_1", "status": "running"},
        {"search_id": "s_1", "status": "running"},
        {
            "search_id": "s_1",
            "status": "completed",
            "synthesis": "They agree.",
            "results": [{"source": "chatgpt", "summary": "ok", "full_content": "very long..."}],
        },
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)
    monkeypatch.setattr("prefect_anakin._http.time.sleep", lambda _seconds: None)

    result = ai_visibility_search.fn("what do AI engines say about acme", credentials)

    assert result["status"] == "completed"
    assert "full_content" not in result["results"][0]
    assert mock_http_client.request.call_count == 3


def test_ai_visibility_search_includes_full_content_when_requested(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    # First body answers the POST /ai-visibility/search submit; the second
    # answers the poll's first GET (which always fires at least once).
    mock_http_client = _mock_http_client(
        {"search_id": "s_1", "status": "running"},
        {"search_id": "s_1", "status": "completed", "results": [{"full_content": "text"}]},
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)

    result = ai_visibility_search.fn("q", credentials, include_full_content=True)

    assert result["results"][0]["full_content"] == "text"


# ─── AI browser automation (raw HTTP) ──────────────────────────────────────


def test_browser_task_polls_async_job_to_completion(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client(
        {"workflow_id": "wf_1", "status": "accepted"},
        {"workflow_id": "wf_1", "status": "running"},
        {
            "workflow_id": "wf_1",
            "status": "completed",
            "result": {"success": True, "result": {"cheapest": "TV X"}, "run_id": "run_1"},
        },
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)
    monkeypatch.setattr("prefect_anakin._http.time.sleep", lambda _seconds: None)

    result = browser_task.fn("find the cheapest TV", credentials)

    assert result["success"] is True
    assert result["run_id"] == "run_1"


def test_browser_task_refuses_financial_prompt(credentials: AnakinCredentials) -> None:
    with pytest.raises(ValueError, match="financial transactions"):
        browser_task.fn("checkout and buy now this item", credentials)


def test_browser_task_raises_on_failed_job(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    mock_http_client = _mock_http_client(
        {"workflow_id": "wf_1"},
        {"workflow_id": "wf_1", "status": "failed", "error": "target site unreachable"},
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: mock_http_client)
    monkeypatch.setattr("prefect_anakin._http.time.sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="target site unreachable"):
        browser_task.fn("do something", credentials)
