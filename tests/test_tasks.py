"""Unit tests for the Anakin tasks.

SDK-backed tasks get a mocked `anakin.Anakin` client that returns real
`anakin.models` objects, so the tests cover argument pass-through and the
JSON-serializable shape each task returns. `browser_task` gets a fake
`httpx.Client`. Tasks are called through `.fn(...)`; `test_flow.py` runs
them inside a real Prefect flow.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from anakin.models import (
    AgenticSearchData,
    AgenticSearchResult,
    AIVisibilitySearch,
    AIVisibilitySource,
    BrowserSession,
    CrawlResult,
    Document,
    MapResult,
    Monitor,
    SearchResult,
    SearchResultItem,
    WireActionMatch,
    WireBuildRequest,
    WireCatalog,
    WireLoginResult,
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
    wire_build_status,
    wire_catalog,
    wire_discover,
    wire_identities,
    wire_login,
    wire_read_action,
    wire_write_action,
)


@pytest.fixture
def credentials() -> AnakinCredentials:
    return AnakinCredentials(api_key="ask_" + "0" * 64)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr(credentials, "get_client", lambda: mock)
    return mock


def _assert_json(value: Any) -> None:
    json.dumps(value)  # raises if a datetime or model slipped through


# ─── Scrape and search ──────────────────────────────────────────────────────


def test_scrape_url(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.scrape.return_value = Document.model_validate(
        {
            "id": "job_1",
            "url": "https://example.com",
            "status": "completed",
            "markdown": "# Hello",
            "createdAt": "2026-10-16T10:00:00Z",
        }
    )

    result = scrape_url.fn("https://example.com", credentials, country="gb", use_browser=True)

    assert result["markdown"] == "# Hello"
    assert result["created_at"] == "2026-10-16T10:00:00Z"
    _assert_json(result)
    client.scrape.assert_called_once_with(
        "https://example.com",
        formats=("markdown",),
        country="gb",
        use_browser=True,
        generate_json=False,
        output_schema=None,
        force_fresh=False,
        session_id=None,
        session_name=None,
    )


def test_map(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.map.return_value = MapResult(id="m1", links=["https://a.com/x"], total_links=1)

    result = map.fn("https://a.com", credentials, depth=1, search="x")

    assert result["links"] == ["https://a.com/x"]
    assert client.map.call_args.kwargs["depth"] == 1
    assert client.map.call_args.kwargs["search"] == "x"


def test_crawl(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.crawl.return_value = CrawlResult.model_validate(
        {
            "id": "c1",
            "totalPages": 1,
            "completedPages": 1,
            "results": [{"url": "u", "status": "completed", "markdown": "m"}],
        }
    )

    result = crawl.fn("https://a.com", credentials, include_patterns=["/blog/*"])

    assert result["pages"][0]["markdown"] == "m"
    assert client.crawl.call_args.kwargs["include_patterns"] == ["/blog/*"]
    assert client.crawl.call_args.kwargs["exclude_patterns"] == ()


def test_search(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.search.return_value = SearchResult(
        id="s1", results=[SearchResultItem(url="https://example.com", title="Example")]
    )

    result = search.fn("what is anakin", credentials, limit=3)

    assert result["results"][0]["url"] == "https://example.com"
    client.search.assert_called_once_with("what is anakin", limit=3)


def test_agentic_search(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.agentic_search.return_value = AgenticSearchResult(
        id="a1", status="completed", generated_json=AgenticSearchData(summary="done")
    )

    result = agentic_search.fn("research x", credentials, schema={"type": "object"})

    assert result["generated_json"]["summary"] == "done"
    client.agentic_search.assert_called_once_with(
        "research x", use_browser=True, schema={"type": "object"}
    )


# ─── Wire ───────────────────────────────────────────────────────────────────


def test_wire_discover(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.wire.discover.return_value = [WireActionMatch(action_id="amazon.search", mode="read")]

    result = wire_discover.fn("search amazon", credentials, limit=3)

    assert result[0]["action_id"] == "amazon.search"
    client.wire.discover.assert_called_once_with("search amazon", catalog=None, limit=3)


def test_wire_catalog_lists_all_without_slug(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.wire.catalogs.return_value = [WireCatalog(id="c1", slug="amazon", name="Amazon")]

    result = wire_catalog.fn(credentials)

    assert isinstance(result, list)
    assert result[0]["slug"] == "amazon"
    client.wire.catalog.assert_not_called()


def test_wire_catalog_with_slug(client: MagicMock, credentials: AnakinCredentials) -> None:
    detail = MagicMock()
    detail.model_dump.return_value = {"slug": "amazon", "actions": []}
    client.wire.catalog.return_value = detail

    result = wire_catalog.fn(credentials, slug="amazon")

    assert result == {"slug": "amazon", "actions": []}
    client.wire.catalog.assert_called_once_with("amazon")


def test_wire_read_action_passes_credential(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.wire.run.return_value = WireResult(job_id="j1", status="completed", data={"price": 9})

    result = wire_read_action.fn(
        "amazon.get_product", credentials, params={"asin": "B0"}, credential_id="cred_1"
    )

    assert result["data"] == {"price": 9}
    client.wire.run.assert_called_once_with(
        "amazon.get_product", {"asin": "B0"}, credential_id="cred_1", identity_id=None
    )


def test_wire_read_action_defaults_params(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.wire.run.return_value = WireResult(status="completed")

    wire_read_action.fn("x.list", credentials)

    assert client.wire.run.call_args.args == ("x.list", {})


def test_wire_write_action_runs_benign_action(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.wire.run.return_value = WireResult(status="completed")

    wire_write_action.fn("shop.add_to_cart", credentials, params={"sku": "1"})

    client.wire.run.assert_called_once()


def test_wire_write_action_refuses_payment(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    with pytest.raises(ValueError, match="payments or fund transfers"):
        wire_write_action.fn("shop.checkout", credentials)
    client.wire.run.assert_not_called()


def test_wire_identities(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.wire.identities.return_value = []

    assert wire_identities.fn(credentials, catalog_id="cat_1") == []
    client.wire.identities.assert_called_once_with(catalog_id="cat_1")


def test_wire_login(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.wire.login.return_value = WireLoginResult(
        status="ok", identity_id="i1", credential_id="cred_1"
    )

    result = wire_login.fn("neb", credentials, params={"email": "a@b.c", "password": "x"})

    assert result["credential_id"] == "cred_1"
    client.wire.login.assert_called_once_with(
        "neb",
        {"email": "a@b.c", "password": "x"},
        identity_name=None,
        source_id=None,
        source_ref=None,
    )


def test_wire_build(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.wire.build.return_value = WireBuildRequest(id="b1", status="pending")

    result = wire_build.fn("https://example.com", "list products", credentials)

    assert result["status"] == "pending"
    client.wire.build.assert_called_once_with(
        "https://example.com", "list products", catalog_id=None, visibility="private", force=False
    )


def test_wire_build_refuses_payment(client: MagicMock, credentials: AnakinCredentials) -> None:
    with pytest.raises(ValueError):
        wire_build.fn("https://shop.com", "pay now for the order", credentials)
    client.wire.build.assert_not_called()


def test_wire_build_status(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.wire.get_build.return_value = WireBuildRequest(id="b1", status="success")

    assert wire_build_status.fn("b1", credentials)["status"] == "success"


# ─── Monitors ───────────────────────────────────────────────────────────────


def _monitor(**extra: Any) -> Monitor:
    return Monitor.model_validate({"id": "mon_1", "url": "https://a.com", **extra})


def test_monitor_create_maps_options_and_redacts_secret(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.monitors.create.return_value = _monitor(alertWebhookSecret="whsec_live")

    result = monitor_create.fn(
        "https://a.com", 60, credentials, ai_mode=True, alert_webhook_url="https://hook"
    )

    assert result["id"] == "mon_1"
    assert result["alert_webhook_secret"].startswith("[redacted")
    assert "whsec_live" not in json.dumps(result)
    args, kwargs = client.monitors.create.call_args
    assert args == ("https://a.com", 60)
    assert kwargs["ai_mode"] is True
    assert kwargs["alert_webhook_url"] == "https://hook"
    assert "output_schema" not in kwargs


def test_monitor_list_one_and_all(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.monitors.get.return_value = _monitor(alertWebhookSecret="s")
    client.monitors.list.return_value = [_monitor(alertWebhookSecret="s")]

    one = monitor_list.fn(credentials, monitor_id="mon_1")
    many = monitor_list.fn(credentials)

    assert isinstance(one, dict) and one["alert_webhook_secret"].startswith("[redacted")
    assert isinstance(many, list) and many[0]["alert_webhook_secret"].startswith("[redacted")


def test_monitor_changes(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.monitors.changes.return_value = []

    assert monitor_changes.fn("mon_1", credentials) == []
    client.monitors.changes.assert_called_once_with("mon_1")


@pytest.mark.parametrize(
    ("action", "method"),
    [("pause", "pause"), ("resume", "resume"), ("run_now", "run_now")],
)
def test_monitor_control(
    client: MagicMock, credentials: AnakinCredentials, action: str, method: str
) -> None:
    getattr(client.monitors, method).return_value = _monitor()

    result = monitor_control.fn("mon_1", action, credentials)

    assert result is not None and result["id"] == "mon_1"
    getattr(client.monitors, method).assert_called_once_with("mon_1")


def test_monitor_control_delete(client: MagicMock, credentials: AnakinCredentials) -> None:
    assert monitor_control.fn("mon_1", "delete", credentials) is None
    client.monitors.delete.assert_called_once_with("mon_1")


def test_monitor_control_rejects_unknown_action(credentials: AnakinCredentials) -> None:
    with pytest.raises(ValueError, match="Unknown monitor action"):
        monitor_control.fn("mon_1", "explode", credentials)


# ─── AI visibility ──────────────────────────────────────────────────────────


def test_ai_visibility_sources(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.ai_visibility.sources.return_value = [
        AIVisibilitySource.model_validate({"slug": "chatgpt", "label": "ChatGPT"})
    ]

    assert ai_visibility_sources.fn(credentials)[0]["slug"] == "chatgpt"


def _visibility_result() -> AIVisibilitySearch:
    return AIVisibilitySearch.model_validate(
        {
            "search_id": "v1",
            "status": "completed",
            "results": [{"source": "chatgpt", "status": "completed", "full_content": "long"}],
        }
    )


def test_ai_visibility_search_strips_full_content(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.ai_visibility.search.return_value = _visibility_result()

    result = ai_visibility_search.fn("best scraping api", credentials, sources=["chatgpt"])

    assert "full_content" not in result["results"][0]
    client.ai_visibility.search.assert_called_once_with(
        "best scraping api", sources=["chatgpt"], country="us"
    )


def test_ai_visibility_search_keeps_full_content(
    client: MagicMock, credentials: AnakinCredentials
) -> None:
    client.ai_visibility.search.return_value = _visibility_result()

    result = ai_visibility_search.fn("q", credentials, include_full_content=True)

    assert result["results"][0]["full_content"] == "long"


# ─── Sessions and browser tasks ─────────────────────────────────────────────


def test_session_list(client: MagicMock, credentials: AnakinCredentials) -> None:
    client.sessions.list.return_value = [BrowserSession.model_validate({"id": "s1", "name": "amz"})]

    result = session_list.fn(credentials, domain="amazon.com")

    assert result[0]["id"] == "s1"
    client.sessions.list.assert_called_once_with(domain="amazon.com")


def test_session_delete(client: MagicMock, credentials: AnakinCredentials) -> None:
    session_delete.fn("s1", credentials)
    client.sessions.delete.assert_called_once_with("s1")


def _fake_http(*bodies: Any) -> MagicMock:
    http = MagicMock()
    http.__enter__.return_value = http
    http.__exit__.return_value = False
    responses = []
    for body in bodies:
        response = MagicMock(is_error=False, content=b"x")
        response.json.return_value = body
        responses.append(response)
    http.request.side_effect = responses
    return http


def test_browser_task_polls_until_complete(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    http = _fake_http(
        {"workflow_id": "wf_1"},
        {"status": "running"},
        {"status": "completed", "result": {"success": True, "result": {"x": 1}, "run_id": "r1"}},
    )
    monkeypatch.setattr(credentials, "get_http_client", lambda: http)
    monkeypatch.setattr("prefect_anakin._http.time.sleep", lambda _s: None)

    result = browser_task.fn(
        "find the cheapest TV", credentials, url="https://shop.com", max_steps=5
    )

    assert result["success"] is True
    assert result["run_id"] == "r1"
    method, path = http.request.call_args_list[0].args
    assert (method, path) == ("POST", "/ai/evaluate")
    assert http.request.call_args_list[0].kwargs["json"] == {
        "prompt": "find the cheapest TV",
        "async": True,
        "url": "https://shop.com",
        "max_steps": 5,
    }
    assert http.request.call_args_list[1].args == ("GET", "/ai/jobs/wf_1")


def test_browser_task_raises_on_failure(
    monkeypatch: pytest.MonkeyPatch, credentials: AnakinCredentials
) -> None:
    http = _fake_http({"workflow_id": "wf_1"}, {"status": "failed", "error": "site unreachable"})
    monkeypatch.setattr(credentials, "get_http_client", lambda: http)

    with pytest.raises(RuntimeError, match="site unreachable"):
        browser_task.fn("do something", credentials)


def test_browser_task_refuses_payment(credentials: AnakinCredentials) -> None:
    with pytest.raises(ValueError):
        browser_task.fn("checkout and buy now", credentials)
