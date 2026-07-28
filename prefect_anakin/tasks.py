"""Tasks for the Anakin web data platform.

All 21 of Anakin's REST API capabilities (base URL `https://api.anakin.io/v1`,
auth via `X-API-Key`) as Prefect `@task` functions:

SDK-backed (delegate to `anakin.Anakin`, via `AnakinCredentials.get_client()`,
for the actual HTTP work, request bodies, and job polling):

- `scrape_url`         -> POST /url-scraper, polls GET /url-scraper/:jobId
- `search`              -> POST /search (synchronous, no polling)
- `agentic_search`      -> POST /agentic-search, polls GET /agentic-search/:jobId
- `map`                 -> POST /map, polls GET /map/:jobId
- `crawl`               -> POST /crawl, polls GET /crawl/:jobId
- `wire_read_action`    -> POST /holocron/task, polls GET /holocron/jobs/:jobId
- `wire_write_action`   -> same as `wire_read_action`, plus a financial-
                           transaction guard (see `prefect_anakin._policy`)
- `session_list`        -> GET /sessions (via `Anakin.sessions.list`)
- `session_delete`      -> DELETE /sessions/:id (via `Anakin.sessions.delete`)

Raw-HTTP (call the API directly over `httpx`, via
`AnakinCredentials.get_http_client()` — confirmed missing from `anakin-sdk`
v0.1.0 by reading `anakin-py/src/anakin/client.py` directly, not assumed; see
`prefect_anakin._http` for the shared request/poll/redaction helpers):

- `wire_discover`           -> GET /wire/resolve
- `wire_catalog`            -> GET /wire/catalog[/:slug]
- `wire_identities`         -> GET /wire/identities
- `wire_login`               -> POST /wire/login
- `wire_build`               -> POST /wire/build-request (financial-transaction guard)
- `monitor_create`          -> POST /monitors
- `monitor_list`             -> GET /monitors[/:id]
- `monitor_changes`         -> GET /monitors/:id/changes
- `monitor_control`         -> POST /monitors/:id/{pause,resume,run} or DELETE /monitors/:id
- `ai_visibility_search`    -> POST /ai-visibility/search, polls GET /ai-visibility/search/:id
- `ai_visibility_sources`   -> GET /ai-visibility/sources
- `browser_task`             -> POST /ai/evaluate (async), polls GET /ai/jobs/:id (financial-transaction guard)

Ground truth for every endpoint, request body, and response shape is
Anakin's own MCP server source (`anakin-mcp/src/client.ts`,
`anakin-mcp/src/tools/*.ts`) plus `anakin-py/src/anakin/client.py` for the
SDK-backed ones — not the public API docs (previously found to omit real
parameters). Task signatures and docstring style (Args/Returns, credentials
block as an explicit parameter rather than an implicit global) mirror
`prefect_slack.messages.send_chat_message` and
`prefect_aws.s3.adownload_from_bucket`, both pulled from `PrefectHQ/prefect`'s
current `src/integrations/` tree.
"""

from __future__ import annotations

from typing import Any

from prefect import task
from prefect.logging import get_run_logger

from prefect_anakin import _http as anakin_http
from prefect_anakin._policy import financial_block_reason
from prefect_anakin.credentials import AnakinCredentials


@task(name="anakin_scrape_url", retries=2, retry_delay_seconds=5)
def scrape_url(
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    country: str = "us",
    use_browser: bool = False,
    generate_json: bool = False,
    force_fresh: bool = False,
    session_id: str | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    """Scrape a single URL to markdown, optionally AI-extracting structured JSON.

    Submits to `POST /url-scraper` and polls `GET /url-scraper/:jobId`
    until the job reaches a terminal state (handled internally by
    `anakin-sdk`'s `Anakin.scrape`).

    Args:
        url: The URL to scrape.
        anakin_credentials: Block holding the Anakin API key.
        country: Proxy country code, e.g. "us", "gb", "de". Default "us".
        use_browser: Use a headless browser — best for JS-heavy sites.
            Default False.
        generate_json: AI-extract structured JSON from the page content.
            Default False.
        force_fresh: Bypass the cache and force a fresh extraction.
            Default False.
        session_id: Reuse an existing authenticated browser session.
        session_name: Name of a saved session to reuse (alternative to
            `session_id`).

    Returns:
        The scraped `Document` as a dict (`id`, `url`, `status`, `markdown`,
        `generated_json`, etc. — see `anakin.models.Document`).
    """
    logger = get_run_logger()
    logger.info("Scraping %s (use_browser=%s, generate_json=%s)", url, use_browser, generate_json)

    client = anakin_credentials.get_client()
    document = client.scrape(
        url,
        country=country,
        use_browser=use_browser,
        generate_json=generate_json,
        force_fresh=force_fresh,
        session_id=session_id,
        session_name=session_name,
    )

    logger.info("Scrape of %s completed (job %s, cached=%s)", url, document.id, document.cached)
    return document.model_dump()


@task(name="anakin_search", retries=2, retry_delay_seconds=5)
def search(
    prompt: str,
    anakin_credentials: AnakinCredentials,
    *,
    limit: int = 5,
) -> dict[str, Any]:
    """AI-powered web search with citations. Synchronous — no polling.

    Submits to `POST /search` and returns directly.

    Args:
        prompt: The search query or question.
        anakin_credentials: Block holding the Anakin API key.
        limit: Maximum number of results to return. Default 5.

    Returns:
        The `SearchResult` as a dict — `id` plus a `results` list of
        `{url, title, snippet, date, last_updated}` (see
        `anakin.models.SearchResult`).
    """
    logger = get_run_logger()
    logger.info("Searching Anakin for: %s (limit=%d)", prompt, limit)

    client = anakin_credentials.get_client()
    result = client.search(prompt, limit=limit)

    logger.info("Search returned %d result(s)", len(result.results))
    return result.model_dump()


@task(name="anakin_agentic_search", retries=1, retry_delay_seconds=15)
def agentic_search(
    prompt: str,
    anakin_credentials: AnakinCredentials,
    *,
    use_browser: bool = True,
    schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Multi-stage AI research pipeline: search, scrape citations, extract
    structured data with a summary.

    Submits to `POST /agentic-search` and polls `GET /agentic-search/:jobId`
    until the job reaches a terminal state (handled internally by
    `anakin-sdk`'s `Anakin.agentic_search`). Slower than `search` (can take
    minutes) — use for research questions that need synthesis across
    multiple sources, not quick lookups.

    Args:
        prompt: The research question or topic (max 8KB).
        anakin_credentials: Block holding the Anakin API key.
        use_browser: Use a headless browser for citation scraping.
            Default True.
        schema: Optional JSON Schema (max 50KB) describing the desired
            structured output. Leave unset to let the pipeline choose its
            own shape.

    Returns:
        The `AgenticSearchResult` as a dict — `id`, `status`,
        `generated_json` (`{summary, structured_data, data_schema}`), etc.
        (see `anakin.models.AgenticSearchResult`).
    """
    logger = get_run_logger()
    logger.info("Running Anakin agentic search: %s (use_browser=%s)", prompt, use_browser)

    client = anakin_credentials.get_client()
    result = client.agentic_search(prompt, use_browser=use_browser, schema=schema)

    logger.info("Agentic search %s completed with status=%s", result.id, result.status)
    return result.model_dump()


# ─── map / crawl (SDK-backed) ─────────────────────────────────────────────


@task(name="anakin_map", retries=2, retry_delay_seconds=5)
def map(  # noqa: A001 - matches anakin-sdk's `Anakin.map` method name
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    limit: int = 100,
    depth: int = 2,
    limit_per_level: int = 100,
    include_subdomains: bool = False,
    include_external_links: bool = False,
    use_browser: bool = False,
    search: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Discover URLs reachable from a starting URL (sitemap + link traversal).

    Submits to `POST /map` and polls `GET /map/:jobId` until the job reaches
    a terminal state (handled internally by `anakin-sdk`'s `Anakin.map`).
    Useful for understanding a domain's structure before crawling, or for
    finding the sub-pages an agent should scrape.

    Args:
        url: The starting URL for discovery (typically a homepage or section root).
        anakin_credentials: Block holding the Anakin API key.
        limit: Maximum number of URLs to return overall. Default 100.
        depth: How many link-hops from the starting URL to follow. Default 2.
        limit_per_level: Maximum URLs collected per depth level (controls
            breadth). Default 100.
        include_subdomains: Include URLs on subdomains of the starting host.
        include_external_links: Also collect (but do not follow) external links.
        use_browser: Render with a headless browser — needed for SPAs.
        search: Optional keyword filter — only return URLs whose path/title matches.
        session_id: Reuse an existing authenticated browser session.

    Returns:
        The `MapResult` as a dict (`id`, `url`, `links`, `total_links`,
        `external_links`, `total_external_links`, `duration_ms` — see
        `anakin.models.MapResult`).
    """
    logger = get_run_logger()
    logger.info("Mapping %s (depth=%d, limit=%d)", url, depth, limit)

    client = anakin_credentials.get_client()
    result = client.map(
        url,
        limit=limit,
        depth=depth,
        limit_per_level=limit_per_level,
        include_subdomains=include_subdomains,
        include_external_links=include_external_links,
        use_browser=use_browser,
        search=search,
        session_id=session_id,
    )

    logger.info("Map of %s found %d link(s)", url, result.total_links)
    return result.model_dump()


@task(name="anakin_crawl", retries=1, retry_delay_seconds=10)
def crawl(
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    max_pages: int = 10,
    depth: int = 1,
    country: str = "us",
    use_browser: bool = False,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Bulk-fetch markdown across a site.

    Submits to `POST /crawl` and polls `GET /crawl/:jobId` until the job
    reaches a terminal state (handled internally by `anakin-sdk`'s
    `Anakin.crawl`). Use when an agent needs the contents of many pages at
    once (catalog ingestion, site-wide RAG corpus); pair with
    `include_patterns`/`exclude_patterns` to scope which URLs are fetched.

    Args:
        url: The starting URL.
        anakin_credentials: Block holding the Anakin API key.
        max_pages: Hard cap on pages fetched. Default 10.
        depth: Link-hops from the starting URL to follow. Default 1.
        country: Proxy country code. Default "us".
        use_browser: Render each page in a headless browser — for SPAs.
        include_patterns: Glob/regex patterns; only URLs matching at least
            one pattern are fetched.
        exclude_patterns: Glob/regex patterns; URLs matching any pattern
            are skipped.
        session_id: Reuse an existing authenticated browser session.

    Returns:
        The `CrawlResult` as a dict (`id`, `url`, `total_pages`,
        `completed_pages`, `pages` — each a `{url, status, markdown, html,
        duration_ms, error}` — `duration_ms`; see `anakin.models.CrawlResult`).
    """
    logger = get_run_logger()
    logger.info("Crawling %s (max_pages=%d, depth=%d)", url, max_pages, depth)

    client = anakin_credentials.get_client()
    result = client.crawl(
        url,
        max_pages=max_pages,
        depth=depth,
        country=country,
        use_browser=use_browser,
        include_patterns=include_patterns or (),
        exclude_patterns=exclude_patterns or (),
        session_id=session_id,
    )

    logger.info("Crawl of %s fetched %d/%d page(s)", url, result.completed_pages, result.total_pages)
    return result.model_dump()


# ─── Wire: execution (SDK-backed) ─────────────────────────────────────────


@task(name="anakin_wire_read_action", retries=2, retry_delay_seconds=5)
def wire_read_action(
    action_id: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a Wire READ action — one that extracts data without changing state
    on the target site.

    Submits to `POST /holocron/task` (the real route behind Wire in
    `anakin-sdk`; `anakin-mcp`'s own client hits `/wire/task`, an alias for
    the same endpoint) and polls `GET /holocron/jobs/:jobId` until the job
    reaches a terminal state (handled internally by `anakin-sdk`'s
    `Anakin.wire`). Discover `action_id`s and confirm the action's type is
    "read" first with `wire_discover`/`wire_catalog` — this task does not
    check that server-side. For state-changing actions (type "write") use
    `wire_write_action` instead.

    Note: `anakin-sdk` v0.1.0's `Anakin.wire()` takes only `action_id`,
    `params`, and `poll_timeout` (confirmed by reading
    `anakin-py/src/anakin/client.py`) — it has no `credential_id`/
    `identity_id` parameter yet, so actions whose `auth_mode` is "required"
    cannot be run through this task until the SDK adds that.

    Args:
        action_id: The action to run (from `wire_discover` / `wire_catalog`).
        anakin_credentials: Block holding the Anakin API key.
        params: The action's input parameters, matching its parameter schema
            from discovery. Omit for actions that take none.

    Returns:
        The `WireResult` as a dict (`job_id`, `status`, `data`,
        `credits_used`, `execution_ms`, `error` — see `anakin.models.WireResult`).
    """
    logger = get_run_logger()
    logger.info("Running Wire read action %s", action_id)

    client = anakin_credentials.get_client()
    result = client.wire(action_id, params or {})

    logger.info("Wire read action %s completed with status=%s", action_id, result.status)
    return result.model_dump()


@task(name="anakin_wire_write_action", retries=1, retry_delay_seconds=10)
def wire_write_action(
    action_id: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a Wire WRITE action — one that performs a state-changing
    interaction on the target site (submit a form, add to cart, post
    content, update settings).

    Same execution path as `wire_read_action` (`POST /holocron/task`, polled
    via `GET /holocron/jobs/:jobId`, handled internally by `anakin-sdk`'s
    `Anakin.wire`) — see its docstring for the same `credential_id`
    limitation. Discover `action_id`s and confirm the action's type is
    "write" first with `wire_discover`/`wire_catalog`.

    Refuses to run (raises `ValueError`, without calling the API) any action
    that looks like it completes a payment or transfers funds — same policy
    `anakin-mcp`'s `wire_write_action` tool enforces
    (`anakin-mcp/src/tools/policy.ts::financialBlockReason`), ported to
    Python in `prefect_anakin._policy`.

    Args:
        action_id: The action to run (from `wire_discover` / `wire_catalog`).
        anakin_credentials: Block holding the Anakin API key.
        params: The action's input parameters, matching its parameter schema
            from discovery. Omit for actions that take none.

    Returns:
        The `WireResult` as a dict — same shape as `wire_read_action`.

    Raises:
        ValueError: If `action_id`/`params` look like a payment or
            fund-transfer request.
    """
    logger = get_run_logger()
    resolved_params = params or {}

    blocked = financial_block_reason(f"{action_id} {resolved_params}")
    if blocked:
        logger.error("Refusing Wire write action %s: %s", action_id, blocked)
        raise ValueError(blocked)

    logger.info("Running Wire write action %s", action_id)
    client = anakin_credentials.get_client()
    result = client.wire(action_id, resolved_params)

    logger.info("Wire write action %s completed with status=%s", action_id, result.status)
    return result.model_dump()


# ─── Sessions (SDK-backed) ─────────────────────────────────────────────────


@task(name="anakin_session_list", retries=2, retry_delay_seconds=5)
def session_list(
    anakin_credentials: AnakinCredentials,
    *,
    domain: str | None = None,
) -> list[dict[str, Any]]:
    """List saved browser sessions — encrypted login states reusable by
    `scrape_url`/`crawl`/`monitor_create`/`browser_task`.

    Calls `anakin-sdk`'s `Anakin.sessions.list()` (`GET /sessions`) directly
    — synchronous, no polling.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        domain: Filter to sessions for one website domain, e.g. "amazon.com".

    Returns:
        A list of `BrowserSession` dicts (`id`, `name`, `website_url`,
        `website_domain`, `is_active`, `cookie_count`, `storage_item_count`,
        etc. — see `anakin.models.BrowserSession`).
    """
    logger = get_run_logger()
    logger.info("Listing Anakin browser sessions (domain=%s)", domain)

    client = anakin_credentials.get_client()
    sessions = client.sessions.list(domain=domain)

    logger.info("Found %d saved session(s)", len(sessions))
    return [session.model_dump() for session in sessions]


@task(name="anakin_session_delete", retries=1, retry_delay_seconds=5)
def session_delete(
    session_id: str,
    anakin_credentials: AnakinCredentials,
) -> None:
    """Permanently delete a saved browser session and its encrypted login data.

    Irreversible — the user must log in again through the dashboard to
    recreate it, and anything referencing this `session_id` (scrapes,
    crawls, monitors, browser tasks) loses authenticated access. Calls
    `anakin-sdk`'s `Anakin.sessions.delete()` (`DELETE /sessions/:id`)
    directly. Find ids with `session_list`.

    Args:
        session_id: The session ID to delete (from `session_list`).
        anakin_credentials: Block holding the Anakin API key.
    """
    logger = get_run_logger()
    logger.info("Deleting Anakin browser session %s", session_id)

    client = anakin_credentials.get_client()
    client.sessions.delete(session_id)

    logger.info("Deleted browser session %s", session_id)


# ─── Wire: discovery, identities, login, build (raw HTTP) ─────────────────


@task(name="anakin_wire_discover", retries=2, retry_delay_seconds=5)
def wire_discover(
    q: str,
    anakin_credentials: AnakinCredentials,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Find Wire actions for a task from a natural-language intent.

    Not covered by `anakin-sdk` v0.1.0 (confirmed against
    `anakin-py/src/anakin/client.py` — only `Anakin.wire()` for *running* an
    action exists there; discovery has no SDK wrapper yet), so this task
    calls `GET /wire/resolve` directly over `httpx`
    (`AnakinCredentials.get_http_client()`), matching the real endpoint in
    `anakin-mcp/src/client.ts::wireResolve`. Synchronous — no polling.

    Args:
        q: The intent in natural language, e.g. "top phones on walmart",
            "search airbnb listings in Lisbon".
        anakin_credentials: Block holding the Anakin API key.
        limit: Maximum number of candidate actions to return.

    Returns:
        Ranked candidate actions, each with `action_id`, `type` ("read" or
        "write"), required/optional params, credit cost, and whether auth is
        needed. Feed a returned `action_id` into `wire_read_action` or
        `wire_write_action`.
    """
    logger = get_run_logger()
    logger.info("Discovering Wire actions for: %s", q)

    params: dict[str, Any] = {"q": q}
    if limit is not None:
        params["limit"] = limit

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", "/wire/resolve", params=params)

    logger.info("Wire discovery for %r returned %d result(s)", q, len(result.get("results", []) or []))
    return result


@task(name="anakin_wire_catalog", retries=2, retry_delay_seconds=5)
def wire_catalog(
    anakin_credentials: AnakinCredentials,
    *,
    slug: str | None = None,
) -> dict[str, Any]:
    """Browse the Wire catalog: every supported site, or one site's full
    action list.

    Not covered by `anakin-sdk` v0.1.0 (see `wire_discover`'s docstring) —
    calls `GET /wire/catalog` (or `GET /wire/catalog/:slug`) directly over
    `httpx`, matching `anakin-mcp/src/client.ts::wireCatalog`. Synchronous.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        slug: Catalog slug to inspect, e.g. "walmart", "amazon", "linkedin".
            Omit to list all catalogs.

    Returns:
        With no `slug`: every catalog and its action count. With `slug`:
        that site's full action list — each action's parameter schema,
        type (read/write), auth_mode (none/optional/required), and credit
        cost — plus login fields for credentials-mode sites.
    """
    logger = get_run_logger()
    logger.info("Fetching Wire catalog%s", f" for {slug}" if slug else "")

    path = f"/wire/catalog/{slug}" if slug else "/wire/catalog"
    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", path)

    logger.info("Wire catalog fetch (%s) succeeded", path)
    return result


@task(name="anakin_wire_identities", retries=2, retry_delay_seconds=5)
def wire_identities(
    anakin_credentials: AnakinCredentials,
    *,
    catalog_id: str | None = None,
) -> dict[str, Any]:
    """List your saved Wire identities and their credentials.

    Not covered by `anakin-sdk` v0.1.0 — calls `GET /wire/identities`
    directly over `httpx`, matching
    `anakin-mcp/src/client.ts::wireIdentities`. Synchronous.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        catalog_id: Optional — restrict to identities for a single catalog.

    Returns:
        Your saved identities, each with its credential(s) and each
        credential's `id`. Note: passing a `credential_id` to
        `wire_read_action`/`wire_write_action` isn't possible with
        `anakin-sdk` v0.1.0 today — see those tasks' docstrings.
    """
    logger = get_run_logger()
    logger.info("Listing Wire identities%s", f" (catalog_id={catalog_id})" if catalog_id else "")

    params = {"catalog_id": catalog_id} if catalog_id else None
    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", "/wire/identities", params=params)

    return result


@task(name="anakin_wire_login", retries=1, retry_delay_seconds=10)
def wire_login(
    catalog_slug: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
    identity_name: str | None = None,
    source_id: str | None = None,
    source_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Sign in to a credentials-mode Wire site and get a `credential_id`.

    Not covered by `anakin-sdk` v0.1.0 — calls `POST /wire/login` directly
    over `httpx`, matching `anakin-mcp/src/client.ts::wireLogin`. The
    password is never stored by Anakin, only the encrypted session.

    Args:
        catalog_slug: The catalog to sign in to, e.g. "neb".
        anakin_credentials: Block holding the Anakin API key.
        params: Login fields defined by the catalog (e.g.
            `{"email": ..., "password": ...}`) — see `wire_catalog`'s
            `login_input_schema` for field names. Prefect logs task inputs
            by default; treat this as a secret at the call site.
        identity_name: Optional name for the identity. Derived from `params`
            in password mode; required when using a 1Password locator.
        source_id: Optional 1Password identity-source ID (alternative to `params`).
        source_ref: Optional 1Password item locator (`{vault_id, item_id,
            fields}`, used with `source_id` instead of `params`).

    Returns:
        The new identity/credential, including its `id`. Note:
        `wire_read_action`/`wire_write_action` in this package cannot yet
        accept a `credential_id` — see their docstrings (`anakin-sdk`
        v0.1.0's `Anakin.wire()` has no such parameter).
    """
    logger = get_run_logger()
    logger.info("Signing in to Wire catalog %s", catalog_slug)  # never log params — may hold secrets

    body: dict[str, Any] = {"catalog_slug": catalog_slug}
    if params is not None:
        body["params"] = params
    if identity_name is not None:
        body["identity_name"] = identity_name
    if source_id is not None:
        body["source_id"] = source_id
    if source_ref is not None:
        body["source_ref"] = source_ref

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "POST", "/wire/login", json=body)

    logger.info("Wire login for %s completed", catalog_slug)
    return result


@task(name="anakin_wire_build", retries=1, retry_delay_seconds=10)
def wire_build(
    website_url: str,
    goal: str,
    anakin_credentials: AnakinCredentials,
    *,
    catalog_id: str | None = None,
    visibility: str = "private",
    force: bool = False,
) -> dict[str, Any]:
    """Request a brand-new Wire action for a site not yet in the catalog.

    Not covered by `anakin-sdk` v0.1.0 — calls `POST /wire/build-request`
    directly over `httpx`, matching `anakin-mcp/src/client.ts::wireBuild`.
    Asynchronous server-side (returns status "pending"); this task does not
    poll it to completion — re-check via `wire_catalog`/`wire_discover`
    later to see whether the new action published. Charges credits,
    refunded automatically if the build fails. Only use this after
    `wire_discover`/`wire_catalog` confirm no existing action covers the site.

    Refuses to run (raises `ValueError`, without calling the API) any
    request whose `goal`/`website_url` looks like a payment or fund-transfer
    request — same policy `anakin-mcp`'s `wire_build` tool enforces
    (`anakin-mcp/src/tools/policy.ts::financialBlockReason`), ported to
    Python in `prefect_anakin._policy`.

    Args:
        website_url: The site to build an action for. The domain is
            extracted automatically.
        goal: Natural-language description of what the action should do or
            extract. Be specific — the builder synthesizes the scraper from this.
        anakin_credentials: Block holding the Anakin API key.
        catalog_id: Optional — attach to an existing catalog instead of
            creating one.
        visibility: Action visibility, "private" (default) or "public".
        force: Build even if similar actions already exist for the domain
            (otherwise the request is rejected with `ACTION_EXISTS`).

    Returns:
        The build request's status/id — re-check via `wire_catalog` once it
        publishes.

    Raises:
        ValueError: If `goal`/`website_url` look like a payment or
            fund-transfer request.
    """
    logger = get_run_logger()

    blocked = financial_block_reason(f"{goal} {website_url}")
    if blocked:
        logger.error("Refusing Wire build for %s: %s", website_url, blocked)
        raise ValueError(blocked)

    logger.info("Requesting a new Wire action for %s: %s", website_url, goal)

    body: dict[str, Any] = {
        "website_url": website_url,
        "goal": goal,
        "visibility": visibility,
        "force": force,
    }
    if catalog_id is not None:
        body["catalog_id"] = catalog_id

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "POST", "/wire/build-request", json=body)

    logger.info("Wire build request for %s submitted", website_url)
    return result


# ─── Website Monitoring (raw HTTP) ─────────────────────────────────────────


_MONITOR_ACTIONS = {"pause", "resume", "run_now", "delete"}


@task(name="anakin_monitor_create", retries=1, retry_delay_seconds=10)
def monitor_create(
    url: str,
    interval_minutes: int,
    anakin_credentials: AnakinCredentials,
    *,
    scope: str = "page",
    watch_mode: str = "full_page",
    watch_format: str = "markdown",
    output_schema: dict[str, Any] | None = None,
    ai_mode: bool = False,
    ai_goal: str | None = None,
    use_browser: bool = False,
    country: str = "us",
    session_id: str | None = None,
    is_active: bool = True,
    expires_at: str | None = None,
    alert_webhook_url: str | None = None,
    alert_emails: str | None = None,
    max_pages: int | None = None,
    max_depth: int | None = None,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    wire_action_id: str | None = None,
    wire_catalog_slug: str | None = None,
    wire_credential_id: str | None = None,
    wire_params: dict[str, Any] | None = None,
    wire_watch_paths: list[str] | None = None,
) -> dict[str, Any]:
    """Create a scheduled website monitor.

    Checks `url` every `interval_minutes` (minimum 15) and records a change
    when the content differs — optionally alerting a webhook or email.
    `scope` "page" (default) watches one URL; "site" crawls the site each
    run and tracks pages added/removed/changed; "wire" runs a Wire action
    each check and diffs its JSON. `watch_mode` "full_page" (2 credits/check)
    compares the whole page; "specific_data" (3 credits/check) extracts only
    the fields in `output_schema` with AI — ideal for price/stock/status
    tracking. `ai_mode` (+1 credit/check) filters out trivial noise and
    summarizes real changes. Active-monitor caps per plan: Free 5, Pro 20,
    Scale 100.

    Not covered by `anakin-sdk` v0.1.0 — calls `POST /monitors` directly
    over `httpx`, matching `anakin-mcp/src/client.ts::monitorCreate`. The
    response's `alertWebhookSecret` is redacted before it reaches the flow
    (see `prefect_anakin._http.redact_secrets`) — retrieve it from the
    Anakin dashboard.

    Args:
        url: The URL to watch (root URL for site scope; the Wire site's URL
            for wire scope).
        interval_minutes: Check frequency in minutes. Minimum 15.
        anakin_credentials: Block holding the Anakin API key.
        scope: "page" (default), "site", or "wire".
        watch_mode: "full_page" (default) or "specific_data".
        watch_format: Format compared in full_page mode — "markdown"
            (default), "html", or "cleaned_html".
        output_schema: JSON Schema of the fields to track. Required when
            `watch_mode` is "specific_data".
        ai_mode: AI meaningful-change filtering.
        ai_goal: Natural-language description of which changes count as
            meaningful (used with `ai_mode`).
        use_browser: Render checks with a stealth headless browser. Forced
            true server-side when `session_id` is set.
        country: Two-letter proxy country code. Default "us".
        session_id: Saved browser-session ID for monitoring login-protected
            pages (see `session_list`).
        is_active: Start running immediately. Default True.
        expires_at: Optional end date (ISO 8601 timestamp or `YYYY-MM-DD`);
            the monitor auto-pauses when it passes.
        alert_webhook_url: Webhook URL that receives signed change alerts.
        alert_emails: Comma-separated email recipients for change alerts.
        max_pages: Site scope: max pages crawled per run.
        max_depth: Site scope: crawl depth (1-5). Defaults to 2.
        include_patterns: Site scope: glob patterns or hand-picked
            same-site URLs to track.
        exclude_patterns: Site scope: glob patterns to skip.
        wire_action_id: Wire scope (required there): the Wire action run
            each check, e.g. "amazon.search_products".
        wire_catalog_slug: Wire scope: catalog slug of the Wire site.
        wire_credential_id: Wire scope: credential ID when the action needs auth.
        wire_params: Wire scope: parameters passed to the action each check.
        wire_watch_paths: Wire scope: JSON paths to diff instead of the
            whole response.

    Returns:
        The created monitor's configuration and status, with
        `alertWebhookSecret` redacted.
    """
    logger = get_run_logger()
    logger.info("Creating Anakin monitor for %s (scope=%s, interval=%dm)", url, scope, interval_minutes)

    body: dict[str, Any] = {
        "url": url,
        "intervalMinutes": interval_minutes,
        "scope": scope,
        "watchMode": watch_mode,
        "watchFormat": watch_format,
        "aiMode": ai_mode,
        "useBrowser": use_browser,
        "country": country,
        "isActive": is_active,
    }
    optional: dict[str, Any] = {
        "outputSchema": output_schema,
        "aiGoal": ai_goal,
        "sessionId": session_id,
        "expiresAt": expires_at,
        "alertWebhookUrl": alert_webhook_url,
        "alertEmails": alert_emails,
        "maxPages": max_pages,
        "maxDepth": max_depth,
        "includePatterns": include_patterns,
        "excludePatterns": exclude_patterns,
        "wireActionId": wire_action_id,
        "wireCatalogSlug": wire_catalog_slug,
        "wireCredentialId": wire_credential_id,
        "wireParams": wire_params,
        "wireWatchPaths": wire_watch_paths,
    }
    for key, value in optional.items():
        if value is not None:
            body[key] = value

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "POST", "/monitors", json=body)

    logger.info("Created monitor %s for %s", result.get("id", "?"), url)
    return anakin_http.redact_secrets(result)


@task(name="anakin_monitor_list", retries=2, retry_delay_seconds=5)
def monitor_list(
    anakin_credentials: AnakinCredentials,
    *,
    monitor_id: str | None = None,
) -> dict[str, Any]:
    """List your website monitors, or fetch one monitor's full configuration.

    Not covered by `anakin-sdk` v0.1.0 — calls `GET /monitors` (or `GET
    /monitors/:id` when `monitor_id` is set) directly over `httpx`, matching
    `anakin-mcp/src/client.ts::monitorList`/`monitorGet`. Use this to find a
    monitor's id before `monitor_changes` or `monitor_control`.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        monitor_id: Fetch just this monitor (its full configuration and
            status — next/last check time, active state, per-check credit
            cost, alert settings) instead of the full list.

    Returns:
        The monitor list, or a single monitor's configuration, with
        `alertWebhookSecret` redacted.
    """
    logger = get_run_logger()
    logger.info("Listing Anakin monitors%s", f" (id={monitor_id})" if monitor_id else "")

    path = f"/monitors/{monitor_id}" if monitor_id else "/monitors"
    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", path)

    return anakin_http.redact_secrets(result)


@task(name="anakin_monitor_changes", retries=2, retry_delay_seconds=5)
def monitor_changes(
    monitor_id: str,
    anakin_credentials: AnakinCredentials,
) -> dict[str, Any]:
    """Get the detected changes for a monitor.

    Each entry records when the watched content differed from the previous
    check, with a diff/summary (and the AI change summary when `ai_mode` is
    on). Not covered by `anakin-sdk` v0.1.0 — calls `GET
    /monitors/:id/changes` directly over `httpx`, matching
    `anakin-mcp/src/client.ts::monitorChanges`. Use `monitor_list` first to
    find the monitor id.

    Args:
        monitor_id: The monitor ID (from `monitor_list` or `monitor_create`).
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        The monitor's change history, with `alertWebhookSecret` redacted.
    """
    logger = get_run_logger()
    logger.info("Fetching changes for monitor %s", monitor_id)

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", f"/monitors/{monitor_id}/changes")

    return anakin_http.redact_secrets(result)


@task(name="anakin_monitor_control", retries=1, retry_delay_seconds=10)
def monitor_control(
    monitor_id: str,
    action: str,
    anakin_credentials: AnakinCredentials,
) -> dict[str, Any]:
    """Control an existing website monitor.

    "pause" stops scheduled checks, "resume" restarts them (may hit the
    plan's active-monitor cap), "run_now" triggers an immediate
    out-of-schedule check (billed like a normal check), and "delete"
    permanently removes the monitor and its history. Not covered by
    `anakin-sdk` v0.1.0 — calls `POST /monitors/:id/{pause,resume,run}` or
    `DELETE /monitors/:id` directly over `httpx`, matching
    `anakin-mcp/src/client.ts::monitorControl`. Use `monitor_list` to find
    the id.

    Args:
        monitor_id: The monitor ID (from `monitor_list` or `monitor_create`).
        action: One of "pause", "resume", "run_now", "delete".
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        The updated (or, for "delete", deletion-confirmation) response,
        with `alertWebhookSecret` redacted.

    Raises:
        ValueError: If `action` isn't one of the four supported verbs.
    """
    logger = get_run_logger()
    if action not in _MONITOR_ACTIONS:
        raise ValueError(f"Unknown monitor action {action!r} — use pause, resume, run_now, or delete.")

    logger.info("Applying action=%s to monitor %s", action, monitor_id)

    base = f"/monitors/{monitor_id}"
    method_by_action = {
        "pause": ("POST", f"{base}/pause"),
        "resume": ("POST", f"{base}/resume"),
        "run_now": ("POST", f"{base}/run"),
        "delete": ("DELETE", base),
    }
    method, path = method_by_action[action]

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, method, path)

    logger.info("Monitor %s: %s applied", monitor_id, action)
    return anakin_http.redact_secrets(result)


# ─── AI Visibility (raw HTTP) ───────────────────────────────────────────────


@task(name="anakin_ai_visibility_sources", retries=2, retry_delay_seconds=5)
def ai_visibility_sources(anakin_credentials: AnakinCredentials) -> dict[str, Any]:
    """List the AI answer engines available to `ai_visibility_search`.

    Not covered by `anakin-sdk` v0.1.0 — calls `GET /ai-visibility/sources`
    directly over `httpx`, matching
    `anakin-mcp/src/client.ts::aiVisibilitySources`.

    Args:
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        Each engine's slug (what to pass in `ai_visibility_search`'s
        `sources`) and display label.
    """
    logger = get_run_logger()
    logger.info("Fetching AI Visibility sources")

    with anakin_credentials.get_http_client() as http_client:
        result = anakin_http.request(http_client, "GET", "/ai-visibility/sources")

    return result


@task(name="anakin_ai_visibility_search", retries=1, retry_delay_seconds=15)
def ai_visibility_search(
    query: str,
    anakin_credentials: AnakinCredentials,
    *,
    sources: list[str] | None = None,
    country: str | None = None,
    include_full_content: bool = False,
) -> dict[str, Any]:
    """Ask multiple AI answer engines (ChatGPT, Gemini, Google AI Overview,
    ...) the same question and compare their answers.

    Submits to `POST /ai-visibility/search` and polls `GET
    /ai-visibility/search/:searchId` until the status leaves `"running"` —
    unlike the job-polling endpoints, a `"failed"` result here is returned,
    not raised: it still carries per-source results and errors. Not covered
    by `anakin-sdk` v0.1.0 — calls the API directly over `httpx`, matching
    `anakin-mcp/src/client.ts::aiVisibilitySearch`. Use for brand/AI-SEO
    visibility checks and geo-specific AI answers (set `country`). Billed
    per source; failed sources are free.

    Args:
        query: The question to ask every engine (max 2000 characters).
        anakin_credentials: Block holding the Anakin API key.
        sources: Engine slugs to query (see `ai_visibility_sources`). Omit
            to query all enabled engines.
        country: Two-letter ISO country code for search geography. Defaults
            to "us" server-side.
        include_full_content: Include each engine's raw full answer in the
            results. False by default — summaries and the synthesis are
            returned regardless.

    Returns:
        `{search_id, status, country, synthesis, results}` — `results` has
        one entry per engine; `full_content` is stripped from each unless
        `include_full_content=True`.

    Raises:
        TimeoutError: If the search doesn't leave "running" within 3 minutes
            of polling.
    """
    logger = get_run_logger()
    logger.info("Running AI Visibility search: %s", query)

    request_body: dict[str, Any] = {"query": query}
    if sources is not None:
        request_body["sources"] = sources
    if country is not None:
        request_body["country"] = country

    with anakin_credentials.get_http_client() as http_client:
        submitted = anakin_http.request(http_client, "POST", "/ai-visibility/search", json=request_body)
        search_id = submitted.get("search_id")
        if not search_id:
            raise RuntimeError(f"Anakin API response did not include a search_id: {submitted!r}")

        search = anakin_http.poll(
            http_client,
            f"/ai-visibility/search/{search_id}",
            is_terminal=lambda poll_body: poll_body.get("status") != "running",
        )

    results = search.get("results") or []
    if not include_full_content:
        results = [{k: v for k, v in r.items() if k != "full_content"} for r in results]

    logger.info("AI Visibility search %s completed with status=%s", search_id, search.get("status"))
    return {
        "search_id": search.get("search_id", search_id),
        "status": search.get("status"),
        "country": search.get("country"),
        "synthesis": search.get("synthesis"),
        "results": results,
    }


# ─── AI browser automation (raw HTTP) ──────────────────────────────────────


@task(name="anakin_browser_task", retries=1, retry_delay_seconds=15)
def browser_task(
    prompt: str,
    anakin_credentials: AnakinCredentials,
    *,
    url: str | None = None,
    session_id: str | None = None,
    max_steps: int | None = None,
    timeout_ms: int | None = None,
    output_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a natural-language task in a real cloud browser driven by an AI agent.

    The agent navigates, clicks, types, scrolls, and extracts on your
    behalf. Use when `scrape_url` can't do the job (multi-step flows,
    interactions, complex navigation) and no Wire action covers the site
    (check `wire_discover` first — Wire actions are faster and cheaper).
    Always submits in async mode (`async: true`) and polls `GET
    /ai/jobs/:workflowId` — the sync variant blocks the HTTP request for up
    to ~5.5 minutes, far past a normal request timeout (server hard-caps a
    run at ~330s). Not covered by `anakin-sdk` v0.1.0 — calls the API
    directly over `httpx`, matching `anakin-mcp/src/client.ts::browserTask`.

    For login-protected tasks pass `session_id` from `session_list` — never
    put passwords in `prompt`. Refuses to run (raises `ValueError`, without
    calling the API) any task whose `prompt`/`url` looks like a payment or
    fund-transfer request — same policy `anakin-mcp`'s `browser_task` tool
    enforces (`anakin-mcp/src/tools/policy.ts::financialBlockReason`), ported
    to Python in `prefect_anakin._policy`.

    Args:
        prompt: The task in natural language. Be specific about the goal and
            what to return. Never include passwords or secrets.
        anakin_credentials: Block holding the Anakin API key.
        url: Navigate here before starting. Omit to let the agent follow
            URLs named in the prompt.
        session_id: Saved browser-session ID (from `session_list`) so the
            task runs logged in.
        max_steps: Cap on agent steps (navigation/click/type actions).
        timeout_ms: Task timeout in milliseconds (server caps runs at ~330s
            regardless).
        output_schema: JSON Schema for the result — the agent returns
            structured data conforming to it.

    Returns:
        `{success, result, steps_taken, iterations, cached, duration_ms, run_id}`.

    Raises:
        ValueError: If `prompt`/`url` look like a payment or fund-transfer request.
        RuntimeError: If the task fails or times out server-side.
    """
    logger = get_run_logger()

    blocked = financial_block_reason(f"{prompt} {url or ''}")
    if blocked:
        logger.error("Refusing browser task: %s", blocked)
        raise ValueError(blocked)

    logger.info("Running Anakin browser task: %s", prompt)

    request_body: dict[str, Any] = {"prompt": prompt, "async": True}
    if url is not None:
        request_body["url"] = url
    if session_id is not None:
        request_body["session_id"] = session_id
    if max_steps is not None:
        request_body["max_steps"] = max_steps
    if timeout_ms is not None:
        request_body["timeout_ms"] = timeout_ms
    if output_schema is not None:
        request_body["output_schema"] = output_schema

    with anakin_credentials.get_http_client() as http_client:
        accepted = anakin_http.request(http_client, "POST", "/ai/evaluate", json=request_body)
        workflow_id = accepted.get("workflow_id")

        if not workflow_id:
            # Service answered synchronously (shouldn't happen with async=True).
            resolved = accepted
        else:
            job = anakin_http.poll(
                http_client,
                f"/ai/jobs/{workflow_id}",
                is_terminal=lambda poll_body: poll_body.get("status") in {"completed", "failed", "timed_out"},
                max_attempts=anakin_http.BROWSER_TASK_POLL_MAX_ATTEMPTS,
            )
            if job.get("status") in {"failed", "timed_out"}:
                raise RuntimeError(f"Browser task {job.get('status')}: {job.get('error', 'unknown')}")
            resolved = job.get("result") or job

    logger.info("Browser task completed (run_id=%s)", resolved.get("run_id"))
    return {
        "success": resolved.get("success"),
        "result": resolved.get("result"),
        "steps_taken": len(resolved["steps"]) if isinstance(resolved.get("steps"), list) else None,
        "iterations": resolved.get("iterations"),
        "cached": resolved.get("cached"),
        "duration_ms": resolved.get("duration_ms"),
        "run_id": resolved.get("run_id"),
    }
