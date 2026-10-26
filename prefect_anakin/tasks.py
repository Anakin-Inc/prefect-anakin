"""Prefect tasks for the Anakin web data API.

Every task takes an `AnakinCredentials` block as an explicit argument and
returns plain JSON-serializable data (dicts and lists), so results can be
persisted, cached and passed between tasks.

All tasks except `browser_task` delegate to the official `anakin-sdk` client
(`AnakinCredentials.get_client()`), which handles request bodies, retries on
429/5xx and job polling. `browser_task` calls `POST /ai/evaluate` directly
because the SDK doesn't wrap that endpoint yet.

- Scrape and search: `scrape_url`, `map`, `crawl`, `search`, `agentic_search`
- Wire: `wire_discover`, `wire_catalog`, `wire_read_action`,
  `wire_write_action`, `wire_identities`, `wire_login`, `wire_build`,
  `wire_build_status`
- Website monitoring: `monitor_create`, `monitor_list`, `monitor_changes`,
  `monitor_control`
- AI visibility: `ai_visibility_search`, `ai_visibility_sources`
- Browser: `session_list`, `session_delete`, `browser_task`
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from prefect import task
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger
from pydantic import BaseModel

from prefect_anakin import _http as anakin_http
from prefect_anakin._policy import financial_block_reason
from prefect_anakin.credentials import AnakinCredentials

_fallback_logger = logging.getLogger("prefect_anakin")


def _logger() -> Any:
    """Prefect's run logger inside a flow or task run, a module logger outside one.

    The fallback lets `task.fn(...)` be called directly (scripts, unit tests).
    """
    try:
        return get_run_logger()
    except MissingContextError:
        return _fallback_logger


def _dump(value: BaseModel) -> dict[str, Any]:
    return value.model_dump(mode="json")


def _dump_all(values: Sequence[BaseModel]) -> list[dict[str, Any]]:
    return [_dump(v) for v in values]


def _refuse_if_financial(text: str, what: str) -> None:
    blocked = financial_block_reason(text)
    if blocked:
        _logger().error("Refusing %s: %s", what, blocked)
        raise ValueError(blocked)


# ─── Scrape and search ──────────────────────────────────────────────────────


@task(name="anakin_scrape_url", retries=2, retry_delay_seconds=5)
def scrape_url(
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    country: str = "us",
    use_browser: bool = False,
    generate_json: bool = False,
    output_schema: dict[str, Any] | None = None,
    formats: list[str] | None = None,
    force_fresh: bool = False,
    session_id: str | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    """Scrape one URL to markdown, optionally extracting structured JSON with AI.

    Works without an API key on a small free per-IP allowance.

    Args:
        url: The URL to scrape.
        anakin_credentials: Block holding the Anakin API key.
        country: Proxy country code, e.g. "us", "gb", "de".
        use_browser: Render with a headless browser. Use for JavaScript-heavy pages.
        generate_json: Extract structured JSON from the page with AI.
        output_schema: JSON Schema for the extracted JSON (implies AI extraction).
        formats: Output formats, any of "markdown" (default), "html",
            "cleaned_html", "screenshot", "links".
        force_fresh: Skip the cache and fetch the page again.
        session_id: Saved browser session to scrape as a logged-in user.
        session_name: Saved browser session name (alternative to `session_id`).

    Returns:
        The scraped document: `id`, `url`, `status`, `markdown`,
        `generated_json` and the other fields of `anakin.models.Document`.
    """
    logger = _logger()
    logger.info("Scraping %s (use_browser=%s)", url, use_browser)

    document = anakin_credentials.get_client().scrape(
        url,
        formats=tuple(formats or ("markdown",)),  # type: ignore[arg-type]
        country=country,
        use_browser=use_browser,
        generate_json=generate_json,
        output_schema=output_schema,
        force_fresh=force_fresh,
        session_id=session_id,
        session_name=session_name,
    )

    logger.info("Scraped %s (job %s, cached=%s)", url, document.id, document.cached)
    return _dump(document)


@task(name="anakin_map", retries=2, retry_delay_seconds=5)
def map(
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    limit: int = 100,
    depth: int = 2,
    limit_per_level: int = 100,
    include_subdomains: bool = False,
    include_external_links: bool = False,
    search: str | None = None,
    use_browser: bool = False,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Discover the URLs on a site (sitemap plus link traversal).

    Useful before `crawl`, or to find which pages to `scrape_url`.

    Args:
        url: Starting URL, usually a homepage or section root.
        anakin_credentials: Block holding the Anakin API key.
        limit: Maximum URLs to return (max 5000).
        depth: Link hops to follow from `url` (max 5).
        limit_per_level: Maximum URLs collected per depth level.
        include_subdomains: Include URLs on subdomains of the starting host.
        include_external_links: Also collect (but don't follow) external links.
        search: Only return URLs whose path or title matches this keyword.
        use_browser: Render with a headless browser. Needed for SPAs.
        session_id: Saved browser session to map as a logged-in user.

    Returns:
        `id`, `url`, `links`, `total_links`, `external_links`,
        `total_external_links`, `duration_ms`.
    """
    logger = _logger()
    logger.info("Mapping %s (depth=%d, limit=%d)", url, depth, limit)

    result = anakin_credentials.get_client().map(
        url,
        limit=limit,
        depth=depth,
        limit_per_level=limit_per_level,
        include_subdomains=include_subdomains,
        include_external_links=include_external_links,
        search=search,
        use_browser=use_browser,
        session_id=session_id,
    )

    logger.info("Map of %s found %d link(s)", url, result.total_links)
    return _dump(result)


@task(name="anakin_crawl", retries=1, retry_delay_seconds=10)
def crawl(
    url: str,
    anakin_credentials: AnakinCredentials,
    *,
    max_pages: int = 10,
    depth: int = 1,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
    country: str = "us",
    use_browser: bool = False,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Fetch markdown for many pages of a site in one job.

    Args:
        url: Starting URL.
        anakin_credentials: Block holding the Anakin API key.
        max_pages: Maximum pages fetched.
        depth: Link hops to follow from `url`.
        include_patterns: Only fetch URLs matching at least one of these patterns.
        exclude_patterns: Skip URLs matching any of these patterns.
        country: Proxy country code.
        use_browser: Render each page with a headless browser.
        session_id: Saved browser session to crawl as a logged-in user.

    Returns:
        `id`, `url`, `total_pages`, `completed_pages` and `pages`, each with
        `url`, `status`, `markdown` and `error`.
    """
    logger = _logger()
    logger.info("Crawling %s (max_pages=%d, depth=%d)", url, max_pages, depth)

    result = anakin_credentials.get_client().crawl(
        url,
        max_pages=max_pages,
        depth=depth,
        include_patterns=include_patterns or (),
        exclude_patterns=exclude_patterns or (),
        country=country,
        use_browser=use_browser,
        session_id=session_id,
    )

    logger.info(
        "Crawl of %s fetched %d/%d page(s)", url, result.completed_pages, result.total_pages
    )
    return _dump(result)


@task(name="anakin_search", retries=2, retry_delay_seconds=5)
def search(
    prompt: str,
    anakin_credentials: AnakinCredentials,
    *,
    limit: int = 5,
) -> dict[str, Any]:
    """AI web search with citations. Synchronous.

    Args:
        prompt: The search query or question.
        anakin_credentials: Block holding the Anakin API key.
        limit: Maximum results (max 20).

    Returns:
        `id` and `results`, each with `url`, `title`, `snippet` and `date`.
    """
    logger = _logger()
    logger.info("Searching: %s (limit=%d)", prompt, limit)

    result = anakin_credentials.get_client().search(prompt, limit=limit)

    logger.info("Search returned %d result(s)", len(result.results))
    return _dump(result)


@task(name="anakin_agentic_search", retries=1, retry_delay_seconds=15)
def agentic_search(
    prompt: str,
    anakin_credentials: AnakinCredentials,
    *,
    use_browser: bool = True,
    schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Multi-step research: search, scrape the cited pages, return a structured answer.

    Takes minutes rather than seconds. Use `search` for quick lookups.

    Args:
        prompt: The research question (max 8 KB).
        anakin_credentials: Block holding the Anakin API key.
        use_browser: Scrape citations with a headless browser.
        schema: Optional JSON Schema (max 50 KB) for `structured_data`.

    Returns:
        `id`, `status` and `generated_json` with `summary`,
        `structured_data` and `data_schema`.
    """
    logger = _logger()
    logger.info("Running agentic search: %s", prompt)

    result = anakin_credentials.get_client().agentic_search(
        prompt, use_browser=use_browser, schema=schema
    )

    logger.info("Agentic search %s finished with status=%s", result.id, result.status)
    return _dump(result)


# ─── Wire ───────────────────────────────────────────────────────────────────


@task(name="anakin_wire_discover", retries=2, retry_delay_seconds=5)
def wire_discover(
    q: str,
    anakin_credentials: AnakinCredentials,
    *,
    catalog: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Find Wire actions for a task described in plain language.

    Works without an API key.

    Args:
        q: What you want to do, e.g. "top phones on walmart".
        anakin_credentials: Block holding the Anakin API key.
        catalog: Restrict to one site's catalog slug, e.g. "amazon".
        limit: Maximum candidate actions to return.

    Returns:
        Ranked actions, each with `action_id`, `catalog_slug`, `params` and
        `credits` (plus `mode` and auth details when called with an API
        key). Pass an `action_id` to
        `wire_read_action` or `wire_write_action`.
    """
    logger = _logger()
    logger.info("Discovering Wire actions for: %s", q)

    matches = anakin_credentials.get_client().wire.discover(q, catalog=catalog, limit=limit)

    logger.info("Wire discovery returned %d action(s)", len(matches))
    return _dump_all(matches)


@task(name="anakin_wire_catalog", retries=2, retry_delay_seconds=5)
def wire_catalog(
    anakin_credentials: AnakinCredentials,
    *,
    slug: str | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Browse the Wire catalog: every supported site, or one site's actions.

    Works without an API key.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        slug: Site to inspect, e.g. "walmart". Omit to list every site.

    Returns:
        Without `slug`, a list of sites with their action counts. With
        `slug`, that site's actions (parameter schema, mode, auth mode,
        credit cost) and its login fields if it needs credentials.
    """
    logger = _logger()
    logger.info("Fetching Wire catalog%s", f" for {slug}" if slug else "")

    wire = anakin_credentials.get_client().wire
    if slug:
        return _dump(wire.catalog(slug))
    return _dump_all(wire.catalogs())


@task(name="anakin_wire_read_action", retries=2, retry_delay_seconds=5)
def wire_read_action(
    action_id: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
    credential_id: str | None = None,
    identity_id: str | None = None,
) -> dict[str, Any]:
    """Run a Wire read action, which extracts data without changing anything on the site.

    For actions that change state, use `wire_write_action`.

    Args:
        action_id: The action to run, from `wire_discover` or `wire_catalog`.
        anakin_credentials: Block holding the Anakin API key.
        params: The action's parameters, as described by discovery.
        credential_id: Needed when the action's `auth_mode` is "required".
            Get one from `wire_identities` or `wire_login`.
        identity_id: Alternative to `credential_id`: run as this saved identity.

    Returns:
        `job_id`, `status`, `data`, `credits_used`, `execution_ms` and `error`.
    """
    logger = _logger()
    logger.info("Running Wire read action %s", action_id)

    result = anakin_credentials.get_client().wire.run(
        action_id, params or {}, credential_id=credential_id, identity_id=identity_id
    )

    logger.info("Wire action %s finished with status=%s", action_id, result.status)
    return _dump(result)


@task(name="anakin_wire_write_action", retries=0)
def wire_write_action(
    action_id: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
    credential_id: str | None = None,
    identity_id: str | None = None,
) -> dict[str, Any]:
    """Run a Wire write action: submit a form, add to cart, post content, update settings.

    Not retried automatically, since repeating a write can duplicate it.
    Refuses (raises `ValueError` before calling the API) anything that looks
    like a payment or fund transfer.

    Args:
        action_id: The action to run, from `wire_discover` or `wire_catalog`.
        anakin_credentials: Block holding the Anakin API key.
        params: The action's parameters, as described by discovery.
        credential_id: Needed when the action's `auth_mode` is "required".
        identity_id: Alternative to `credential_id`: run as this saved identity.

    Returns:
        Same shape as `wire_read_action`.
    """
    resolved_params = params or {}
    _refuse_if_financial(f"{action_id} {resolved_params}", f"Wire action {action_id}")

    logger = _logger()
    logger.info("Running Wire write action %s", action_id)

    result = anakin_credentials.get_client().wire.run(
        action_id, resolved_params, credential_id=credential_id, identity_id=identity_id
    )

    logger.info("Wire action %s finished with status=%s", action_id, result.status)
    return _dump(result)


@task(name="anakin_wire_identities", retries=2, retry_delay_seconds=5)
def wire_identities(
    anakin_credentials: AnakinCredentials,
    *,
    catalog_id: str | None = None,
) -> list[dict[str, Any]]:
    """List your saved Wire identities (site accounts) and their credentials.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        catalog_id: Only identities for this catalog.

    Returns:
        Identities, each with `credentials`; every credential's `id` can be
        passed as `credential_id` to the Wire action tasks.
    """
    _logger().info("Listing Wire identities")
    identities = anakin_credentials.get_client().wire.identities(catalog_id=catalog_id)
    return _dump_all(identities)


@task(name="anakin_wire_login", retries=0)
def wire_login(
    catalog_slug: str,
    anakin_credentials: AnakinCredentials,
    *,
    params: dict[str, Any] | None = None,
    identity_name: str | None = None,
    source_id: str | None = None,
    source_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Sign in to a site that needs credentials and get a `credential_id`.

    Anakin stores the encrypted session, never the password. Prefect records
    task inputs, so don't pass a password literal from flow code: load it
    from a Prefect `Secret` block inside the flow instead.

    Args:
        catalog_slug: The site to sign in to.
        anakin_credentials: Block holding the Anakin API key.
        params: The login fields listed in `wire_catalog(slug=...)`'s
            `login_input_schema`, e.g. `{"email": ..., "password": ...}`.
        identity_name: Name for the identity.
        source_id: 1Password identity source, instead of `params`.
        source_ref: 1Password item locator (`vault_id`, `item_id`, `fields`),
            used with `source_id`.

    Returns:
        The login result, including the new `credential_id`.
    """
    logger = _logger()
    logger.info("Signing in to Wire catalog %s", catalog_slug)  # never log params

    result = anakin_credentials.get_client().wire.login(
        catalog_slug,
        params,
        identity_name=identity_name,
        source_id=source_id,
        source_ref=source_ref,
    )
    return _dump(result)


@task(name="anakin_wire_build", retries=0)
def wire_build(
    website_url: str,
    goal: str,
    anakin_credentials: AnakinCredentials,
    *,
    catalog_id: str | None = None,
    visibility: str = "private",
    force: bool = False,
) -> dict[str, Any]:
    """Ask Anakin to build a new Wire action for a site that isn't in the catalog.

    Builds run asynchronously: this returns the pending build request. Check
    on it with `wire_build_status`. Charges credits up front, refunded if the
    build fails. Run `wire_discover` first to make sure no action exists.
    Refuses payment or fund-transfer goals (raises `ValueError`).

    Args:
        website_url: The site to build an action for.
        goal: What the action should do or extract. Be specific.
        anakin_credentials: Block holding the Anakin API key.
        catalog_id: Attach to an existing catalog instead of creating one.
        visibility: "private" (default) or "public".
        force: Build even if similar actions already exist for the domain.

    Returns:
        The build request: `id`, `status` and related fields.
    """
    _refuse_if_financial(f"{goal} {website_url}", f"Wire build for {website_url}")

    logger = _logger()
    logger.info("Requesting a Wire action for %s: %s", website_url, goal)

    build = anakin_credentials.get_client().wire.build(
        website_url, goal, catalog_id=catalog_id, visibility=visibility, force=force
    )

    logger.info("Wire build %s is %s", build.id, build.status)
    return _dump(build)


@task(name="anakin_wire_build_status", retries=2, retry_delay_seconds=5)
def wire_build_status(build_id: str, anakin_credentials: AnakinCredentials) -> dict[str, Any]:
    """Check a Wire build request started with `wire_build`.

    Args:
        build_id: The `id` returned by `wire_build`.
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        The build's `status` and, once published, its `action_id`.
    """
    return _dump(anakin_credentials.get_client().wire.get_build(build_id))


# ─── Website monitoring ─────────────────────────────────────────────────────


_MONITOR_ACTIONS = ("pause", "resume", "run_now", "delete")


@task(name="anakin_monitor_create", retries=0)
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
    when the content differs, optionally alerting a webhook or email.
    `scope="page"` watches one URL, `"site"` crawls the site and tracks pages
    added, removed and changed, and `"wire"` runs a Wire action each check
    and diffs its JSON. `watch_mode="specific_data"` tracks only the fields
    in `output_schema` (prices, stock, status). `ai_mode` filters out trivial
    changes and summarizes real ones. See https://anakin.io/pricing for the
    credit cost per check and the active-monitor limit on each plan.

    Args:
        url: The URL to watch (the root URL for site scope).
        interval_minutes: Minutes between checks. Minimum 15.
        anakin_credentials: Block holding the Anakin API key.
        scope: "page", "site" or "wire".
        watch_mode: "full_page" or "specific_data".
        watch_format: Compared format in full_page mode: "markdown",
            "html" or "cleaned_html".
        output_schema: JSON Schema of the fields to track. Required for
            `watch_mode="specific_data"`.
        ai_mode: Filter out trivial changes with AI.
        ai_goal: Which changes count as meaningful (used with `ai_mode`).
        use_browser: Check with a headless browser.
        country: Proxy country code.
        session_id: Saved browser session for login-protected pages.
        is_active: Start checking immediately.
        expires_at: End date (ISO 8601 or `YYYY-MM-DD`); the monitor pauses then.
        alert_webhook_url: Webhook that receives signed change alerts.
        alert_emails: Comma-separated email recipients for change alerts.
        max_pages: Site scope: maximum pages per run.
        max_depth: Site scope: crawl depth (1-5).
        include_patterns: Site scope: patterns or URLs to track.
        exclude_patterns: Site scope: patterns to skip.
        wire_action_id: Wire scope (required there): the action to run each check.
        wire_catalog_slug: Wire scope: the action's catalog slug.
        wire_credential_id: Wire scope: credential for actions that need auth.
        wire_params: Wire scope: parameters for the action.
        wire_watch_paths: Wire scope: JSON paths to diff instead of the whole result.

    Returns:
        The new monitor's configuration and status. The webhook signing
        secret is redacted; read it in the Anakin dashboard.
    """
    logger = _logger()
    logger.info("Creating monitor for %s (scope=%s, every %dm)", url, scope, interval_minutes)

    options: dict[str, Any] = {
        "scope": scope,
        "watch_mode": watch_mode,
        "watch_format": watch_format,
        "ai_mode": ai_mode,
        "use_browser": use_browser,
        "country": country,
        "is_active": is_active,
    }
    optional: dict[str, Any] = {
        "output_schema": output_schema,
        "ai_goal": ai_goal,
        "session_id": session_id,
        "expires_at": expires_at,
        "alert_webhook_url": alert_webhook_url,
        "alert_emails": alert_emails,
        "max_pages": max_pages,
        "max_depth": max_depth,
        "include_patterns": include_patterns,
        "exclude_patterns": exclude_patterns,
        "wire_action_id": wire_action_id,
        "wire_catalog_slug": wire_catalog_slug,
        "wire_credential_id": wire_credential_id,
        "wire_params": wire_params,
        "wire_watch_paths": wire_watch_paths,
    }
    options.update({k: v for k, v in optional.items() if v is not None})

    monitor = anakin_credentials.get_client().monitors.create(url, interval_minutes, **options)

    logger.info("Created monitor %s for %s", monitor.id, url)
    return anakin_http.redact_secrets(_dump(monitor))  # type: ignore[no-any-return]


@task(name="anakin_monitor_list", retries=2, retry_delay_seconds=5)
def monitor_list(
    anakin_credentials: AnakinCredentials,
    *,
    monitor_id: str | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """List your website monitors, or fetch one monitor's configuration.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        monitor_id: Return only this monitor.

    Returns:
        A list of monitors, or one monitor when `monitor_id` is set, with
        webhook signing secrets redacted.
    """
    monitors = anakin_credentials.get_client().monitors
    if monitor_id:
        return anakin_http.redact_secrets(_dump(monitors.get(monitor_id)))  # type: ignore[no-any-return]
    return anakin_http.redact_secrets(_dump_all(monitors.list()))  # type: ignore[no-any-return]


@task(name="anakin_monitor_changes", retries=2, retry_delay_seconds=5)
def monitor_changes(monitor_id: str, anakin_credentials: AnakinCredentials) -> list[dict[str, Any]]:
    """Get the changes a monitor has detected, newest first.

    Args:
        monitor_id: The monitor's `id`.
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        One entry per detected change, with its diff and (when `ai_mode` is
        on) the AI summary.
    """
    _logger().info("Fetching changes for monitor %s", monitor_id)
    return _dump_all(anakin_credentials.get_client().monitors.changes(monitor_id))


@task(name="anakin_monitor_control", retries=0)
def monitor_control(
    monitor_id: str,
    action: str,
    anakin_credentials: AnakinCredentials,
) -> dict[str, Any] | None:
    """Pause, resume, run, or delete a monitor.

    Args:
        monitor_id: The monitor's `id`.
        action: "pause", "resume", "run_now" (an immediate check, billed like
            a scheduled one) or "delete" (removes the monitor and its history).
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        The updated monitor for pause/resume, the queued run for run_now,
        and None for delete.
    """
    if action not in _MONITOR_ACTIONS:
        raise ValueError(
            f"Unknown monitor action {action!r}: use one of {', '.join(_MONITOR_ACTIONS)}."
        )

    _logger().info("Monitor %s: %s", monitor_id, action)
    monitors = anakin_credentials.get_client().monitors
    if action == "delete":
        monitors.delete(monitor_id)
        return None
    if action == "pause":
        result: BaseModel = monitors.pause(monitor_id)
    elif action == "resume":
        result = monitors.resume(monitor_id)
    else:
        result = monitors.run_now(monitor_id)
    return anakin_http.redact_secrets(_dump(result))  # type: ignore[no-any-return]


# ─── AI visibility ──────────────────────────────────────────────────────────


@task(name="anakin_ai_visibility_sources", retries=2, retry_delay_seconds=5)
def ai_visibility_sources(anakin_credentials: AnakinCredentials) -> list[dict[str, Any]]:
    """List the AI answer engines `ai_visibility_search` can query.

    Args:
        anakin_credentials: Block holding the Anakin API key.

    Returns:
        Each engine's slug (pass these as `sources`) and display name.
    """
    return _dump_all(anakin_credentials.get_client().ai_visibility.sources())


@task(name="anakin_ai_visibility_search", retries=1, retry_delay_seconds=15)
def ai_visibility_search(
    query: str,
    anakin_credentials: AnakinCredentials,
    *,
    sources: list[str] | None = None,
    country: str = "us",
    include_full_content: bool = False,
) -> dict[str, Any]:
    """Ask several AI answer engines (ChatGPT, Gemini, Google AI Overview, ...) one question.

    Useful for checking how a brand or topic shows up in AI answers, per
    country. Billed per engine that answers; failed engines are free. A
    search where some engines failed is returned, not raised.

    Args:
        query: The question (max 2000 characters).
        anakin_credentials: Block holding the Anakin API key.
        sources: Engine slugs from `ai_visibility_sources`. Omit for all.
        country: Two-letter country code for the search location.
        include_full_content: Keep each engine's full answer text. Off by
            default; summaries and the cross-engine synthesis are always returned.

    Returns:
        `search_id`, `status`, `country`, `synthesis` and one `results`
        entry per engine.
    """
    logger = _logger()
    logger.info("Running AI visibility search: %s", query)

    result = _dump(
        anakin_credentials.get_client().ai_visibility.search(
            query, sources=sources, country=country
        )
    )
    if not include_full_content:
        result["results"] = [
            {k: v for k, v in r.items() if k != "full_content"} for r in result.get("results", [])
        ]

    logger.info("AI visibility search %s: %s", result.get("search_id"), result.get("status"))
    return result


# ─── Browser sessions and AI browser tasks ──────────────────────────────────


@task(name="anakin_session_list", retries=2, retry_delay_seconds=5)
def session_list(
    anakin_credentials: AnakinCredentials,
    *,
    domain: str | None = None,
) -> list[dict[str, Any]]:
    """List saved browser sessions (logged-in states you created in the Anakin dashboard).

    Pass a session's `id` as `session_id` to `scrape_url`, `crawl`,
    `monitor_create` or `browser_task` to run as that logged-in user.

    Args:
        anakin_credentials: Block holding the Anakin API key.
        domain: Only sessions for this domain, e.g. "amazon.com".

    Returns:
        Sessions with `id`, `name`, `website_domain` and `is_active`.
    """
    return _dump_all(anakin_credentials.get_client().sessions.list(domain=domain))


@task(name="anakin_session_delete", retries=0)
def session_delete(session_id: str, anakin_credentials: AnakinCredentials) -> None:
    """Permanently delete a saved browser session and its encrypted login data.

    Anything that uses this session loses its logged-in access.

    Args:
        session_id: The session's `id`, from `session_list`.
        anakin_credentials: Block holding the Anakin API key.
    """
    _logger().info("Deleting browser session %s", session_id)
    anakin_credentials.get_client().sessions.delete(session_id)


@task(name="anakin_browser_task", retries=0)
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
    """Run a task described in plain language in a cloud browser driven by an AI agent.

    The agent navigates, clicks, types and extracts. Use it for multi-step
    flows `scrape_url` can't do and no Wire action covers (Wire actions are
    faster and cheaper, so try `wire_discover` first). Runs are capped at
    about 330 seconds server-side. For logged-in tasks pass `session_id`;
    never put passwords in `prompt`. Refuses payment or fund-transfer tasks
    (raises `ValueError`).

    Args:
        prompt: What to do and what to return.
        anakin_credentials: Block holding the Anakin API key.
        url: Start here. Omit to let the agent follow URLs in the prompt.
        session_id: Saved browser session to run as a logged-in user.
        max_steps: Maximum agent steps.
        timeout_ms: Task timeout in milliseconds.
        output_schema: JSON Schema the returned `result` should follow.

    Returns:
        `success`, `result`, `steps_taken`, `iterations`, `cached`,
        `duration_ms` and `run_id`.

    Raises:
        RuntimeError: If the task fails or times out.
    """
    _refuse_if_financial(f"{prompt} {url or ''}", "browser task")

    logger = _logger()
    logger.info("Running browser task: %s", prompt)

    body: dict[str, Any] = {"prompt": prompt, "async": True}
    optional = {
        "url": url,
        "session_id": session_id,
        "max_steps": max_steps,
        "timeout_ms": timeout_ms,
        "output_schema": output_schema,
    }
    body.update({k: v for k, v in optional.items() if v is not None})

    with anakin_credentials.get_http_client() as http_client:
        accepted = anakin_http.request(http_client, "POST", "/ai/evaluate", json=body)
        workflow_id = accepted.get("workflow_id")
        if not workflow_id:
            resolved = accepted
        else:
            job = anakin_http.poll(
                http_client,
                f"/ai/jobs/{workflow_id}",
                is_terminal=lambda b: b.get("status") in {"completed", "failed", "timed_out"},
            )
            if job.get("status") in {"failed", "timed_out"}:
                raise RuntimeError(
                    f"Browser task {job.get('status')}: {job.get('error', 'unknown')}"
                )
            resolved = job.get("result") or job

    logger.info("Browser task finished (run_id=%s)", resolved.get("run_id"))
    steps = resolved.get("steps")
    return {
        "success": resolved.get("success"),
        "result": resolved.get("result"),
        "steps_taken": len(steps) if isinstance(steps, list) else None,
        "iterations": resolved.get("iterations"),
        "cached": resolved.get("cached"),
        "duration_ms": resolved.get("duration_ms"),
        "run_id": resolved.get("run_id"),
    }
