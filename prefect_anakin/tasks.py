"""Tasks for the Anakin web data platform.

Three tasks, matching the three core Anakin endpoints (base URL
`https://api.anakin.io/v1`, auth via `X-API-Key`) that every other
single-purpose Anakin integration in this batch covers as its baseline:

- `scrape_url`      -> POST /url-scraper, polls GET /url-scraper/:jobId
- `search`           -> POST /search (synchronous, no polling)
- `agentic_search`   -> POST /agentic-search, polls GET /agentic-search/:jobId

All three delegate to the official `anakin-sdk` client (`anakin.Anakin`,
via `AnakinCredentials.get_client()`) for the actual HTTP work, request
bodies, and job polling — this package only adds the Prefect `@task`
wrapper, logging, and Block-based credential wiring. Task signatures and
docstring style (Args/Returns, credentials block as an explicit parameter
rather than an implicit global) mirror `prefect_slack.messages.send_chat_message`
and `prefect_aws.s3.adownload_from_bucket`, both pulled from
`PrefectHQ/prefect`'s current `src/integrations/` tree.
"""

from __future__ import annotations

from typing import Any

from prefect import task
from prefect.logging import get_run_logger

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
