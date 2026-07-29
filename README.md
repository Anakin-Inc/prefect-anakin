# prefect-anakin

Prefect tasks and a credentials Block for [Anakin](https://anakin.io) — web
scraping, mapping, crawling, AI search, agentic research, Wire automation
actions, website monitoring, AI visibility tracking, browser sessions, and
AI browser automation. All 21 of Anakin's REST API capabilities, built on
the official [`anakin-sdk`](https://github.com/Anakin-Inc/anakin-py) Python
client where it already covers an endpoint, and direct `httpx` calls
(same auth/base-URL convention) where it doesn't yet — see
[Tasks](#tasks) below for which is which.

## Install

```bash
pip install -U prefect-anakin
```

Register the Block type with your Prefect server/Cloud workspace (one-time,
per workspace):

```bash
prefect block register -m prefect_anakin
```

## Blocks

| Block | What it holds |
|---|---|
| `AnakinCredentials` | Your Anakin API key (`ask-...`, free at https://anakin.io/dashboard) plus an optional `base_url` override |

```python
from prefect_anakin import AnakinCredentials

AnakinCredentials(api_key="ask-...").save("anakin-prod")
```

## Tasks

Every task is a plain `@task`-decorated function that takes an
`AnakinCredentials` block as an explicit parameter — the same shape as
`prefect_slack.messages.send_chat_message(channel, slack_credentials, ...)`
and `prefect_aws.s3.adownload_from_bucket(bucket, key, aws_credentials, ...)`.
Call `.fn(...)` on any task to invoke the plain Python function directly
(outside a flow run), e.g. for unit tests.

**SDK-backed** — delegate to `anakin.Anakin` (via `AnakinCredentials.get_client()`)
for the HTTP work, request bodies, and job polling:

| Task | What it does |
|---|---|
| `scrape_url` | Scrape a URL to markdown, optionally AI-extract structured JSON |
| `search` | Synchronous AI-powered web search with citations |
| `agentic_search` | Multi-stage research: search, scrape citations, extract structured data with a summary |
| `map` | Discover URLs reachable from a starting URL (sitemap + link traversal) |
| `crawl` | Bulk-fetch markdown across a site |
| `wire_read_action` | Run a Wire read action (extracts data, no state change) |
| `wire_write_action` | Run a Wire write action (state-changing); refuses payment/fund-transfer actions |
| `session_list` | List saved browser sessions |
| `session_delete` | Permanently delete a saved browser session |

**Raw HTTP** — not yet wrapped by `anakin-sdk` v0.1.0 (confirmed by reading
`anakin-py/src/anakin/client.py` directly); call the API over `httpx` via
`AnakinCredentials.get_http_client()` instead (see `prefect_anakin/_http.py`):

| Task | What it does |
|---|---|
| `wire_discover` | Find Wire actions for a task from a natural-language intent |
| `wire_catalog` | Browse the Wire catalog (all sites, or one site's full action list) |
| `wire_identities` | List saved Wire identities/credentials |
| `wire_login` | Sign in to a credentials-mode Wire site, get a `credential_id` |
| `wire_build` | Request a brand-new Wire action for an uncataloged site; refuses payment/fund-transfer requests |
| `monitor_create` | Create a scheduled website monitor |
| `monitor_list` | List monitors, or fetch one by id |
| `monitor_changes` | Get a monitor's detected-change history |
| `monitor_control` | Pause / resume / run-now / delete a monitor |
| `ai_visibility_search` | Ask multiple AI answer engines the same question and compare answers |
| `ai_visibility_sources` | List the AI answer engines available to `ai_visibility_search` |
| `browser_task` | Run a natural-language task in an AI-driven cloud browser; refuses payment/fund-transfer requests |

```python
from prefect import flow
from prefect_anakin import (
    AnakinCredentials,
    agentic_search,
    monitor_create,
    scrape_url,
    search,
    wire_read_action,
)


@flow
def research_flow(topic: str) -> dict:
    anakin_credentials = AnakinCredentials.load("anakin-prod")

    doc = scrape_url("https://example.com", anakin_credentials)
    hits = search(topic, anakin_credentials, limit=5)
    findings = agentic_search(
        f"Summarize the current state of {topic}", anakin_credentials
    )
    price = wire_read_action(
        "amazon.get_product", anakin_credentials, params={"asin": "B0EXAMPLE"}
    )
    monitor = monitor_create(
        "https://example.com/pricing", 60, anakin_credentials, ai_mode=True
    )
    return {"doc": doc, "hits": hits, "findings": findings, "price": price, "monitor": monitor}
```

Two things worth knowing before you reach for Wire or `browser_task`:

- `wire_write_action`, `wire_build`, and `browser_task` refuse (raise
  `ValueError`) anything that looks like it completes a payment or transfers
  funds — ported from Anakin's own MCP server policy
  (`anakin-mcp/src/tools/policy.ts`) into `prefect_anakin/_policy.py`. Set
  `ANAKIN_ALLOW_FINANCIAL=true` to disable, not recommended in production.
- `anakin-sdk` v0.1.0's `Anakin.wire()` takes only `action_id`/`params`/
  `poll_timeout` — no `credential_id`/`identity_id` yet — so
  `wire_read_action`/`wire_write_action` in this package can't run Wire
  actions whose `auth_mode` is `"required"` until the SDK adds that
  parameter. `wire_login`/`wire_identities` still work (raw HTTP) to obtain
  a `credential_id`; there's just no task-level way to pass it to `wire()`
  today.

## Development

This package depends on `anakin-sdk` and `prefect`, both normal PyPI
installs — no local path overrides needed.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
pytest
```

## License

Apache-2.0.
