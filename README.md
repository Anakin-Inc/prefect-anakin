# prefect-anakin

Prefect tasks and a credentials Block for [Anakin](https://anakin.io) — web
scraping, AI search, and multi-stage agentic research, built on the official
[`anakin-sdk`](https://github.com/Anakin-Inc/anakin-py) Python client (not a
separate HTTP implementation).

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

| Task | What it does |
|---|---|
| `scrape_url` | Scrape a URL to markdown, optionally AI-extract structured JSON |
| `search` | Synchronous AI-powered web search with citations |
| `agentic_search` | Multi-stage research: search, scrape citations, extract structured data with a summary |

```python
from prefect import flow
from prefect_anakin import AnakinCredentials, agentic_search, scrape_url, search


@flow
def research_flow(topic: str) -> dict:
    anakin_credentials = AnakinCredentials.load("anakin-prod")

    doc = scrape_url("https://example.com", anakin_credentials)
    hits = search(topic, anakin_credentials, limit=5)
    findings = agentic_search(
        f"Summarize the current state of {topic}", anakin_credentials
    )
    return {"doc": doc, "hits": hits, "findings": findings}
```

Each task is a plain `@task`-decorated function that takes an
`AnakinCredentials` block as an explicit parameter — the same shape as
`prefect_slack.messages.send_chat_message(channel, slack_credentials, ...)`
and `prefect_aws.s3.adownload_from_bucket(bucket, key, aws_credentials, ...)`.
Call `.fn(...)` on any task to invoke the plain Python function directly
(outside a flow run), e.g. for unit tests.

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
