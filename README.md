# prefect-anakin

Prefect tasks and a credentials Block for [Anakin](https://anakin.io), a web
data API. Scrape and crawl sites to markdown, run AI web search and research,
run pre-built [Wire](https://anakin.io/docs) actions on hundreds of sites,
watch pages for changes, and drive an AI agent in a cloud browser, all from
your Prefect flows.

Built on the official [`anakin-sdk`](https://pypi.org/project/anakin-sdk/)
Python client. Supports Prefect 3 on Python 3.10 to 3.13.

## Install

```bash
pip install prefect-anakin
```

Register the Block type with your Prefect server or Cloud workspace (once per
workspace):

```bash
prefect block register -m prefect_anakin
```

## Credentials

Get an API key at [anakin.io/dashboard](https://anakin.io/dashboard). The free
tier needs no card. Save it as a Block, either in the Prefect UI (Blocks, then
**Anakin Credentials**) or from Python:

```python
from prefect_anakin import AnakinCredentials

AnakinCredentials(api_key="ask_...").save("anakin")
```

If the Block's `api_key` is empty, the `ANAKIN_API_KEY` environment variable is
used. With no key at all, `scrape_url`, `wire_discover` and `wire_catalog`
still work on a small free allowance, which is enough to try the package.

## Example

```python
from prefect import flow
from prefect_anakin import AnakinCredentials, scrape_url, search, wire_discover, wire_read_action


@flow(log_prints=True)
def competitor_watch(product: str) -> dict:
    anakin = AnakinCredentials.load("anakin")

    page = scrape_url("https://example.com/pricing", anakin)
    news = search(f"{product} pricing changes", anakin, limit=5)

    actions = wire_discover(f"search {product} on amazon", anakin, limit=1)
    listings = wire_read_action(actions[0]["action_id"], anakin, params={"query": product})

    print(page["markdown"][:500])
    return {"news": news["results"], "listings": listings["data"]}


if __name__ == "__main__":
    competitor_watch("standing desk")
```

More in [`examples/`](examples/).

Every task takes the `AnakinCredentials` block as an argument and returns plain
JSON (dicts and lists), so results can be cached, persisted and passed between
tasks. Call `task.fn(...)` to run one outside a flow.

## Tasks

| Task | What it does |
|---|---|
| `scrape_url` | Scrape a URL to markdown; optionally extract structured JSON with AI |
| `map` | List the URLs on a site (sitemap plus link traversal) |
| `crawl` | Fetch markdown for many pages of a site in one job |
| `search` | AI web search with citations |
| `agentic_search` | Multi-step research: search, read the sources, return a structured answer |
| `wire_discover` | Find a Wire action for a task described in plain language |
| `wire_catalog` | List supported sites, or one site's actions and their parameters |
| `wire_read_action` | Run a Wire action that reads data |
| `wire_write_action` | Run a Wire action that changes something (form submit, add to cart) |
| `wire_identities` | List your saved site accounts and their `credential_id`s |
| `wire_login` | Sign in to a site that needs credentials and get a `credential_id` |
| `wire_build` / `wire_build_status` | Request a new Wire action for a site that isn't covered yet, and track it |
| `monitor_create` | Watch a page, site or Wire action on a schedule, with webhook or email alerts |
| `monitor_list` / `monitor_changes` | Inspect monitors and the changes they detected |
| `monitor_control` | Pause, resume, run now, or delete a monitor |
| `ai_visibility_search` | Ask ChatGPT, Gemini, Google AI Overview and others one question and compare answers |
| `ai_visibility_sources` | List the AI engines you can query |
| `session_list` / `session_delete` | Manage saved logged-in browser sessions |
| `browser_task` | Have an AI agent complete a multi-step task in a cloud browser |

Each task's docstring lists its parameters. Credit costs per call are on
[anakin.io/pricing](https://anakin.io/pricing).

### Notes

- `wire_write_action`, `wire_build` and `browser_task` refuse requests that
  look like a payment or fund transfer (raise `ValueError`). Set
  `ANAKIN_ALLOW_FINANCIAL=true` to turn the check off.
- Write tasks (`wire_write_action`, `wire_login`, `wire_build`,
  `monitor_create`, `monitor_control`, `session_delete`, `browser_task`) don't
  retry automatically, so a transient error can't repeat a state change. Read
  tasks retry twice.
- Monitor results never include the webhook signing secret; read it in the
  Anakin dashboard.
- Prefect records task inputs. Don't pass passwords to `wire_login` as
  literals: load them from a Prefect `Secret` block inside the flow.

## Development

```bash
uv venv && uv pip install -e . --group test --group lint
uv run pytest                       # unit tests + flow tests on a temporary Prefect API
ANAKIN_LIVE_TESTS=1 uv run pytest tests/test_live.py   # keyless calls to the live API
uv run ruff check . && uv run ruff format --check . && uv run mypy prefect_anakin
```

Releases are published to PyPI by `.github/workflows/publish.yml` when a
`v*` tag is pushed (PyPI trusted publishing, no token).

## License

Apache-2.0
