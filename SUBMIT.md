# prefect-anakin — submission instructions

## Correction to the starting premise — read this first

The brief for this package assumed Prefect's docs confirm a documented
`prefect-collection-template` scaffold and that third-party collections are
"auto-discovered into their Integrations Catalog once published under the
`prefect-` PyPI prefix — no PR into prefect's own monorepo needed." That
turned out to be **not what the current, live sources say.** Checked
directly, not assumed:

- `github.com/PrefectHQ/prefect-collection-template` — fetched live —
  carries a banner marking it **archived as of March 20, 2026**. It's a
  real cookiecutter template (`cruft create https://github.com/PrefectHQ/prefect-collection-template`,
  `cookiecutter.json` confirmed with fields `collection_name`,
  `collection_slug`, etc.) but it's dead — not the current path.
- `docs.prefect.io/contribute/contribute-integrations.md` — fetched live —
  states plainly: **"All integrations are hosted in the Prefect GitHub
  repository under `src/integrations`"** and, for a genuinely new
  integration idea, points contributors to open a GitHub Discussion, not a
  standalone-repo self-serve process.
- `PrefectHQ/prefect-collection-registry` (the service that actually
  populates docs.prefect.io's Integrations Catalog) — read its source
  directly, not the README — confirms this in code, not just docs.
  `src/prefect_collection_registry/utils.py::get_collection_names()` lists
  collections by calling the GitHub Contents API against
  `PrefectHQ/prefect/src/integrations` and filtering directory names
  starting with `prefect-`. Its own docstring: *"Collections live as
  subdirectories under `src/integrations/` in the prefect monorepo (the
  standalone `prefect-*` repos are archived). Every directory there is a
  Prefect-authored integration."* There is no PyPI-namespace scan anywhere
  in this codebase. A `prefect-anakin` package published to PyPI does
  **not** get picked up by this automation.
- The catalog does list a few non-Prefect-maintained entries (Coiled,
  Fivetran, EBI Metagenomics/Slurm — confirmed via
  `docs.prefect.io/integrations/integrations`), so third-party listing is
  possible, but nothing in the registry code or contribute docs describes
  a public, automatic, self-serve path to get there. It reads as
  case-by-case/manual, not documented self-serve.

**What is real and still worth building:** a standalone `prefect-anakin`
PyPI package is a legitimate, fully-functional Prefect collection the
moment someone installs it — that part of the premise holds, just at the
*local package* level, not the *public catalog listing* level. Confirmed
live in `PrefectHQ/prefect`'s current source
(`src/prefect/_internal/plugins/collections.py`):
`load_prefect_collections()` calls
`importlib.metadata.entry_points(group="prefect.collections")` and imports
every match. That's a real, functioning discovery mechanism — any package
that declares `[project.entry-points."prefect.collections"]` in its
`pyproject.toml` gets auto-imported by Prefect once installed, which is
what makes its Blocks and tasks show up for `prefect block register -m
prefect_anakin` and normal `from prefect_anakin import ...` use. This
package declares that entry point. It just doesn't, by itself, put
`prefect-anakin` on docs.prefect.io.

## What's here

Structure follows the real, current `prefect_aws`/`prefect_slack` pattern
(pulled live from `PrefectHQ/prefect`'s `src/integrations/prefect-aws` and
`src/integrations/prefect-slack` via `gh api`/raw fetch — the
monorepo-internal shape is what's live and current, since the standalone
template is archived), adapted for a real standalone PyPI package the way
`langchain-anakin` in this same batch is standalone relative to
`langchain-community`:

```
prefect_anakin/
    __init__.py         exports AnakinCredentials + all 21 task functions
    _version.py          __version__ = "0.1.0"
    credentials.py       AnakinCredentials(Block) — api_key, base_url,
                          get_client() (anakin-sdk), get_http_client() (raw httpx)
    tasks.py              all 21 capabilities as @task functions
    _http.py              internal: raw-HTTP request/poll/redact_secrets
                          helpers for endpoints anakin-sdk doesn't wrap yet
    _policy.py            internal: financial-transaction guard, ported
                          from anakin-mcp/src/tools/policy.ts
tests/
    test_credentials.py
    test_tasks.py
    test_http.py
    test_policy.py
pyproject.toml, README.md, LICENSE
```

- `AnakinCredentials` subclasses `prefect.blocks.core.Block` directly
  (not `CredentialsBlock` from `prefect.blocks.abstract` — that ABC is
  used by `prefect_aws.AwsCredentials` for boto3-session-shaped auth with
  multiple credential paths; Anakin auth is a single `X-API-Key` header,
  which is exactly the shape `prefect_slack.SlackCredentials` uses with
  plain `Block`, so that's the closer real reference).
- `get_client()` returns a real `anakin.Anakin` SDK instance — same
  "reuse the SDK, don't reimplement HTTP" choice as `langchain-anakin` and
  `dagster-anakin` elsewhere in this integration set.
- `get_http_client()` (new) returns a raw `httpx.Client` (same base URL and
  `X-API-Key` header convention as `anakin.Anakin`'s internal HTTP client)
  for the 12 capabilities `anakin-sdk` v0.1.0 doesn't wrap — see "Full
  coverage: reused SDK vs. raw HTTP" below.
- Every task takes the credentials block as an explicit parameter
  (`scrape_url(url, anakin_credentials, ...)`), matching
  `prefect_slack.messages.send_chat_message(channel, slack_credentials, ...)`
  and `prefect_aws.s3.adownload_from_bucket(bucket, key, aws_credentials, ...)`
  — both read from the live monorepo, not guessed.
- `pyproject.toml` declares `[project.entry-points."prefect.collections"]`
  (`prefect_anakin = "prefect_anakin"`), matching the real entry-point
  group name and value shape in `prefect_aws`'s and `prefect_slack`'s
  current `pyproject.toml` (`prefect_aws = "prefect_aws"` /
  `prefect_slack = "prefect_slack"`).
- Request/response shapes read directly from `anakin-py/src/anakin/client.py`
  and `anakin-py/src/anakin/models.py` for the SDK-backed tasks, and from
  Anakin's own MCP server source (`anakin-mcp/src/client.ts`,
  `anakin-mcp/src/tools/*.ts`) for both the SDK-backed tasks' semantics and
  the raw-HTTP tasks' endpoints/bodies/polling — not the public API docs,
  consistent with every other integration in this batch (the docs were
  previously found to omit real parameters like `forceFresh` and agentic
  search's `useBrowser`/`schema`).

## Full coverage: reused SDK vs. raw HTTP

This package now covers all 21 of Anakin's REST API capabilities (up from
3 at initial submission). 9 reuse `anakin-sdk`; 12 call the API directly.

**Reused `anakin-sdk` (`AnakinCredentials.get_client()`):** `scrape_url`,
`search`, `agentic_search` (original three) plus `map`, `crawl`,
`wire_read_action`, `wire_write_action` (both via `Anakin.wire()`),
`session_list`, `session_delete` (both via `Anakin.sessions.*`).

**Raw HTTP (`AnakinCredentials.get_http_client()` + `prefect_anakin/_http.py`):**
`wire_discover`, `wire_catalog`, `wire_identities`, `wire_login`,
`wire_build`, `monitor_create`, `monitor_list`, `monitor_changes`,
`monitor_control`, `ai_visibility_search`, `ai_visibility_sources`,
`browser_task` — confirmed missing from `anakin-sdk` v0.1.0 by reading
`anakin-py/src/anakin/client.py` directly (only `Anakin.scrape`, `.map`,
`.crawl`, `.search`, `.agentic_search`, `.wire`, and `.sessions.*` exist
there), not assumed from a task brief.

**Correction to the task brief's own premise, verified by reading the
source, not assumed:** the brief that produced this PR grouped
`wire_discover`/`wire_catalog`/`wire_read_action`/`wire_write_action`/
`wire_identities`/`wire_login`/`wire_build` together as "anakin-sdk's
`Anakin.wire()` method family" and separately asserted `session_list`/
`session_delete` were "NOT in anakin-sdk (confirmed missing)". Reading
`anakin-py/src/anakin/client.py` directly shows both claims are only
half right:
- `Anakin.wire()` is a single method (`action_id`, `params`,
  `poll_timeout`) that only covers the two *execution* tools
  (`wire_read_action`/`wire_write_action`). Discovery/catalog/identities/
  login/build have no SDK method at all — those 5 are raw HTTP here.
- `session_list`/`session_delete` **are** covered — `anakin.Anakin` exposes
  a `SessionsClient` sub-namespace (`client.sessions.list()` /
  `client.sessions.delete()`) hitting the same `GET /sessions` /
  `DELETE /sessions/:id` routes `anakin-mcp`'s `session_list`/
  `session_delete` tools call. Both tasks use `Anakin.sessions.*`, not raw
  HTTP, since the SDK already covers them.

**Known gap, surfaced rather than hidden:** `anakin-sdk` v0.1.0's
`Anakin.wire()` takes only `action_id`/`params`/`poll_timeout` — no
`credential_id`/`identity_id` parameter (unlike `anakin-mcp`'s
`wireRun(actionId, params, {credentialId, identityId})`). So
`wire_read_action`/`wire_write_action` in this package cannot run Wire
actions whose `auth_mode` is `"required"` until the SDK adds that
parameter, even though `wire_login`/`wire_identities` (raw HTTP, this PR)
can obtain a `credential_id`. Documented in both tasks' docstrings and the
README rather than silently unsupported.

**Compliance parity:** `wire_write_action`, `wire_build`, and
`browser_task` all refuse (`ValueError`, before any API call) prompts/
actions that look like they complete a payment or transfer funds —
`prefect_anakin/_policy.py::financial_block_reason` is a direct Python port
of `anakin-mcp/src/tools/policy.ts::financialBlockReason` (same regex,
same `ANAKIN_ALLOW_FINANCIAL` escape hatch), so this package doesn't ship a
laxer policy than Anakin's own MCP connector for the same underlying API
surface.

**Secret handling:** Website Monitoring's `POST /monitors` (and every other
monitor endpoint) returns each monitor's `alertWebhookSecret` inline. Per
this project's own secrets rule (a secret that enters a log/transcript is
compromised by definition), every raw monitor response is passed through
`prefect_anakin._http.redact_secrets` — a port of
`anakin-mcp/src/tools/monitor.ts::redactSecrets` — before it reaches a
flow's logs or return value; callers retrieve the real secret from the
Anakin dashboard.

## Verified, not assumed

- `python3 -m py_compile` passes clean on all ten Python files
  (`prefect_anakin/__init__.py`, `_version.py`, `credentials.py`,
  `tasks.py`, `_http.py`, `_policy.py`, `tests/test_credentials.py`,
  `tests/test_tasks.py`, `tests/test_http.py`, `tests/test_policy.py`) —
  syntactically valid.
- `anakin-sdk` is live on PyPI right now (confirmed via
  `curl https://pypi.org/pypi/anakin-sdk/json` → 200, version `0.1.0`).
  `pyproject.toml` depends on it normally (`anakin-sdk>=0.1.0,<0.2.0`) — no
  local path override.
- `anakin-py/src/anakin/client.py` read in full (not skimmed) to confirm
  exactly which of the 21 capabilities it wraps before writing a single
  raw-HTTP task — see "Full coverage" above for what that reading changed
  versus the task brief's own premise.
- `httpx>=0.27.0` added as an explicit (not just transitive)
  `pyproject.toml` dependency, matching the floor `anakin-sdk` itself pins
  — `prefect_anakin/_http.py` and `credentials.py::get_http_client()` now
  import it directly.
- Current `prefect` release on PyPI is `3.8.0` (checked live); pinned
  `prefect>=3.0.0,<4.0.0` — the `Block`/`@task`/entry-points API surface
  used here is stable across the Prefect 3.x line as of this reading.
- Every Block field, task signature, and entry-point declaration checked
  against `PrefectHQ/prefect`'s real current source
  (`src/integrations/prefect-aws/prefect_aws/credentials.py`,
  `.../s3.py`, `src/integrations/prefect-slack/prefect_slack/credentials.py`,
  `.../messages.py`, `src/prefect/plugins.py`,
  `src/prefect/_internal/plugins/collections.py`), fetched live via `curl`
  against `raw.githubusercontent.com` and the GitHub Contents API — not
  memory, not the archived template.

## Not verified

- **Never installed `prefect`, `anakin-sdk`, or `httpx` in this environment
  and never ran `pytest`.** The four test files (`tests/test_credentials.py`,
  `tests/test_tasks.py`, `tests/test_http.py`, `tests/test_policy.py`) mock
  the Anakin client / raw HTTP client and call `.fn(...)` on each task to
  bypass the need for a live Prefect orchestration context — they should
  run with a plain `pip install -e ".[test]"` and no network access beyond
  that install, but that install/run was not performed here. This is the
  same gap the original 3-task submission had, now applying to all 18 new
  tasks too — `python3 -m py_compile` (confirmed clean above) checks syntax
  only, not runtime behavior, mock correctness, or the real Anakin API's
  actual response shapes for the 12 endpoints only read from `anakin-mcp`'s
  TypeScript source (never called live).
- **Never ran `prefect block register -m prefect_anakin`** against a real
  Prefect server or Prefect Cloud workspace — that requires a running
  Prefect API (local `prefect server start` or a Cloud workspace) which
  this environment doesn't have. Block schema registration, the Block's
  appearance in the Prefect UI, and `AnakinCredentials.save()`/`.load()`
  round-tripping through a real API are all unverified.
- **Never executed a `@flow` that calls these tasks** — no live proof the
  `@task` decorator, retries, or `get_run_logger()` behave as expected
  inside real Prefect orchestration (only that the underlying plain
  functions, called via `.fn(...)`, produce the right dict shapes against
  a mocked SDK client).
- **Never confirmed a path to the public Integrations Catalog** — see the
  correction above. `entry-points` discovery is confirmed at the code
  level; a docs.prefect.io listing is not, and no self-serve process for
  it was found.

## Steps (needs the account owner)

1. `python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[test]" && pytest` —
   never run here, needs normal network access.
2. `prefect server start` (or a Cloud workspace) +
   `prefect block register -m prefect_anakin`, then manually confirm
   `AnakinCredentials` appears in the Prefect UI and a `@flow` using these
   tasks actually runs end-to-end against the real Anakin API.
3. Push as `Anakin-Inc/anakin-prefect`, publish to PyPI as `prefect-anakin`.
4. If a public catalog listing is wanted: open the GitHub Discussion
   `contribute-integrations.md` points to
   (https://github.com/PrefectHQ/prefect/discussions/new?category=ideas)
   to ask Prefect directly what the current path is for a third-party
   integration to be added to `src/integrations` or otherwise listed —
   there is no documented self-serve alternative as of this reading.

## Not done

Never installed or run — verified by close reading against real, live
current source (`PrefectHQ/prefect`'s monorepo, not the archived
standalone template) and `py_compile` only, same status as the other
integrations in this batch.
