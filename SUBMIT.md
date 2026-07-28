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
    __init__.py         exports AnakinCredentials, scrape_url, search, agentic_search
    _version.py          __version__ = "0.1.0"
    credentials.py       AnakinCredentials(Block) — api_key, base_url, get_client()
    tasks.py              scrape_url / search / agentic_search — @task functions
tests/
    test_credentials.py
    test_tasks.py
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
- All three tasks take the credentials block as an explicit parameter
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
  (`Anakin.scrape`, `.search`, `.agentic_search` signatures) and
  `anakin-py/src/anakin/models.py` (`Document`, `SearchResult`,
  `SearchResultItem`, `AgenticSearchResult`, `AgenticSearchData`) — not the
  public API docs, consistent with every other integration in this batch
  (the docs were previously found to omit real parameters like
  `forceFresh` and agentic search's `useBrowser`/`schema`).

## Verified, not assumed

- `python3 -m py_compile` passes clean on all five Python files
  (`prefect_anakin/__init__.py`, `_version.py`, `credentials.py`,
  `tasks.py`, `tests/test_credentials.py`, `tests/test_tasks.py`) —
  syntactically valid.
- `anakin-sdk` is live on PyPI right now (confirmed via
  `curl https://pypi.org/pypi/anakin-sdk/json` → 200, version `0.1.0`).
  `pyproject.toml` depends on it normally (`anakin-sdk>=0.1.0,<0.2.0`) — no
  local path override.
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

- **Never installed `prefect` or `anakin-sdk` in this environment and
  never ran `pytest`.** The two test files (`tests/test_credentials.py`,
  `tests/test_tasks.py`) mock the Anakin client and call `.fn(...)` on
  each task to bypass the need for a live Prefect orchestration context —
  they should run with a plain `pip install -e ".[test]"` and no network
  access beyond that install, but that install/run was not performed here.
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
