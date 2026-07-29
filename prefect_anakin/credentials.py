"""Credential Block for authenticating with the Anakin API.

Pattern mirrors `prefect_slack.credentials.SlackCredentials` and
`prefect_aws.credentials.AwsCredentials` (both pulled from
`PrefectHQ/prefect`'s current `src/integrations/` tree, not assumed): a
`Block` subclass holding the secret, plus a `get_client()` method that
returns an authenticated client instance for tasks to use.
"""

from __future__ import annotations

import httpx
from anakin import Anakin
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

_DEFAULT_BASE_URL = "https://api.anakin.io/v1"


class AnakinCredentials(Block):
    """Block holding an Anakin API key for use in tasks and flows.

    Args:
        api_key: Anakin API key (`ask-...`). Get one free at
            https://anakin.io/dashboard.
        base_url: Override the API base URL. Leave unset for the default
            production endpoint (https://api.anakin.io/v1) — only needed
            for internal/test environments.

    Examples:
        Save credentials (once, e.g. from a script or the Prefect UI):
        ```python
        from prefect_anakin import AnakinCredentials

        AnakinCredentials(api_key="ask-...").save("anakin-prod")
        ```

        Load stored credentials and get a client:
        ```python
        from prefect_anakin import AnakinCredentials

        anakin_credentials = AnakinCredentials.load("anakin-prod")
        client = anakin_credentials.get_client()
        doc = client.scrape("https://example.com")
        ```
    """

    _block_type_name = "Anakin Credentials"
    _logo_url = "https://anakin.io/favicon.ico"
    _documentation_url = "https://anakin.io/docs"

    api_key: SecretStr = Field(
        default=...,
        description="Anakin API key, sent as the X-API-Key header.",
        title="API Key",
    )
    base_url: str | None = Field(
        default=None,
        description=(
            "Override the Anakin API base URL. Leave unset to use the "
            "SDK default (https://api.anakin.io/v1)."
        ),
        title="Base URL",
    )

    def get_client(self) -> Anakin:
        """Return an authenticated `anakin.Anakin` SDK client.

        Reuses the official `anakin-sdk` client directly (same "reuse the
        SDK, don't reimplement HTTP" choice as `langchain-anakin` and
        `dagster-anakin` elsewhere in this integration set) rather than
        issuing raw `httpx`/`requests` calls from this package.
        """
        kwargs: dict[str, object] = {"api_key": self.api_key.get_secret_value()}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        return Anakin(**kwargs)  # type: ignore[arg-type]

    def get_http_client(self) -> httpx.Client:
        """Return a raw `httpx.Client` for Anakin endpoints `anakin-sdk` v0.1.0
        doesn't wrap yet.

        `anakin.Anakin` (see `get_client()`) covers scrape/map/crawl/search/
        agentic_search/wire/sessions — confirmed by reading
        `anakin-py/src/anakin/client.py` directly. Website Monitoring, AI
        Visibility, `browser_task`, and the Wire discovery/catalog/identities/
        login/build endpoints have no SDK wrapper yet, so `prefect_anakin`'s
        tasks for those (see `prefect_anakin.tasks`) call the API directly
        through the client this method returns — same base URL and
        `X-API-Key` auth convention as `anakin.Anakin`'s internal HTTP client
        (`anakin._http.HttpClient`), so behavior is consistent with the
        SDK-backed tasks. Use as a context manager so the connection pool is
        closed:

        ```python
        with anakin_credentials.get_http_client() as http_client:
            ...
        ```
        """
        return httpx.Client(
            base_url=self.base_url or _DEFAULT_BASE_URL,
            headers={"X-API-Key": self.api_key.get_secret_value()},
            timeout=60.0,
        )
