"""Credential Block for authenticating with the Anakin API.

Pattern mirrors `prefect_slack.credentials.SlackCredentials` and
`prefect_aws.credentials.AwsCredentials` (both pulled from
`PrefectHQ/prefect`'s current `src/integrations/` tree, not assumed): a
`Block` subclass holding the secret, plus a `get_client()` method that
returns an authenticated client instance for tasks to use.
"""

from __future__ import annotations

from anakin import Anakin
from prefect.blocks.core import Block
from pydantic import Field, SecretStr


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
