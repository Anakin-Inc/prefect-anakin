"""Credentials Block for the Anakin API."""

from __future__ import annotations

import os

import httpx
from anakin import Anakin
from prefect.blocks.core import Block
from pydantic import Field, SecretStr

DEFAULT_BASE_URL = "https://api.anakin.io/v1"


class AnakinCredentials(Block):
    """Block holding an Anakin API key for use in tasks and flows.

    Args:
        api_key: Anakin API key (`ask_...`). Get one at
            https://anakin.io/dashboard; the free tier needs no card. If left
            empty, the `ANAKIN_API_KEY` environment variable is used, and if
            that is unset too the client runs in keyless mode, where only
            `scrape_url`, `wire_discover` and `wire_catalog` work (on a small
            free per-IP allowance).
        base_url: Override the API base URL. Only needed for internal or
            test environments.

    Examples:
        Save credentials once (or create the block in the Prefect UI):
        ```python
        from prefect_anakin import AnakinCredentials

        AnakinCredentials(api_key="ask_...").save("anakin")
        ```

        Load them in a flow and use the SDK client directly:
        ```python
        from prefect_anakin import AnakinCredentials

        client = AnakinCredentials.load("anakin").get_client()
        doc = client.scrape("https://example.com")
        ```
    """

    _block_type_name = "Anakin Credentials"
    _block_type_slug = "anakin-credentials"
    _logo_url = "https://anakin.io/favicon.png"  # type: ignore[assignment]
    _description = "Authenticates Anakin web data tasks (scrape, crawl, search, Wire, monitors)."
    _documentation_url = "https://anakin.io/docs/integrations/workflow/prefect"  # type: ignore[assignment]

    api_key: SecretStr | None = Field(
        default=None,
        description=(
            "Anakin API key, sent as the X-API-Key header. Falls back to the "
            "ANAKIN_API_KEY environment variable when empty."
        ),
        title="API Key",
    )
    base_url: str | None = Field(
        default=None,
        description=f"Override the Anakin API base URL. Defaults to {DEFAULT_BASE_URL}.",
        title="Base URL",
    )

    def _api_key(self) -> str | None:
        return self.api_key.get_secret_value() if self.api_key else None

    def get_client(self) -> Anakin:
        """Return an authenticated `anakin.Anakin` SDK client."""
        if self.base_url:
            return Anakin(api_key=self._api_key(), base_url=self.base_url)
        return Anakin(api_key=self._api_key())

    def get_http_client(self) -> httpx.Client:
        """Return a raw `httpx.Client` for the one endpoint `anakin-sdk` doesn't wrap.

        Only `browser_task` (`POST /ai/evaluate`) uses this. Use it as a
        context manager so the connection pool is closed.
        """
        key = self._api_key() or os.environ.get("ANAKIN_API_KEY")
        if not key:
            raise ValueError(
                "This task needs an Anakin API key: set api_key on the "
                "AnakinCredentials block or the ANAKIN_API_KEY environment variable."
            )
        return httpx.Client(
            base_url=self.base_url or DEFAULT_BASE_URL,
            headers={"X-API-Key": key},
            timeout=60.0,
        )
