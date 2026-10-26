"""Live calls to api.anakin.io. Skipped unless ANAKIN_LIVE_TESTS=1.

Uses the keyless endpoints (scrape, Wire discovery), so no API key is needed.
"""

from __future__ import annotations

import os

import pytest
from prefect import flow

from prefect_anakin import AnakinCredentials, scrape_url, wire_catalog, wire_discover

pytestmark = pytest.mark.skipif(
    os.environ.get("ANAKIN_LIVE_TESTS") != "1",
    reason="set ANAKIN_LIVE_TESTS=1 to call the live API",
)


def test_keyless_flow_against_live_api() -> None:
    @flow
    def live() -> dict:
        creds = AnakinCredentials()
        doc = scrape_url("https://example.com", creds)
        actions = wire_discover("search products on amazon", creds, limit=3)
        catalogs = wire_catalog(creds)
        return {"doc": doc, "actions": actions, "catalogs": catalogs}

    out = live()

    assert "documentation examples" in (out["doc"]["markdown"] or "")
    assert out["actions"] and out["actions"][0]["action_id"]
    assert len(out["catalogs"]) > 0
