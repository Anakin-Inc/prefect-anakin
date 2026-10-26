"""Runs the tasks inside real Prefect flows against a temporary local Prefect API.

Covers what `.fn(...)` tests can't: task decoration, the run logger, result
serialization, and saving/loading the credentials Block through the API.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from anakin.models import Document, WireActionMatch
from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from prefect_anakin import AnakinCredentials, scrape_url, wire_discover

KEY = "ask_" + "1" * 64


@pytest.fixture(scope="module", autouse=True)
def prefect_api() -> Iterator[None]:
    with prefect_test_harness():
        yield


def test_block_save_and_load_round_trip() -> None:
    AnakinCredentials(api_key=KEY).save("anakin-test", overwrite=True)

    loaded = AnakinCredentials.load("anakin-test")

    assert loaded.api_key is not None
    assert loaded.api_key.get_secret_value() == KEY


def test_tasks_run_inside_a_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    client.scrape.return_value = Document(
        id="j1", url="https://example.com", status="completed", markdown="# Hi"
    )
    client.wire.discover.return_value = [WireActionMatch(action_id="example.read")]
    monkeypatch.setattr(AnakinCredentials, "get_client", lambda self: client)

    @flow
    def pipeline() -> tuple[str, str]:
        creds = AnakinCredentials(api_key=KEY)
        doc = scrape_url("https://example.com", creds)
        actions = wire_discover("read example", creds)
        return doc["markdown"], actions[0]["action_id"]

    assert pipeline() == ("# Hi", "example.read")
