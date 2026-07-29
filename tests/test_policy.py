"""Unit tests for the financial-transaction guard (`prefect_anakin._policy`).

Pure-function tests — no HTTP, no Prefect context. Ported alongside
`anakin-mcp/src/tools/policy.ts::financialBlockReason`, whose behavior these
cases mirror (see `_policy.py`'s module docstring for why this guard exists).
"""

from __future__ import annotations

import pytest

from prefect_anakin._policy import financial_block_reason


@pytest.mark.parametrize(
    "text",
    [
        "amazon.checkout {}",
        "amazon_checkout {}",  # snake_case action id
        "stripe-charge-card {}",  # kebab-case action id
        "walmart.place_order {'item': 'tv'}",
        "buy now on this site",
        "wire transfer $500 to this account",
        "process a payout to the vendor",
    ],
)
def test_blocks_financial_text(text: str) -> None:
    assert financial_block_reason(text) is not None


@pytest.mark.parametrize(
    "text",
    [
        "amazon.add_to_cart {'item': 'tv'}",
        "linkedin.submit_contact_form {'name': 'a'}",
        "walmart.search_products {'q': 'tv'}",
        "read the product price and specs",
    ],
)
def test_allows_benign_text(text: str) -> None:
    assert financial_block_reason(text) is None


def test_env_escape_hatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANAKIN_ALLOW_FINANCIAL", "true")
    assert financial_block_reason("amazon.checkout {}") is None
