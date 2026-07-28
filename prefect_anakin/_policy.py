"""Financial-transaction guard for actions that can change state on a target site.

Ports `anakin-mcp/src/tools/policy.ts::financialBlockReason` to Python. The
Anthropic Connectors Directory prohibits a connector from "executing
financial transactions or transferring assets"
(`compliance/policies/01-software-directory-policy.md` in `anakin-mcp`), and
`anakin-mcp` applies the same guard to its `wire_write_action`, `wire_build`,
and `browser_task` tools — all three exist in this package too
(`wire_write_action`, `wire_build`, `browser_task`), so they carry the same
guard here for parity. This is defense-in-depth on top of whatever the
Anakin API itself refuses; it is intentionally conservative and targets
payment *completion* / fund *transfer* only (pay, checkout, purchase, place
order, buy now, wire transfer, remit, payout, charge card) — not benign
writes like "add to cart" or "submit a form", and it is never applied to
read actions.
"""

from __future__ import annotations

import os
import re

_FINANCIAL_PATTERN = re.compile(
    r"\b(payments?|pay\s?now|checkout|purchase|place\s?order|buy\s?now|"
    r"wire\s?transfer|remit(?:tance)?|payout|charge\s?card|transfer\s?funds)\b",
    re.IGNORECASE,
)


def financial_block_reason(text: str) -> str | None:
    """Return a human-readable refusal reason if `text` looks like a payment
    or fund-transfer request, else `None`.

    `text` is typically an action id + serialized params, or a goal/prompt +
    URL. Set the `ANAKIN_ALLOW_FINANCIAL=true` environment variable to
    disable this guard (matches `anakin-mcp`'s escape hatch) — not
    recommended for production use of this package.
    """
    if os.environ.get("ANAKIN_ALLOW_FINANCIAL") == "true":
        return None

    # `\b` doesn't break on `_`/`-`, so "amazon_checkout" must become "amazon
    # checkout" for `\bcheckout\b` to match snake_case/kebab-case action ids.
    haystack = re.sub(r"[_-]+", " ", text)
    if _FINANCIAL_PATTERN.search(haystack):
        return (
            "This connector does not perform financial transactions or transfer "
            "funds/assets, so this action was not run (Anthropic Connectors "
            "Directory policy, mirrored from anakin-mcp/src/tools/policy.ts). Use "
            "a read action to look up information, or complete any payment "
            "directly on the site."
        )
    return None
