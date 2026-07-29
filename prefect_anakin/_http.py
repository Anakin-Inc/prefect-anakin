"""Internal HTTP helpers for Anakin endpoints not yet covered by `anakin-sdk` v0.1.0.

`anakin-sdk`'s `anakin.Anakin` client (read directly from
`anakin-py/src/anakin/client.py`) wraps `scrape`, `map`, `crawl`, `search`,
`agentic_search`, `wire` (run one action), and `sessions.*` (list/create/save
/update/delete). It does **not** wrap Wire discovery/catalog/identities/login
/build, Website Monitoring, AI Visibility, or `browser_task` — confirmed by
reading the module directly, not assumed. Those are real, documented REST
endpoints per Anakin's own MCP server source (`anakin-mcp/src/client.ts`,
`anakin-mcp/src/tools/*.ts`), so the tasks that cover them in this package
call the API directly over `httpx` via `AnakinCredentials.get_http_client()`.

This module intentionally mirrors, in miniature, the shape of
`anakin-py/src/anakin/_http.py` and `anakin-mcp/src/client.ts` (error
normalization, job polling) so behavior is consistent with the SDK-backed
tasks in this same package — but it is not part of the public API.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

# Matches anakin-mcp/src/client.ts: POLL_INTERVAL_MS=3000, POLL_MAX_ATTEMPTS=60
# (3 minutes total).
POLL_INTERVAL_SECONDS = 3.0
POLL_MAX_ATTEMPTS = 60
# Browser AI tasks are hard-capped server-side at ~330s, so the poll window
# must outlast the longest legitimate run (anakin-mcp's
# BROWSER_TASK_POLL_MAX_ATTEMPTS = 120, 6 minutes total).
BROWSER_TASK_POLL_MAX_ATTEMPTS = 120

_MONITOR_SECRET_KEYS = {"alertWebhookSecret"}
_REDACTED = "[redacted — view in the Anakin dashboard]"


class AnakinHTTPError(RuntimeError):
    """Raised for a non-2xx response from a raw Anakin HTTP call.

    Mirrors the error-shape handling in `anakin-mcp`'s `AnakinClient.toError`
    and `anakin-py`'s `HttpClient._handle_response`: Anakin returns either a
    flat `{"error": "msg", "code": "CODE"}` body or Wire's nested
    `{"error": {"code": ..., "message": ..., ...}}` envelope.
    """

    def __init__(self, method: str, path: str, response: httpx.Response) -> None:
        self.status_code = response.status_code
        self.code: str | None = None
        self.details: dict[str, Any] | None = None
        message = f"{method} {path} failed ({response.status_code})"

        try:
            body = response.json()
        except ValueError:
            body = None

        if isinstance(body, dict):
            err = body.get("error")
            if isinstance(err, dict):
                # Wire's nested envelope.
                message = err.get("message", message)
                self.code = err.get("code")
                self.details = err
            elif isinstance(err, str):
                # Legacy flat envelope.
                message = err
                self.code = body.get("code")

        super().__init__(message)


def request(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    json: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Issue one request against `client` and return the parsed JSON body.

    Raises `AnakinHTTPError` on a non-2xx response.
    """
    response = client.request(method, path, json=json, params=params)
    if response.is_error:
        raise AnakinHTTPError(method, path, response)
    if not response.content:
        return {}
    return response.json()


def poll(
    client: httpx.Client,
    path: str,
    *,
    is_terminal: Callable[[dict[str, Any]], bool],
    max_attempts: int = POLL_MAX_ATTEMPTS,
    interval_seconds: float = POLL_INTERVAL_SECONDS,
) -> dict[str, Any]:
    """Poll `GET path` until `is_terminal(body)` is true, at a fixed interval.

    Matches `anakin-mcp/src/client.ts`'s `pollJob`/`aiVisibilitySearch` loop
    (fixed-interval polling), not `anakin-py`'s exponential backoff — kept
    simple since these endpoints have no SDK-side polling to stay consistent
    with.
    """
    for _ in range(max_attempts):
        body = request(client, "GET", path)
        if is_terminal(body):
            return body
        time.sleep(interval_seconds)
    raise TimeoutError(
        f"Polling {path} did not reach a terminal state after "
        f"{max_attempts * interval_seconds:.0f}s ({max_attempts} attempts)."
    )


def redact_secrets(value: Any) -> Any:
    """Recursively replace known secret-bearing keys with a placeholder.

    Ports `anakin-mcp/src/tools/monitor.ts::redactSecrets`. The Website
    Monitoring API returns each monitor's `alertWebhookSecret` (an HMAC
    signing secret) inline; a secret that enters a Prefect log or task
    result is compromised by definition, so every raw monitor response in
    this package is passed through this before it reaches a flow — callers
    retrieve the real value from the Anakin dashboard instead.
    """
    if isinstance(value, list):
        return [redact_secrets(v) for v in value]
    if isinstance(value, dict):
        return {
            k: (_REDACTED if k in _MONITOR_SECRET_KEYS and v else redact_secrets(v))
            for k, v in value.items()
        }
    return value
