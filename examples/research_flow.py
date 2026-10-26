"""Example: scrape, search and Wire in one Prefect flow.

Runs without an API key (keyless mode covers scrape and Wire discovery):

    python examples/research_flow.py

Save an `AnakinCredentials` block named "anakin" (or set ANAKIN_API_KEY) to
unlock the other tasks.
"""

from __future__ import annotations

from typing import Any

from prefect import flow

from prefect_anakin import AnakinCredentials, scrape_url, wire_catalog, wire_discover


def _credentials() -> AnakinCredentials:
    try:
        return AnakinCredentials.load("anakin")
    except ValueError:
        return AnakinCredentials()  # ANAKIN_API_KEY, or keyless


@flow(log_prints=True)
def research(url: str = "https://example.com", intent: str = "search products on amazon") -> Any:
    anakin = _credentials()

    page = scrape_url(url, anakin)
    print(f"Scraped {url}: {len(page['markdown'] or '')} characters of markdown")

    actions = wire_discover(intent, anakin, limit=3)
    for action in actions:
        print(f"Wire action {action['action_id']} ({action['credits']} credits)")

    sites = wire_catalog(anakin)
    print(f"Wire covers {len(sites)} sites")

    return {"page": page, "actions": actions}


if __name__ == "__main__":
    research()
