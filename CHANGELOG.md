# Changelog

## 0.1.0 (unreleased)

First release.

- `AnakinCredentials` Block (API key optional: falls back to `ANAKIN_API_KEY`,
  then to keyless mode for scrape and Wire discovery).
- 22 tasks covering scrape, map, crawl, search, agentic search, Wire
  (discover, catalog, read, write, identities, login, build, build status),
  website monitors, AI visibility, browser sessions and AI browser tasks.
- Built on `anakin-sdk` 0.2. Wire actions accept `credential_id` /
  `identity_id`, so actions that need a signed-in account work.
- Tasks return JSON-serializable dicts and lists. Write tasks don't retry.
