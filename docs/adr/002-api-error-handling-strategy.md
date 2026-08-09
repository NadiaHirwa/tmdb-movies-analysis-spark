# ADR 002: Explicit timeout, retry, and status-code checking for API calls

## Status
Accepted

## Context
The original ingestion script called `requests.get()` with no timeout, no retry logic, and no `raise_for_status()` check, and had no `try/except` anywhere in the file. Invalid movie IDs were detected only indirectly, by checking whether the `"title"` key was missing from the parsed response — which happens to work for a 404, but does not check what the API actually reported. During live execution this contributed to at least one unhandled crash.

## Decision
`fetch_movie()` in `src/ingestion/api.py` now:
- Sets an explicit `timeout` on every request.
- Checks `response.status_code == 404` directly to detect an invalid movie ID, rather than inferring it from the response body's shape.
- Calls `response.raise_for_status()` to convert any other bad HTTP status into a catchable exception.
- Wraps the request in `try/except`, distinguishing three failure types: `Timeout`, `HTTPError`, and the broader `RequestException`.
- Retries transient failures (timeouts, server errors, network errors) up to 3 times with linear backoff, but does not retry a 404, since retrying can never make an invalid ID valid.

## Consequences
- A single bad or slow request can no longer crash the entire ingestion run.
- Invalid IDs are now correctly detected based on what the server actually reported, not an assumption about response shape.
- Retry attempts add a small amount of latency to genuinely failing requests, which is an acceptable trade-off for resilience against transient network issues.

## Alternatives Considered
Exponential backoff (doubling the wait time between retries) was considered instead of linear backoff. Linear backoff was chosen as sufficient given the small number of requests (19) and the low likelihood of sustained rate-limiting from TMDb's API at this volume; exponential backoff would be worth revisiting if this pipeline were scaled to a much larger number of requests.