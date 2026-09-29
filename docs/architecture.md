# Architecture

Janky has a Go HTTP control API and workload runner, plus a small dependency-free JavaScript ESM SDK. It runs as one process; fleet agents are goroutines and outbound target traffic uses Go's HTTP client.

## Request flow

1. A caller submits a fleet configuration to `POST /runs` or invokes one named agent through `POST /agents/{id}/invoke`.
2. The control API validates JSON, workflow fields, numerical values, and target URLs.
3. A fleet run schedules agents by arrival rate and gates active workers by concurrency. An agent selects a profile, walks the workflow in order, and applies think time and jitter between steps.
4. Each step becomes a JSON POST to the configured target. Janky drains and discards the target response body, then records status, latency, timeout, and retry outcome.
5. Callers inspect run snapshots, global status, Prometheus metrics, or server-sent events. Optional target telemetry is sampled once per second and attached to snapshots.

Fleet runs and direct agents can operate at the same time. Active runs remain queryable, recent completed run summaries are retained, and direct agent state remains in memory for the lifetime of the process. Completed fleet configurations are not retained. A direct agent stores its profile, workflow, and next position so the next invocation can advance. The workflow is state-machine data, not a conversation transcript or query-result history.

## Control API and target boundary

The control server binds to `127.0.0.1:<port>`. Target and telemetry URLs must use HTTP(S) and a hostname in the allowlist. By default, that allowlist contains `127.0.0.1`, `localhost`, and `::1`; `ALLOWED_TARGET_HOSTS` replaces it with a comma-separated list. Requests do not use environment proxies and redirects are not followed.

Treat the control API as trusted local tooling. It has no remote authentication layer. Only expose it in environments where callers are trusted. Fleet steps resolve to a top-level URL, a scenario target, or a step target before the run starts. Direct agent invocations accept a single `target_url`. Connection reuse and HTTP version negotiation are handled by Go's HTTP client.

## API surface

- `GET /health`: liveness response.
- `POST /runs`: validate and start a fleet; returns run ID and status URL.
- `GET /runs/{id}`: run state and summary.
- `POST /agents/{id}/invoke`: advance one caller-defined direct workflow step.
- `GET /status`: global totals and recent runs.
- `GET /metrics`: Prometheus text metrics.
- `GET /events`: server-sent event stream, limited to 32 observers.

## JavaScript SDK

Import `createClient` from [`../sdk/js/client.mjs`](../sdk/js/client.mjs). It wraps health, status, metrics, events, fleet submission, run lookup, and direct agent invocation with `fetch`. Methods accept abort signals; non-success HTTP responses reject with an error carrying the numeric status. The SDK does not run the workload itself.

## Code layout

```text
cmd/janky/main.go              CLI entry point
internal/stress/config.go      input parsing and validation
internal/stress/metrics.go     run and request counters
internal/stress/sampling.go    standalone percentile and heap helpers
internal/stress/server.go      HTTP control API and state
internal/stress/work.go        scheduling and outbound requests
sdk/js/client.mjs              JavaScript SDK
```

Go tests live alongside their packages, and SDK tests are under `sdk/js/`. The source tree is organized by responsibility while keeping the executable entry point small.
