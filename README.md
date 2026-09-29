# Janky

Janky is a lightweight HTTP workload generator for testing agent shaped workflows. It runs scripted virtual agents through caller defined sequences and sends each step to an HTTP endpoint. The backend is written in Go with no third party Go dependencies; a small JavaScript SDK is available for Node.js.

The agents are deterministic workflow runners, not LLMs. Janky does not invent roles, prompts, policies, or actions. You provide the profiles, workflow inputs, expected status codes, pacing, and target endpoint.

## Quick start

Start the local control API:

```sh
go run ./cmd/janky 3000
```

The API binds to loopback. Outbound target hosts default to `127.0.0.1`, `localhost`, and `::1`. Set `ALLOWED_TARGET_HOSTS` to a comma separated allowlist to test another host. Only use the control API as trusted local tooling.

Submit a small fleet to your adapter:

```sh
curl -s http://127.0.0.1:3000/runs \
  -H 'content-type: application/json' \
  -d '{"target_url":"http://127.0.0.1:4000/events","agents":100,"concurrency":20,"workflow":[{"action":"lookup","input":{"record":"synthetic-1"}},{"action":"read","input":{"record":"synthetic-1"}}]}'
```

The response includes a `run_id`. Poll `/runs/{run_id}` for progress and results. See [use cases](docs/use-cases.md), [methodology](docs/methodology.md), and [architecture](docs/architecture.md) for details.

## What it does

- Runs many virtual agents as Go goroutines, bounded by a concurrency limit.
- Replays each caller-defined workflow in order, with profile rotation and pacing controls.
- Sends each action as a JSON HTTP POST to one configured target URL.
- Retries throttled and server-error responses, checks expected status codes, and records request outcomes.
- Exposes live events, run status, Prometheus text metrics, and optional target telemetry.
- Keeps recent run history and direct agent state in memory; target response bodies are discarded. Agent workflows hold one current position rather than a query history.
- Provides standalone p50, p95, p99, latency, and heap sampling helpers. These helpers are not connected to live runtime collection.

## API and SDK

The local API provides `/health`, `/status`, `/metrics`, `/events`, `POST /runs`, `/runs/{id}`, and `POST /agents/{id}/invoke`. The Node ESM SDK lives in [`sdk/js/client.mjs`](sdk/js/client.mjs); see [its behavior](docs/architecture.md#javascript-sdk).

## Capacity

Janky does not impose fixed count caps on agents, concurrency, arrival rate, profiles, steps, retries, or simultaneous workloads. Configuration requests remain limited to 64 KiB. The values you request still consume memory, sockets, and CPU, and the target and network may impose lower practical limits. Use synthetic inputs and monitor the target alongside Janky.

## Development

```sh
go test -race -coverprofile=/tmp/janky.cover ./...
go tool cover -func=/tmp/janky.cover | awk '$1 == "total:" && $3 == "100.0%" { ok = 1 } END { exit !ok }'
node --test --experimental-test-coverage --test-coverage-exclude=sdk/js/client.test.mjs \
  --test-coverage-lines=100 --test-coverage-branches=100 \
  --test-coverage-functions=100 sdk/js/client.test.mjs
```

More docs: [documentation index](docs/README.md).
