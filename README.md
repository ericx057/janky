# Agent workflow stress tester

A dependency-free Go HTTP toolkit for replaying agent-shaped workflows against a caller-selected HTTP endpoint. It uses state machines, not an LLM. Agents keep a workflow identity, carry caller-defined context, pause between steps, retry throttled or failed requests, and issue real HTTP requests. A fleet uses goroutines in one process with bounded concurrency.

## Run

```sh
go run . 3000
```

The control API listens on the loopback port passed to the Go command. Outbound targets are restricted to `127.0.0.1`, `localhost`, and `::1` by default. To test another host, set `ALLOWED_TARGET_HOSTS=api.example.test`. Treat the control API as trusted local tooling.

## JavaScript SDK

Import the dependency-free ESM client from `sdk.mjs` in Node.js:

```js
import { createClient } from './sdk.mjs';

const client = createClient('http://127.0.0.1:3000');
const { run_id } = await client.startRun({
  target_url: 'http://127.0.0.1:4000/events',
  agents: 100,
  workflow: [{ action: 'lookup', input: { record: 'synthetic-1' } }],
});
console.log(await client.getRun(run_id));
```

`startRun()` returns when the run is accepted; call `getRun(run_id)` again to see progress or completion. The client also provides `invokeAgent(id, options)`, `getStatus()`, `getMetrics()` (Prometheus text), and `health()`. `events()` yields parsed live events until its `{ signal }` is aborted or the stream closes. Each method accepts an optional final `{ signal }` argument. HTTP errors reject with the API message and a numeric `status` property.

## Single agent

```sh
curl -s http://127.0.0.1:3000/agents/example/invoke \
  -H 'content-type: application/json' \
  -d '{"profile":{"team":"alpha","context":{"task":"sample"}},"workflow":[{"action":"lookup","input":{"record":"synthetic-1"}},{"action":"check_access","input":{"record":"synthetic-1"},"expect_status":[200,403]}]}'
```

Each call returns an assistant message with a `workflow_request` tool call. The next call for the same ID advances the workflow. Add `target_url` to send the tool call as an HTTP POST; when delivery fails, the next call retries the same step. The endpoint can be used without a target to inspect the generated action.

The tester consumes and discards target response bodies. It keeps no per-query result history. A direct agent retains its profile, workflow definition, and next step in memory so later calls can advance without resending them. Each agent has one current workflow position; requests replace that state rather than append a conversation. Restarting the process clears it.

## Fleet

The target receives one JSON event per workflow step. Set `target_url` to the full POST URL accepted by your adapter. All steps can go to the same endpoint and port; the target can route events by the `action` field.

```sh
curl -s http://127.0.0.1:3000/runs \
  -H 'content-type: application/json' \
  -d '{
    "target_url":"http://127.0.0.1:4000/events",
    "telemetry_url":"http://127.0.0.1:4000/metrics",
    "agents":1000,
    "concurrency":100,
    "arrival_rate":200,
    "think_ms":100,
    "jitter_ms":250,
    "timeout_ms":3000,
    "retries":2,
    "seed":42,
    "profiles":[
      {"team":"alpha","context":{"scope":"project-a"}},
      {"team":"beta","context":{"scope":"project-b"}}
    ],
    "workflow":[
      {"action":"lookup","input":{"resource":"synthetic-1"}},
      {"action":"read","input":{"resource":"synthetic-1"}},
      {"action":"authorize","input":{"resource":"synthetic-2"},"expect_status":[200,403]}
    ]
  }'
```

The response contains a `run_id` and `status_url`. Profiles rotate across agents; each agent runs all steps in order. `expect_status` sets accepted HTTP codes for a step; omitted means any 2xx. An unexpected status increments the attempt-level `expectation_failures` metric. Retryable 429/5xx responses can still succeed on a later attempt; a final mismatch ends that agent's workflow. The POST body sent to the target has this shape:

```json
{
  "workflow_id": "uuid",
  "workflow_step": 1,
  "action": "lookup",
  "agent": { "id": "run-id-1", "profile": { "team": "alpha", "context": { "scope": "project-a" } } },
  "input": { "resource": "synthetic-1" }
}
```

The caller defines every profile, workflow action, input, and expected status; the toolkit assigns no roles or policies. Use synthetic context and inputs. Go schedules agents as goroutines; `concurrency` is the total cap. The `workers` setting is accepted for compatibility with the earlier JavaScript server but is unused. Limits: 10,000 agents, 256 concurrent agents, 100 profiles, 32 steps, five retries, and one active run per process. Run a second process or host when generator CPU/network overhead skews the target measurements.

Fleet configurations are discarded when a run finishes. Run summaries retain counts, latency samples, and any configured target telemetry; they do not retain individual request or response bodies.

### Ports and connections

The generator sends requests to one configured target host and port. `concurrency` limits simultaneous workflow requests, not the number of destination hosts or ports. Go's HTTP client manages its connections; concurrent requests may use multiple connections to that same host, and HTTPS may use HTTP/2 if negotiated. A single listening port can accept them all. The target's server, proxy, and operating system may impose their own connection or request limits; monitor those alongside the generator metrics.

## Observe

```sh
watch -n 1 'curl -s http://127.0.0.1:3000/status'
curl -N http://127.0.0.1:3000/events
curl -s http://127.0.0.1:3000/metrics
curl -s http://127.0.0.1:3000/runs/RUN_ID
```

`/status` and `/runs/:id` report completed and failed agents, in-flight requests, retries, timeouts, throttling, server errors, expectation failures, and client-observed average/p95 latency. `/events` streams request and agent events; `/metrics` emits Prometheus text. If `telemetry_url` is set, the tester samples that GET endpoint once per second and includes its JSON or text response as `target_telemetry` in `/status` and `/runs/:id`. Point it at a safe target exporter for system metrics relevant to your workload. Target internals cannot be inferred from HTTP responses alone.

## Check

```sh
go test -race -coverprofile=/tmp/janky.cover ./...
go tool cover -func=/tmp/janky.cover | awk '$1 == "total:" && $3 == "100.0%" { ok = 1 } END { exit !ok }'
node --test --experimental-test-coverage --test-coverage-exclude=sdk.test.mjs \
  --test-coverage-lines=100 --test-coverage-branches=100 \
  --test-coverage-functions=100 sdk.test.mjs
```
