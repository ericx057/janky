# Agent workflow stress tester

A dependency-free HTTP toolkit for replaying agent-shaped workflows against a caller-selected HTTP endpoint. It uses state machines, not an LLM. Agents keep a workflow identity, carry caller-defined context, pause between steps, retry throttled or failed requests, and issue real HTTP requests. A fleet uses virtual agents in one Node process with bounded concurrency.

## Run

```sh
node cli.mjs 3000
```

The control API listens on the loopback port passed to `cli.mjs`. Outbound targets are restricted to `127.0.0.1`, `localhost`, and `::1` by default. To test another host, set `ALLOWED_TARGET_HOSTS=api.example.test`. Treat the control API as trusted local tooling if embedding `createApp()` in another server.

## Single agent

```sh
curl -s http://127.0.0.1:3000/agents/example/invoke \
  -H 'content-type: application/json' \
  -d '{"profile":{"team":"alpha","context":{"task":"sample"}},"workflow":[{"action":"lookup","input":{"record":"synthetic-1"}},{"action":"check_access","input":{"record":"synthetic-1"},"expect_status":[200,403]}]}'
```

Each call returns an assistant message with a `workflow_request` tool call. The next call for the same ID advances the workflow. Add `target_url` to send the tool call as an HTTP POST; when delivery fails, the next call retries the same step. The endpoint can be used without a target to inspect the generated action.

## Fleet

The target receives one JSON event per workflow step. Set `target_url` to the full POST URL accepted by your adapter.

```sh
curl -s http://127.0.0.1:3000/runs \
  -H 'content-type: application/json' \
  -d '{
    "target_url":"http://127.0.0.1:4000/events",
    "telemetry_url":"http://127.0.0.1:4000/metrics",
    "agents":1000,
    "concurrency":100,
    "workers":4,
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

The caller defines every profile, workflow action, input, and expected status; the toolkit assigns no roles or policies. Use synthetic context and inputs. `workers` splits a fleet across Node worker threads while keeping the configured `concurrency` as the total cap; fleets of at least 1,000 agents use up to four workers by default. Limits: 10,000 agents, 256 concurrent agents, eight workers, 100 profiles, 32 steps, five retries, and one active run per process. Run a second process or host when generator CPU/network overhead skews the target measurements.

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
node --test --experimental-test-coverage --test-coverage-exclude=test.mjs \
  --test-coverage-lines=100 --test-coverage-branches=100 \
  --test-coverage-functions=100 test.mjs
```
