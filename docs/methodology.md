# Methodology

Janky measures an HTTP service from the client side by running scripted workflows and recording each request attempt. It does not infer workload intent or create actions: the caller specifies workflow steps, payloads, profiles, expected responses, and pacing.

## Define a workload

Each fleet run includes a target URL, number of agents, concurrency cap, arrival rate, optional profiles, and ordered workflow. Profiles rotate across agents. Every agent attempts each workflow step in order and stops at its first final failure.

Each target POST contains a workflow ID, one-based step number, action, agent ID and profile, and the step input. All workflow steps can use the same endpoint; the target adapter can route by `action`.

## Pace and bound traffic

- `agents` sets total virtual agents (1–10,000).
- `concurrency` caps agents running at once (1–256); it is not a destination host or port count.
- `arrival_rate` schedules agent starts per second (1–10,000).
- `think_ms` and `jitter_ms` introduce a pause between sequential steps; jitter also spreads arrival times.
- `timeout_ms` bounds an HTTP request attempt.

Fleet work runs in one process. The configured concurrency is a generator-side cap; target, proxy, OS, and network limits may be lower. Start conservatively and monitor both sides.

## Expected statuses and retries

Without `expect_status`, any 2xx response succeeds. A step can list one or more expected status codes from 100 to 599, which is useful when a response such as 403 is an expected workflow outcome. Other codes count as expectation failures.

The configured retry count is the maximum number of retries after an initial attempt. Network errors, 429, and 5xx responses can be retried; other unexpected HTTP statuses stop the step. Janky honors a numeric or HTTP-date `Retry-After` value, bounded to one second, and otherwise uses increasing short delays. Metrics count attempts, so retries also affect request totals and latency measurements.

## Observe results

`/runs/{id}` and `/status` include agent counts and request counters, including success/failure, throttling, server errors, timeouts, retries, expectation failures, average latency, and client-observed p95 latency. `/events` streams request and agent lifecycle events. `/metrics` emits Prometheus text.

An optional `telemetry_url` is sampled once per second during a fleet run. It must be on an allowed target host; responses up to 16 KiB are retained in run/status snapshots as JSON or text. This is supplied target telemetry, not an estimate of target internals.

The p50/p95/p99 and heap sampling functions are standalone helpers. They are not wired into request execution or per-agent runtime reporting. Runtime request metrics retain bounded latency samples for average and p95 reporting; individual target response bodies are discarded.

## Interpret results

Use the observed request rate, latency, error mix, retry count, target telemetry, and target-side logs together. Client latency includes network and service time as observed by Janky. It cannot distinguish queueing, application work, or downstream delays without target-side instrumentation. Repeat runs with the same seed and settings to compare changes; keep runs small enough that the generator itself does not dominate.
