# Use cases

Janky helps exercise an HTTP service through sequences that resemble multi-step agent activity. Each virtual agent runs the same workflow with a selected profile, and each step is sent to the configured target endpoint.

## Useful for

- **Capacity checks:** increase agent count, concurrency, or arrival rate to find when throughput, latency, throttling, or errors change.
- **Workflow integration checks:** confirm an adapter accepts actions and inputs in sequence, including expected non-2xx outcomes such as authorization denials.
- **Retry behavior:** see how an endpoint handles repeated requests after 429 or 5xx responses, and whether `Retry-After` is observed.
- **Pacing experiments:** vary think time and jitter to approximate bursts or less synchronized clients.
- **Target monitoring:** collect an optional target telemetry endpoint alongside client-observed request metrics.
- **Local development:** run a small reproducible workload against a loopback service before testing a shared environment.

The action names and input payloads are yours to define. A target adapter can route all steps through one URL and port using the `action` field. Use synthetic profiles and data when exercising services that contain real user information.

## Where it is useful

Use it for local adapters, staging services, test deployments, and controlled performance experiments where you can safely generate traffic. For a production test, coordinate the workload with service owners and choose limits appropriate for the target.

Janky is not an agent simulator with autonomous reasoning, a browser automation system, or a distributed load generator. It does not model token usage, tool selection, user sessions, or target internals. A single process can itself become the bottleneck; run more generators or hosts if its CPU or network use distorts measurements.

## Example scenarios

1. **Read path:** lookup a synthetic record, then fetch its details.
2. **Authorization path:** request a resource and accept either 200 or 403 for the authorization step.
3. **Rate limit response:** send at a chosen arrival rate and observe 429 counts and retries.
4. **Profile variation:** rotate a small set of synthetic tenant or workload profiles across agents.
