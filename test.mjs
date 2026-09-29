import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import { createServer } from 'node:http';
import { Worker } from 'node:worker_threads';
import { createApp } from './server.mjs';

const servers = [];
after(() => Promise.all(servers.map(server => new Promise(resolve => server.close(resolve)))));

async function listen(server) {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  servers.push(server);
  return `http://127.0.0.1:${server.address().port}`;
}

async function post(url, body) {
  return fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function waitFor(url, predicate, attempts = 100) {
  for (let attempt = 0; attempt < attempts; attempt++) {
    const value = await (await fetch(url)).json();
    if (predicate(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error('run did not finish');
}

test('one virtual agent keeps identity and emits a caller-defined workflow', async () => {
  const received = [];
  const target = await listen(createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    received.push(JSON.parse(body));
    res.writeHead(received.at(-1).action === 'review' ? 403 : 200).end('{}');
  }));
  const app = await listen(createApp());
  const workflow = [
    { action: 'search', input: { type: 'record' } },
    { action: 'read', input: { id: 'sample-1' } },
    { action: 'update', input: { id: 'sample-1' } },
    { action: 'review', input: { id: 'sample-2' }, expect_status: [403] },
  ];

  const actions = [];
  for (let i = 0; i < 4; i++) {
    const response = await post(`${app}/agents/alice/invoke`, {
      target_url: target,
      profile: { team: 'alpha', context: { tenant: 'north', role: 'analyst' } },
      workflow,
    });
    assert.equal(response.status, 200);
    actions.push(await response.json());
  }

  assert.deepEqual(received.map(event => event.action), workflow.map(step => step.action));
  assert.deepEqual(received.map(event => event.workflow_step), [1, 2, 3, 4]);
  assert.ok(received.every(event => event.agent.id === 'alice' && event.agent.profile.team === 'alpha'));
  assert.ok(received.every(event => event.agent.profile.context.tenant === 'north'));
  assert.deepEqual(received[0].input, { type: 'record' });
  assert.equal(new Set(received.map(event => event.workflow_id)).size, 1);
  assert.ok(actions.every(result => result.message.role === 'assistant' && result.message.tool_calls.length === 1));
  assert.equal(actions[0].message.tool_calls[0].function.name, 'workflow_request');
  assert.equal(actions.at(-1).delivered, true);
});

test('fleet bounds concurrent I/O, varies contexts, and exposes status and metrics', async () => {
  let active = 0;
  let peak = 0;
  const received = [];
  const target = await listen(createServer(async (req, res) => {
    active++;
    peak = Math.max(peak, active);
    let body = '';
    for await (const chunk of req) body += chunk;
    const event = JSON.parse(body);
    received.push(event);
    await new Promise(resolve => setTimeout(resolve, 5));
    active--;
    res.writeHead(event.action === 'review' ? 403 : 200).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target,
    profiles: [{ team: 'alpha', context: { scope: 'one' } }, { team: 'beta', context: { scope: 'two' } }],
    workflow: [
      { action: 'search' }, { action: 'read' }, { action: 'update' },
      { action: 'review', expect_status: [403] },
    ],
    agents: 12,
    concurrency: 3,
    arrival_rate: 1000,
    think_ms: 1,
    jitter_ms: 2,
    seed: 7,
  });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.state, 'completed', run.error);
  assert.equal(run.agents.completed, 12);
  assert.equal(run.agents.failed, 0);
  assert.equal(run.requests.total, 48);
  assert.equal(run.requests.expected_non_2xx, 12);
  assert.ok(peak <= 3);
  assert.equal(new Set(received.map(event => event.agent.id)).size, 12);
  assert.ok(new Set(received.map(event => event.agent.profile.team)).size > 1);
  const metrics = await (await fetch(`${app}/metrics`)).text();
  assert.match(metrics, /agent_stress_requests_total 48/);
  assert.match(metrics, /agent_stress_agents_completed_total 12/);
});

test('an unexpected status is reported as an expectation failure', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{"decision":"allow"}')));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target, agents: 1, concurrency: 1, arrival_rate: 100,
    workflow: [{ action: 'custom_check', expect_status: [403] }],
    think_ms: 0, jitter_ms: 0, retries: 0,
  });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.agents.failed, 1);
  assert.equal(run.requests.expectation_failures, 1);
});

test('429 retries are bounded and visible', async () => {
  let calls = 0;
  const target = await listen(createServer((_req, res) => {
    calls++;
    res.writeHead(429, { 'retry-after': '0' }).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target,
    agents: 1,
    concurrency: 1,
    arrival_rate: 100,
    think_ms: 0,
    jitter_ms: 0,
    retries: 2,
  });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(calls, 3);
  assert.equal(run.agents.failed, 1);
  assert.equal(run.requests.throttled, 3);
  assert.equal(run.requests.retries, 2);
});

test('invalid and disallowed targets fail before traffic', async () => {
  const app = await listen(createApp());
  for (const body of [
    { target_url: 'file:///tmp/x', agents: 1 },
    { target_url: 'http://example.com', agents: 1 },
    { target_url: 'http://127.0.0.1:9999', agents: 0 },
    { target_url: 'http://127.0.0.1:9999', concurrency: 0 },
    { target_url: 'http://127.0.0.1:9999', arrival_rate: -1 },
  ]) {
    const response = await post(`${app}/runs`, body);
    assert.equal(response.status, 400);
  }
});

test('redirects do not forward synthetic events', async () => {
  let forwarded = 0;
  const destination = await listen(createServer((_req, res) => { forwarded++; res.end(); }));
  const target = await listen(createServer((_req, res) => {
    res.writeHead(307, { location: destination }).end();
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/agents/alice/invoke`, { target_url: target });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).delivered, false);
  assert.equal(forwarded, 0);
});

test('invalid invocation settings do not advance the agent', async () => {
  const app = await listen(createApp());
  const invalid = await post(`${app}/agents/bob/invoke`, { retries: -1 });
  assert.equal(invalid.status, 400);
  const valid = await post(`${app}/agents/bob/invoke`, {});
  assert.equal((await valid.json()).step, 1);
});

test('optional target telemetry appears beside client load metrics', async () => {
  const target = await listen(createServer((_req, res) => {
    res.writeHead(200).end('{}');
  }));
  const telemetry = await listen(createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' }).end('{"db":{"connections":12,"queue_depth":3}}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target, telemetry_url: telemetry, agents: 1,
    arrival_rate: 100, think_ms: 0, jitter_ms: 0,
  });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running' && value.target_telemetry);
  assert.deepEqual(run.target_telemetry.data.db, { connections: 12, queue_depth: 3 });
  assert.equal(run.requests.total, 1);
});

test('a slow target times out and a second active run is rejected', async () => {
  const target = await listen(createServer(async (_req, res) => {
    await new Promise(resolve => setTimeout(resolve, 100));
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const config = { target_url: target, agents: 1, timeout_ms: 10, retries: 0,
    think_ms: 0, jitter_ms: 0 };
  const first = await post(`${app}/runs`, config);
  const { run_id } = await first.json();
  assert.equal((await post(`${app}/runs`, config)).status, 409);
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.agents.failed, 1);
  assert.equal(run.requests.timeouts, 1);
});

test('a larger virtual fleet completes without exceeding its concurrency cap', async () => {
  let active = 0;
  let peak = 0;
  const target = await listen(createServer(async (_req, res) => {
    active++;
    peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 1));
    active--;
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target, agents: 300, concurrency: 40, arrival_rate: 10000,
    think_ms: 0, jitter_ms: 0, retries: 0,
  });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.agents.completed, 300);
  assert.equal(run.requests.total, 300);
  assert.ok(peak <= 40);
});

test('concurrent calls to one agent do not duplicate a workflow step', async () => {
  let started;
  const seen = new Promise(resolve => { started = resolve; });
  const target = await listen(createServer(async (_req, res) => {
    started();
    await new Promise(resolve => setTimeout(resolve, 40));
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const input = { target_url: target, workflow: [{ action: 'first' }, { action: 'second' }] };
  const first = post(`${app}/agents/alice/invoke`, input);
  await seen;
  assert.equal((await post(`${app}/agents/alice/invoke`, input)).status, 409);
  assert.equal((await (await first).json()).step, 1);
  assert.equal((await (await post(`${app}/agents/alice/invoke`, input)).json()).step, 2);
});

test('a direct agent call and a fleet run cannot stack their load', async () => {
  let started;
  const seen = new Promise(resolve => { started = resolve; });
  const target = await listen(createServer(async (_req, res) => {
    started();
    await new Promise(resolve => setTimeout(resolve, 40));
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const direct = post(`${app}/agents/alice/invoke`, { target_url: target });
  await seen;
  assert.equal((await post(`${app}/runs`, { target_url: target })).status, 409);
  await direct;
});

test('control POSTs require an application/json content type', async () => {
  const app = await listen(createApp());
  const response = await fetch(`${app}/runs`, { method: 'POST',
    headers: { 'content-type': 'text/plain' }, body: '{}' });
  assert.equal(response.status, 400);
});

test('invalid profiles, workflow steps, URLs, and JSON bodies are rejected', async () => {
  const app = await listen(createApp());
  const target_url = 'http://127.0.0.1:9999';
  for (const body of [
    { target_url: 'not a URL' },
    { target_url: 'http://user:pass@127.0.0.1:9999' },
    { target_url: `${target_url}/#fragment` },
    { target_url, profiles: [] },
    { target_url, profiles: [null] },
    { target_url, profiles: Array.from({ length: 101 }, () => ({})) },
    { target_url, workflow: [] },
    { target_url, workflow: Array.from({ length: 33 }, () => ({ action: 'read' })) },
    { target_url, workflow: [{}] },
    { target_url, workflow: [{ action: 'a b' }] },
    { target_url, workflow: [{ action: 'read', input: [] }] },
    { target_url, workflow: [{ action: 'read', expect_status: [] }] },
    { target_url, workflow: [{ action: 'read', expect_status: [600] }] },
    { target_url, workflow: [{ action: 'read', expect_status: ['200'] }] },
    { target_url, timeout_ms: 0 },
    { target_url, seed: -1 },
    { target_url, workers: 0 },
    { target_url, workers: 9 },
  ]) {
    const response = await post(`${app}/runs`, body);
    assert.equal(response.status, 400, JSON.stringify(body));
  }
  for (const body of ['[]', 'null', '{', '"text"', ' '.repeat(65537)]) {
    const response = await fetch(`${app}/runs`, { method: 'POST',
      headers: { 'content-type': 'application/json' }, body });
    assert.equal(response.status, 400);
  }
});

test('transient errors retry, while a non-retryable response stops immediately', async () => {
  const calls = new Map();
  const target = await listen(createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    const action = JSON.parse(body).action;
    const count = (calls.get(action) ?? 0) + 1;
    calls.set(action, count);
    if (count > 1) return res.writeHead(200).end('{}');
    if (action === 'server_error') return res.writeHead(503).end('{}');
    if (action === 'bad_retry_after') return res.writeHead(429, { 'retry-after': 'invalid' }).end('{}');
    if (action === 'date_retry_after') return res.writeHead(429,
      { 'retry-after': new Date(Date.now() - 10000).toUTCString() }).end('{}');
    return res.writeHead(400).end('{}');
  }));
  const app = await listen(createApp());
  for (const action of ['server_error', 'bad_retry_after', 'date_retry_after', 'client_error']) {
    const response = await post(`${app}/agents/${action}/invoke`, {
      target_url: target, workflow: [{ action }], retries: 1,
    });
    assert.equal(response.status, 200);
    assert.equal((await response.json()).delivered, action !== 'client_error');
  }
  assert.deepEqual([...calls], [
    ['server_error', 2], ['bad_retry_after', 2], ['date_retry_after', 2], ['client_error', 1],
  ]);
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.requests.retries, 3);
  assert.equal(status.requests.server_errors, 1);
  assert.equal(status.requests.throttled, 2);
  assert.equal(status.requests.expectation_failures, 4);
});

test('status, event stream, and unknown routes are usable from a terminal', async () => {
  const app = await listen(createApp());
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  assert.deepEqual((await (await fetch(`${app}/status`)).json()).agents,
    { started: 0, completed: 0, failed: 0 });
  assert.equal((await fetch(`${app}/runs/00000000-0000-0000-0000-000000000000`)).status, 404);
  assert.equal((await fetch(`${app}/missing`)).status, 404);

  const stream = new AbortController();
  const timeout = setTimeout(() => stream.abort(), 2000);
  const events = await fetch(`${app}/events`, { signal: stream.signal });
  assert.equal(events.status, 200);
  assert.match(events.headers.get('content-type'), /text\/event-stream/);
  const reader = events.body.getReader();
  try {
    assert.match(new TextDecoder().decode((await reader.read()).value), /connected/);
    await post(`${app}/agents/observer/invoke`, { target_url: target });
    let message = '';
    while (!message.includes('"type":"request"')) {
      const next = await reader.read();
      assert.equal(next.done, false);
      message += new TextDecoder().decode(next.value);
    }
    assert.match(message, /"type":"request"/);
  } finally {
    clearTimeout(timeout);
    stream.abort();
  }
});

test('target telemetry accepts text and reports invalid JSON as an error', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const telemetry = await listen(createServer((req, res) => {
    if (req.url === '/text') return res.writeHead(200, { 'content-type': 'text/plain' }).end('queue 4');
    return res.writeHead(200, { 'content-type': 'application/json' }).end('{invalid');
  }));
  const app = await listen(createApp());
  for (const [path, check] of [
    ['/text', item => assert.equal(item.data, 'queue 4')],
    ['/invalid', item => assert.equal(typeof item.error, 'string')],
  ]) {
    const response = await post(`${app}/runs`, { target_url: target, telemetry_url: telemetry + path,
      agents: 1, arrival_rate: 1000, think_ms: 0, jitter_ms: 0 });
    assert.equal(response.status, 202);
    const { run_id } = await response.json();
    const run = await waitFor(`${app}/runs/${run_id}`,
      value => value.state !== 'running' && value.target_telemetry);
    check(run.target_telemetry);
  }
});

test('delivery waits for a delayed response body without aborting the target', async () => {
  let bodyFinished = false;
  let targetAborted = false;
  let targetClosed;
  const closed = new Promise(resolve => { targetClosed = resolve; });
  const target = await listen(createServer((_req, res) => {
    res.on('close', () => { targetAborted = !res.writableEnded; targetClosed(); });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.flushHeaders();
    setTimeout(() => {
      if (!res.destroyed) {
        res.end('{"result":"ok"}');
        bodyFinished = true;
      }
    }, 60);
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/agents/delayed/invoke`, { target_url: target });
  assert.equal((await response.json()).delivered, true);
  await closed;
  assert.equal(bodyFinished, true);
  assert.equal(targetAborted, false);
  const status = await (await fetch(`${app}/status`)).json();
  assert.ok(status.requests.latency_avg_ms >= 50);
});

test('a response that stalls after headers fails the agent on timeout', async () => {
  const target = await listen(createServer((_req, res) => {
    res.writeHead(200);
    res.flushHeaders();
    setTimeout(() => res.end('{}'), 100);
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/agents/body_timeout/invoke`, {
    target_url: target, timeout_ms: 10, retries: 0,
  });
  assert.equal((await response.json()).delivered, false);
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.requests.succeeded, 0);
  assert.equal(status.requests.timeouts, 1);
});

test('event observer limit returns a clear error', async () => {
  const app = await listen(createApp());
  const observers = Array.from({ length: 32 }, () => new AbortController());
  try {
    const streams = await Promise.all(observers.map(observer =>
      fetch(`${app}/events`, { signal: observer.signal })));
    assert.ok(streams.every(stream => stream.status === 200));
    const overflow = await fetch(`${app}/events`);
    assert.equal(overflow.status, 503);
    assert.match((await overflow.json()).error, /observers/);
  } finally {
    observers.forEach(observer => observer.abort());
  }
});

test('completed run history keeps the newest 100 runs', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const app = await listen(createApp());
  const ids = [];
  for (let index = 0; index < 101; index++) {
    const response = await post(`${app}/runs`, { target_url: target, agents: 1,
      arrival_rate: 10000, think_ms: 0, jitter_ms: 0, retries: 0 });
    assert.equal(response.status, 202);
    const { run_id } = await response.json();
    ids.push(run_id);
    const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
    assert.equal(run.agents.completed, 1);
  }
  assert.equal((await fetch(`${app}/runs/${ids[0]}`)).status, 404);
  assert.equal((await fetch(`${app}/runs/${ids.at(-1)}`)).status, 200);
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.runs.length, 100);
});

test('two workers preserve agent workflows and the shared concurrency cap', async () => {
  let active = 0;
  let peak = 0;
  const received = [];
  const target = await listen(createServer(async (req, res) => {
    active++;
    peak = Math.max(peak, active);
    let body = '';
    for await (const chunk of req) body += chunk;
    received.push(JSON.parse(body));
    await new Promise(resolve => setTimeout(resolve, 10));
    active--;
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, {
    target_url: target, workers: 2, agents: 20, concurrency: 4,
    arrival_rate: 1000, think_ms: 0, jitter_ms: 0, retries: 0,
    profiles: [{ team: 'one' }, { team: 'two' }],
    workflow: [{ action: 'read' }, { action: 'write' }],
  });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.state, 'completed', run.error);
  assert.deepEqual(run.agents, { started: 20, completed: 20, failed: 0 });
  assert.equal(run.requests.total, 40);
  assert.equal(run.requests.succeeded, 40);
  assert.equal(run.requests.in_flight, 0);
  assert.ok(peak <= 4);
  assert.equal(received.length, 40);
  const agentIds = new Set(received.map(event => event.agent.id));
  assert.equal(agentIds.size, 20);
  for (const id of agentIds) {
    const events = received.filter(event => event.agent.id === id);
    assert.deepEqual(events.map(event => event.action), ['read', 'write']);
    assert.equal(new Set(events.map(event => event.workflow_id)).size, 1);
  }
  assert.deepEqual(new Set(received.map(event => event.agent.profile.team)), new Set(['one', 'two']));
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.agents.completed, 20);
  assert.equal(status.requests.total, 40);
});

test('latency metrics remain bounded after more than 10,000 attempts', async () => {
  let received = 0;
  const target = await listen(createServer(async (req, res) => {
    for await (const _chunk of req) {}
    received++;
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const observer = new AbortController();
  const stream = await fetch(`${app}/events`, { signal: observer.signal });
  assert.equal(stream.status, 200);
  const response = await post(`${app}/runs`, {
    target_url: target, agents: 313, concurrency: 128, workers: 1,
    arrival_rate: 10000, think_ms: 0, jitter_ms: 0, retries: 0,
    workflow: Array.from({ length: 32 }, (_, index) => ({ action: `step_${index}` })),
  });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running', 3000);
  assert.equal(run.state, 'completed', run.error);
  assert.equal(run.agents.completed, 313);
  assert.equal(run.requests.total, 10016);
  assert.equal(received, 10016);
  assert.ok(run.requests.latency_p95_ms >= 0);
  observer.abort();
});

test('direct invocation cap rejects request 257 while 256 are active', async () => {
  let entered = 0;
  let release;
  let allEntered;
  const held = new Promise(resolve => { release = resolve; });
  const ready = new Promise(resolve => { allEntered = resolve; });
  const target = await listen(createServer(async (_req, res) => {
    entered++;
    if (entered === 256) allEntered();
    await held;
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const pending = Array.from({ length: 256 }, (_, index) =>
    post(`${app}/agents/cap_${index}/invoke`, { target_url: target, timeout_ms: 15000, retries: 0 }));
  let timer;
  try {
    await Promise.race([ready, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`only ${entered} requests reached the target`)), 10000);
    })]);
    const overflow = await post(`${app}/agents/cap_overflow/invoke`, { target_url: target });
    assert.equal(overflow.status, 429);
  } finally {
    clearTimeout(timer);
    release();
  }
  const responses = await Promise.all(pending);
  assert.ok(responses.every(response => response.status === 200));
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.requests.total, 256);
});

test('slow telemetry samples do not overlap', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  let active = 0;
  let peak = 0;
  const telemetry = await listen(createServer(async (_req, res) => {
    active++;
    peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 1500));
    active--;
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ready');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, { target_url: target, telemetry_url: telemetry,
    agents: 3, arrival_rate: 1, think_ms: 0, jitter_ms: 0, retries: 0 });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running', 400);
  assert.equal(run.state, 'completed', run.error);
  assert.equal(peak, 1);
  assert.equal(run.target_telemetry.data, 'ready');
});

test('CLI starts a local control server', async () => {
  const previous = process.argv[2];
  process.argv[2] = '0';
  let server;
  try {
    ({ server } = await import('./cli.mjs'));
    if (!server.listening) await new Promise(resolve => server.once('listening', resolve));
    const response = await fetch(`http://127.0.0.1:${server.address().port}/health`);
    assert.equal(response.status, 200);
  } finally {
    process.argv[2] = previous;
    if (server) await new Promise(resolve => server.close(resolve));
  }
});

test('an unprocessable workflow fails the run without taking down the control API', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const app = await listen(createApp());
  const deep = '{"x":'.repeat(7000) + '0' + '}'.repeat(7000);
  for (const workers of [1, 2]) {
    const body = `{"target_url":${JSON.stringify(target)},"agents":2,"concurrency":${workers},` +
      `"workers":${workers},"arrival_rate":1000,"think_ms":0,"jitter_ms":0,` +
      `"workflow":[{"action":"deep","input":${deep}}]}`;
    const response = await fetch(`${app}/runs`, { method: 'POST',
      headers: { 'content-type': 'application/json' }, body });
    assert.equal(response.status, 202);
    const { run_id } = await response.json();
    const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running', 400);
    assert.equal(run.state, 'failed');
    assert.equal(typeof run.error, 'string');
    assert.equal((await fetch(`${app}/health`)).status, 200);
  }
});

test('worker retries appear once in run and global metrics', async () => {
  const attempts = new Map();
  const target = await listen(createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    const id = JSON.parse(body).agent.id;
    const count = (attempts.get(id) ?? 0) + 1;
    attempts.set(id, count);
    res.writeHead(count === 1 ? 429 : 200, { 'retry-after': '0' }).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, { target_url: target, agents: 4,
    workers: 2, concurrency: 2, arrival_rate: 1000, think_ms: 0, jitter_ms: 0, retries: 1 });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.state, 'completed', run.error);
  assert.equal(run.agents.completed, 4);
  assert.equal(run.requests.total, 8);
  assert.equal(run.requests.throttled, 4);
  assert.equal(run.requests.retries, 4);
  assert.deepEqual(new Set(attempts.values()), new Set([2]));
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.requests.total, 8);
  assert.equal(status.requests.retries, 4);
});

test('empty responses and connection failures are handled without a response body', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(204).end()));
  const app = await listen(createApp());
  const delivered = await post(`${app}/agents/empty/invoke`, { target_url: target });
  assert.equal((await delivered.json()).delivered, true);
  const response = await post(`${app}/runs`, {
    target_url: target, telemetry_url: target, agents: 1,
    arrival_rate: 1000, think_ms: 0, jitter_ms: 0, retries: 0,
  });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.state, 'completed', run.error);
  assert.equal(run.target_telemetry.data, '');

  const closed = createServer();
  const unreachable = await listen(closed);
  await new Promise(resolve => closed.close(resolve));
  servers.splice(servers.indexOf(closed), 1);
  const failed = await post(`${app}/agents/unreachable/invoke`, {
    target_url: unreachable, retries: 1, timeout_ms: 100,
  });
  assert.equal((await failed.json()).delivered, false);
  const status = await (await fetch(`${app}/status`)).json();
  assert.equal(status.requests.retries, 1);
});

test('oversized target telemetry is reported and environment host allowlist is used', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const telemetry = await listen(createServer((_req, res) => res.writeHead(200).end('x'.repeat(17000))));
  const previous = process.env.ALLOWED_TARGET_HOSTS;
  process.env.ALLOWED_TARGET_HOSTS = '127.0.0.1';
  const app = await listen(createApp());
  if (previous === undefined) delete process.env.ALLOWED_TARGET_HOSTS;
  else process.env.ALLOWED_TARGET_HOSTS = previous;
  const response = await post(`${app}/runs`, { target_url: target, telemetry_url: telemetry,
    agents: 1, arrival_rate: 1000, think_ms: 0, jitter_ms: 0 });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.match(run.target_telemetry.error, /16 KiB/);
  assert.equal((await post(`${app}/runs`, { target_url: 'http://localhost:9999' })).status, 400);
});

test('direct agent state evicts the oldest ID after 10,000 agents', async () => {
  const app = await listen(createApp());
  const workflow = [{ action: 'first' }, { action: 'second' }];
  const first = await post(`${app}/agents/oldest/invoke`, { workflow });
  assert.equal((await first.json()).step, 1);
  for (let start = 0; start < 10000; start += 250) {
    const responses = await Promise.all(Array.from({ length: 250 }, (_, offset) =>
      post(`${app}/agents/agent_${start + offset}/invoke`, {})));
    assert.ok(responses.every(response => response.status === 200));
    await Promise.all(responses.map(response => response.arrayBuffer()));
  }
  const again = await post(`${app}/agents/oldest/invoke`, { workflow });
  assert.equal((await again.json()).step, 1);
});

test('default large-fleet worker count and uneven worker quotas complete', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const app = await listen(createApp());
  for (const config of [
    { agents: 1000, concurrency: 32 },
    { agents: 9, concurrency: 4, workers: 3 },
  ]) {
    const response = await post(`${app}/runs`, { target_url: target, ...config,
      arrival_rate: 10000, think_ms: 0, jitter_ms: 0, retries: 0 });
    const { run_id } = await response.json();
    const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running', 500);
    assert.equal(run.state, 'completed', run.error);
    assert.equal(run.agents.completed, config.agents);
    assert.equal(run.requests.total, config.agents);
  }
});

test('worker timeout accounting reaches run and global metrics', async () => {
  const target = await listen(createServer(async (_req, res) => {
    await new Promise(resolve => setTimeout(resolve, 100));
    res.writeHead(200).end('{}');
  }));
  const app = await listen(createApp());
  const response = await post(`${app}/runs`, { target_url: target, agents: 2,
    workers: 2, concurrency: 2, arrival_rate: 1000, think_ms: 0,
    jitter_ms: 0, timeout_ms: 10, retries: 0 });
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.agents.failed, 2);
  assert.equal(run.requests.timeouts, 2);
  assert.equal((await (await fetch(`${app}/status`)).json()).requests.timeouts, 2);
});

test('a deep direct invocation returns an internal error without advancing state', async () => {
  const app = await listen(createApp());
  const deep = '{"x":'.repeat(7000) + '0' + '}'.repeat(7000);
  const body = `{"workflow":[{"action":"deep","input":${deep}}]}`;
  const failed = await fetch(`${app}/agents/deep/invoke`, { method: 'POST',
    headers: { 'content-type': 'application/json' }, body });
  assert.equal(failed.status, 500);
  const next = await post(`${app}/agents/deep/invoke`, {
    workflow: [{ action: 'first' }, { action: 'second' }],
  });
  assert.equal((await next.json()).step, 1);
});

test('an agent reuses its workflow when later calls omit it', async () => {
  const app = await listen(createApp());
  const first = await post(`${app}/agents/reuse/invoke`, {
    workflow: [{ action: 'one' }, { action: 'two' }],
  });
  assert.equal((await first.json()).step, 1);
  const second = await post(`${app}/agents/reuse/invoke`, {});
  const result = await second.json();
  assert.equal(result.step, 2);
  assert.equal(JSON.parse(result.message.tool_calls[0].function.arguments).action, 'two');
});

test('a worker module without fleet data exits cleanly', async () => {
  const worker = new Worker(new URL('./server.mjs', import.meta.url));
  const code = await new Promise((resolve, reject) => {
    worker.once('exit', resolve);
    worker.once('error', reject);
  });
  assert.equal(code, 0);
});

test('a worker startup failure fails the run and leaves the control API responsive', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const app = await listen(createApp({ workerScript: new URL('./worker-failure.cjs', import.meta.url) }));
  const response = await post(`${app}/runs`, { target_url: target,
    agents: 4, workers: 2, concurrency: 2, arrival_rate: 1000,
    think_ms: 0, jitter_ms: 0 });
  assert.equal(response.status, 202);
  const { run_id } = await response.json();
  const run = await waitFor(`${app}/runs/${run_id}`, value => value.state !== 'running');
  assert.equal(run.state, 'failed');
  assert.equal((await fetch(`${app}/health`)).status, 200);
});
