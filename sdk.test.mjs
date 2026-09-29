import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import { createServer } from 'node:http';
import { spawn, execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { createClient } from './sdk.mjs';

const servers = [];
let backend;
let buildDir;
let backendUrl;
after(async () => {
  await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve))));
  if (backend && backend.exitCode === null && backend.signalCode === null) {
    backend.kill();
    await new Promise(resolve => backend.once('exit', resolve));
  }
  if (buildDir) await rm(buildDir, { recursive: true, force: true });
});

async function listen(server) {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  servers.push(server);
  return `http://127.0.0.1:${server.address().port}`;
}

async function listenBackend() {
  if (backendUrl) return backendUrl;
  buildDir = await mkdtemp(join(tmpdir(), 'janky-sdk-'));
  const binary = join(buildDir, 'server');
  await promisify(execFile)('go', ['build', '-o', binary, '.']);
  const reservation = createServer();
  await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  backend = spawn(binary, [String(port)], { stdio: 'ignore' });
  backendUrl = `http://127.0.0.1:${port}`;
  for (let attempt = 0; attempt < 200; attempt++) {
    if (backend.exitCode !== null) throw new Error('Go backend exited during startup');
    try {
      const response = await fetch(`${backendUrl}/health`, { signal: AbortSignal.timeout(500) });
      if (response.ok) return backendUrl;
    } catch { /* Wait for the listener. */ }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('Go backend did not start');
}

test('SDK invokes agents and reads fleet results and metrics', async () => {
  const target = await listen(createServer((_req, res) => res.writeHead(200).end('{}')));
  const client = createClient(await listenBackend());

  assert.deepEqual(await client.health(), { ok: true });
  const agent = await client.invokeAgent('alice', {
    target_url: target, workflow: [{ action: 'lookup', input: { id: 'sample-1' } }],
  });
  assert.equal(agent.agent_id, 'alice');
  assert.equal(agent.delivered, true);

  const { run_id } = await client.startRun({ target_url: target, agents: 1,
    workflow: [{ action: 'lookup' }], think_ms: 0, jitter_ms: 0 });
  let run;
  for (let attempt = 0; attempt < 100; attempt++) {
    run = await client.getRun(run_id);
    if (run.state !== 'running') break;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.equal(run.state, 'completed', run.error);
  assert.equal(run.agents.completed, 1);
  assert.equal((await client.getStatus()).requests.total, 2);
  assert.match(await client.getMetrics(), /agent_stress_requests_total 2/);
});

test('SDK rejects invalid IDs and preserves API error status', async () => {
  assert.throws(() => createClient('file:///tmp/control'), /HTTP/);
  const client = createClient(await listenBackend());
  await assert.rejects(client.invokeAgent('bad/id'), /agent ID/);
  await assert.rejects(client.getRun('../bad'), /run ID/);
  await assert.rejects(client.getRun('deadbeef'), error => error.status === 404 &&
    error.message === 'run not found');
  await assert.rejects(client.startRun({ agents: 0 }), error => error.status === 400 &&
    /target_url/.test(error.message));
});

test('SDK yields streamed events across response chunks', async () => {
  const source = await listen(createServer((req, res) => {
    assert.equal(req.url, '/events');
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write(': connected\n\ndata: {"type":"request"');
    setImmediate(() => res.end(',"agent_id":"alice"}\n\n'));
  }));
  const received = [];
  for await (const event of createClient(source).events()) received.push(event);
  assert.deepEqual(received, [{ type: 'request', agent_id: 'alice' }]);
});

test('SDK unit: validates origins and sends requests with options', async () => {
  for (const url of [
    'ftp://localhost', 'http://user@localhost', 'http://:pass@localhost',
    'http://localhost/path', 'http://localhost/?q=1', 'http://localhost/#fragment',
  ]) assert.throws(() => createClient(url), /HTTP\(S\) server origin/);

  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options });
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'content-type': 'application/json' },
    });
  };
  try {
    const client = createClient();
    const signal = new AbortController().signal;
    assert.deepEqual(await client.health({ signal }), { ok: true });
    assert.deepEqual(await createClient('https://example.com').getStatus(), { ok: true });
    assert.deepEqual(await client.invokeAgent('a_1-Z'), { ok: true });
    assert.deepEqual(await client.startRun({ agents: 1 }), { ok: true });
    assert.deepEqual(await client.getRun('dead-beef'), { ok: true });
    await assert.rejects(client.getRun('../bad'), /invalid run ID/);
    await assert.rejects(client.invokeAgent('bad/id'), /invalid agent ID/);
    assert.deepEqual(calls.map(call => [call.url, call.options.method, call.options.body]), [
      ['http://127.0.0.1:3000/health', 'GET', undefined],
      ['https://example.com/status', 'GET', undefined],
      ['http://127.0.0.1:3000/agents/a_1-Z/invoke', 'POST', '{}'],
      ['http://127.0.0.1:3000/runs', 'POST', '{"agents":1}'],
      ['http://127.0.0.1:3000/runs/dead-beef', 'GET', undefined],
    ]);
    assert.equal(calls[0].options.signal, signal);
    assert.deepEqual(calls[2].options.headers, { 'content-type': 'application/json' });
    assert.equal(calls[4].options.headers, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('SDK unit: exposes text responses and server error fallbacks', async () => {
  const originalFetch = globalThis.fetch;
  const responses = [
    new Response('metric 1'),
    new Response('{"error":"bad request"}', { status: 400 }),
    new Response('{}', { status: 403 }),
    new Response('not json', { status: 502 }),
  ];
  globalThis.fetch = async () => responses.shift();
  try {
    const client = createClient();
    assert.equal(await client.getMetrics(), 'metric 1');
    await assert.rejects(client.health(), error => error.status === 400 && error.message === 'bad request');
    await assert.rejects(client.health(), error => error.status === 403 && error.message === 'HTTP 403');
    await assert.rejects(client.health(), error => error.status === 502 && error.message === 'HTTP 502');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('SDK unit: parses multiline CRLF events and ignores comments', async () => {
  const originalFetch = globalThis.fetch;
  const encoder = new TextEncoder();
  const chunks = [
    ': connected\r\n\r\ndata: {"value":\r\n',
    'data: 1}\r\n\r\n: heartbeat\n\ndata: {"value":2}\n\n',
  ];
  globalThis.fetch = async () => new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  }));
  try {
    const received = [];
    for await (const event of createClient().events()) received.push(event);
    assert.deepEqual(received, [{ value: 1 }, { value: 2 }]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
