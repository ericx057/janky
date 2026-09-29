import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { availableParallelism } from 'node:os';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

const DEFAULT_WORKFLOW = [{ action: 'request' }];
const DEFAULT_HOSTS = ['127.0.0.1', 'localhost', '[::1]'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

class InputError extends Error {}

function integer(value, fallback, min, max, name) {
  const number = value ?? fallback;
  if (!Number.isInteger(number) || number < min || number > max) {
    throw new InputError(`${name} must be an integer from ${min} to ${max}`);
  }
  return number;
}

function targetUrl(value, allowedHosts) {
  let url;
  try { url = new URL(value); } catch { throw new InputError('target_url must be a URL'); }
  if (!['http:', 'https:'].includes(url.protocol) || !allowedHosts.has(url.hostname) ||
      url.username || url.password || url.hash) {
    throw new InputError('target_url must be HTTP(S) on an allowed host without credentials');
  }
  return url.toString();
}

function object(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}

function profiles(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > 100 || !value.every(object)) {
    throw new InputError('profiles must contain 1 to 100 JSON objects');
  }
  return value;
}

function workflow(value) {
  if (!Array.isArray(value) || value.length < 1 || value.length > 32) {
    throw new InputError('workflow must contain 1 to 32 steps');
  }
  return value.map(step => {
    if (!object(step) || typeof step.action !== 'string' || !/^[a-zA-Z0-9_.-]{1,64}$/.test(step.action) ||
        (step.input !== undefined && !object(step.input)) ||
        (step.expect_status != null && (!Array.isArray(step.expect_status) ||
          step.expect_status.length < 1 || step.expect_status.length > 10 ||
          !step.expect_status.every(status => Number.isInteger(status) && status >= 100 && status <= 599)))) {
      throw new InputError('workflow steps need an action, optional input object, and optional expect_status array');
    }
    return { action: step.action, input: step.input ?? {}, expect_status: step.expect_status ?? null };
  });
}

function eventFor(agent, step, workflowId, steps) {
  const item = steps[step];
  return {
    workflow_id: workflowId,
    workflow_step: step + 1,
    action: item.action,
    agent: { id: agent.id, profile: agent.profile },
    input: item.input,
  };
}

function metricState() {
  return { total: 0, succeeded: 0, failed: 0, throttled: 0, server_errors: 0,
    expected_non_2xx: 0, expectation_failures: 0,
    timeouts: 0, retries: 0, in_flight: 0, latency_sum_ms: 0, latencies: [] };
}

function recordAttempt(metrics, latency, status, error, expected) {
  metrics.total++;
  metrics.latency_sum_ms += latency;
  if (metrics.latencies.length < 10000) metrics.latencies.push(latency);
  else metrics.latencies[metrics.total % 10000] = latency;
  if (expected) metrics.succeeded++;
  else metrics.failed++;
  if (expected && (status < 200 || status >= 300)) metrics.expected_non_2xx++;
  if (status && !expected) metrics.expectation_failures++;
  if (status === 429) metrics.throttled++;
  if (status >= 500) metrics.server_errors++;
  if (error?.name === 'TimeoutError' || error?.name === 'AbortError') metrics.timeouts++;
}

function publicMetrics(metrics) {
  if (metrics.p95_at_total !== metrics.total) {
    const sorted = [...metrics.latencies].sort((a, b) => a - b);
    metrics.p95_ms = sorted.length ? Math.round(sorted[Math.ceil(sorted.length * 0.95) - 1]) : 0;
    metrics.p95_at_total = metrics.total;
  }
  return { total: metrics.total, succeeded: metrics.succeeded, failed: metrics.failed,
    throttled: metrics.throttled, server_errors: metrics.server_errors,
    expected_non_2xx: metrics.expected_non_2xx, expectation_failures: metrics.expectation_failures,
    timeouts: metrics.timeouts, retries: metrics.retries, in_flight: metrics.in_flight,
    latency_avg_ms: metrics.total ? Math.round(metrics.latency_sum_ms / metrics.total) : 0,
    latency_p95_ms: metrics.p95_ms };
}

function retryDelay(response, attempt) {
  const header = response?.headers.get('retry-after');
  if (header !== null && header !== undefined) {
    const seconds = Number(header);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.min(1000, seconds * 1000);
    const date = Date.parse(header);
    if (Number.isFinite(date)) return Math.min(1000, Math.max(0, date - Date.now()));
  }
  return Math.min(1000, 50 * 2 ** attempt);
}

async function dispatch(url, event, step, config, metrics, emit) {
  const body = JSON.stringify(event);
  async function send(attempt) {
    const start = performance.now();
    let response;
    let error;
    for (const item of metrics) item.in_flight++;
    emit({ type: 'request_started', agent_id: event.agent.id, action: event.action });
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body,
        signal: AbortSignal.timeout(config.timeout_ms),
        redirect: 'manual',
      });
      for await (const _chunk of response.body ?? []) { /* consume tool output */ }
    } catch (caught) {
      error = caught;
    } finally {
      for (const item of metrics) item.in_flight--;
    }
    const expected = !error && response &&
      (step.expect_status ? step.expect_status.includes(response.status) : response.ok);
    const latency = performance.now() - start;
    for (const item of metrics) recordAttempt(item, latency, response?.status ?? 0, error, expected);
    emit({ type: 'request', workflow_id: event.workflow_id, agent_id: event.agent.id,
      action: event.action, status: response?.status ?? 0, error: error?.name ?? null,
      expected: Boolean(expected), latency_ms: latency });
    if (expected) return true;
    if (attempt === config.retries) return false;
    if (response && !error && response.status !== 429 && response.status < 500) return false;
    for (const item of metrics) item.retries++;
    emit({ type: 'retry', agent_id: event.agent.id });
    await sleep(retryDelay(response, attempt));
    return send(attempt + 1);
  }
  return send(0);
}

async function readJson(req) {
  if (req.headers['content-type']?.split(';')[0].trim().toLowerCase() !== 'application/json') {
    throw new InputError('content-type must be application/json');
  }
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 65536) throw new InputError('request body exceeds 64 KiB');
    chunks.push(chunk);
  }
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error();
    return value;
  } catch { throw new InputError('request body must be a JSON object'); }
}

function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}

function validateRun(body, allowedHosts) {
  return {
    target_url: targetUrl(body.target_url, allowedHosts),
    telemetry_url: body.telemetry_url === undefined ? null : targetUrl(body.telemetry_url, allowedHosts),
    profiles: profiles(body.profiles ?? [{}]),
    workflow: workflow(body.workflow ?? DEFAULT_WORKFLOW),
    agents: integer(body.agents, 1, 1, 10000, 'agents'),
    concurrency: integer(body.concurrency, 10, 1, 256, 'concurrency'),
    arrival_rate: integer(body.arrival_rate, 10, 1, 10000, 'arrival_rate'),
    think_ms: integer(body.think_ms, 50, 0, 60000, 'think_ms'),
    jitter_ms: integer(body.jitter_ms, 50, 0, 60000, 'jitter_ms'),
    timeout_ms: integer(body.timeout_ms, 5000, 1, 120000, 'timeout_ms'),
    retries: integer(body.retries, 1, 0, 5, 'retries'),
    seed: integer(body.seed, 1, 0, 2147483647, 'seed'),
    workers: integer(body.workers, body.agents >= 1000 ? Math.min(4, availableParallelism()) : 1,
      1, 8, 'workers'),
  };
}

function randomSource(seed) {
  let state = seed;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

function publicRun(run) {
  return { run_id: run.id, state: run.state, agents: { ...run.agents },
    requests: publicMetrics(run.metrics), started_at: run.started_at,
    finished_at: run.finished_at, target_telemetry: run.target_telemetry,
    error: run.error ?? null };
}

async function sampleTelemetry(run, telemetryUrl) {
  try {
    const response = await fetch(telemetryUrl,
      { signal: AbortSignal.timeout(2000), redirect: 'manual' });
    const chunks = [];
    let size = 0;
    for await (const chunk of response.body ?? []) {
      size += chunk.length;
      if (size > 16384) throw new Error('telemetry response exceeds 16 KiB');
      chunks.push(chunk);
    }
    const body = Buffer.concat(chunks).toString('utf8');
    let data = body;
    if (response.headers.get('content-type')?.includes('json')) data = JSON.parse(body);
    run.target_telemetry = { at: new Date().toISOString(), status: response.status, data };
  } catch (error) {
    run.target_telemetry = { at: new Date().toISOString(), error: error.message };
  }
}

async function executeShard(config, runId, indexes, startedAt, concurrency, metrics, emit) {
  const active = new Set();
  const schedule = indexes.map(index => ({ index,
    due: startedAt + index * 1000 / config.arrival_rate +
      randomSource(config.seed + index)() * config.jitter_ms,
  })).sort((a, b) => a.due - b.due);
  let wake;
  let firstError;
  for (const { index, due } of schedule) {
    const wait = due - Date.now();
    if (wait > 0) await sleep(wait);
    while (active.size >= concurrency) await new Promise(resolve => { wake = resolve; });
    if (firstError) throw firstError;
    const agent = { id: `${runId}-${index + 1}`,
      profile: config.profiles[index % config.profiles.length] };
    const agentRandom = randomSource(config.seed + index + 1);
    emit({ type: 'agent_started', run_id: runId, agent_id: agent.id });
    const task = (async () => {
      const workflowId = randomUUID();
      let succeeded = true;
      for (let step = 0; step < config.workflow.length; step++) {
        if (step) await sleep(config.think_ms * (0.5 + agentRandom()) + agentRandom() * config.jitter_ms);
        const event = eventFor(agent, step, workflowId, config.workflow);
        if (!await dispatch(config.target_url, event, config.workflow[step], config, metrics,
          item => emit({ run_id: runId, ...item }))) {
          succeeded = false;
          break;
        }
      }
      emit({ type: 'agent_finished', run_id: runId, agent_id: agent.id, succeeded });
    })();
    active.add(task);
    const settled = () => {
      active.delete(task);
      if (wake) { const resolve = wake; wake = null; resolve(); }
    };
    task.then(settled, error => { firstError ??= error; settled(); });
  }
  await Promise.all(active);
}

if (!isMainThread && workerData?.kind === 'fleet') {
  const { config, runId, indexes, startedAt, concurrency } = workerData;
  let batch = [];
  const flush = () => {
    if (batch.length) { parentPort.postMessage({ type: 'batch', events: batch }); batch = []; }
  };
  const emit = event => {
    batch.push(event);
    if (batch.length >= 128) flush();
  };
  const interval = setInterval(flush, 100);
  void executeShard(config, runId, indexes, startedAt, concurrency, [], emit).then(() => {
    clearInterval(interval);
    flush();
    parentPort.postMessage({ type: 'done' });
  });
}

export function createApp({ allowedHosts = process.env.ALLOWED_TARGET_HOSTS?.split(',') ?? DEFAULT_HOSTS,
  workerScript = new URL(import.meta.url) } = {}) {
  const hosts = new Set(allowedHosts.map(host => host.trim()));
  const runs = new Map();
  const agents = new Map();
  const busyAgents = new Set();
  let directInFlight = 0;
  const listeners = new Set();
  const global = metricState();
  const totals = { started: 0, completed: 0, failed: 0 };
  const emit = event => {
    if (!listeners.size) return;
    const line = `data: ${JSON.stringify(event)}\n\n`;
    for (const res of listeners) {
      if (!res.write(line)) { listeners.delete(res); res.destroy(); }
    }
  };

  async function runFleet(run) {
    const workerCount = Math.min(run.config.workers, run.config.agents, run.config.concurrency);
    const workers = [];
    const handleEvent = (event, fromWorker) => {
      if (event.type === 'agent_started') { run.agents.started++; totals.started++; }
      if (event.type === 'agent_finished') {
        if (event.succeeded) { run.agents.completed++; totals.completed++; }
        else { run.agents.failed++; totals.failed++; }
      }
      if (fromWorker && event.type === 'request') {
        for (const metrics of [run.metrics, global]) {
          metrics.in_flight--;
          recordAttempt(metrics, event.latency_ms, event.status,
            event.error ? { name: event.error } : null, event.expected);
        }
      }
      if (fromWorker && event.type === 'request_started') {
        run.metrics.in_flight++;
        global.in_flight++;
      }
      if (fromWorker && event.type === 'retry') {
        run.metrics.retries++;
        global.retries++;
      }
      emit(event);
    };
    const telemetryUrl = run.config.telemetry_url;
    let sampling = false;
    const sample = async () => {
      if (sampling) return;
      sampling = true;
      try { await sampleTelemetry(run, telemetryUrl); } finally { sampling = false; }
    };
    const initialSample = telemetryUrl ? sample() : Promise.resolve();
    const interval = telemetryUrl ? setInterval(() => { void sample(); }, 1000) : null;
    try {
      if (workerCount === 1) {
        const indexes = Array.from({ length: run.config.agents }, (_, index) => index);
        await executeShard(run.config, run.id, indexes, Date.now(), run.config.concurrency,
          [run.metrics, global], event => handleEvent(event, false));
      } else {
        const startedAt = Date.now() + 100;
        const tasks = Array.from({ length: workerCount }, (_, workerIndex) => new Promise((resolve, reject) => {
          const indexes = Array.from({ length: run.config.agents }, (_, index) => index)
            .filter(index => index % workerCount === workerIndex);
          const concurrency = Math.floor(run.config.concurrency / workerCount) +
            (workerIndex < run.config.concurrency % workerCount ? 1 : 0);
          const worker = new Worker(workerScript, { workerData: {
            kind: 'fleet', config: run.config, runId: run.id, indexes, startedAt, concurrency,
          } });
          workers.push(worker);
          let finished = false;
          worker.on('message', message => {
            if (message.type === 'batch') message.events.forEach(event => handleEvent(event, true));
            else if (message.type === 'done') { finished = true; resolve(); }
          });
          worker.on('error', reject);
          worker.on('exit', code => { if (!finished) reject(new Error(`worker exited ${code}`)); });
        }));
        await Promise.all(tasks);
      }
      run.state = 'completed';
    } catch (error) {
      run.state = 'failed';
      run.error = error.message;
    } finally {
      await Promise.all(workers.map(worker => worker.terminate()));
      if (interval) clearInterval(interval);
      await initialSample;
      delete run.config;
      run.finished_at = new Date().toISOString();
      emit({ type: 'run_finished', run_id: run.id, state: run.state });
    }
  }

  return createServer(async (req, res) => {
    const path = new URL(req.url, 'http://localhost').pathname;
    try {
      if (req.method === 'GET' && path === '/health') return json(res, 200, { ok: true });
      if (req.method === 'GET' && path === '/events') {
        if (listeners.size >= 32) return json(res, 503, { error: 'too many event observers' });
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
        res.write(': connected\n\n');
        listeners.add(res);
        req.on('close', () => listeners.delete(res));
        return;
      }
      if (req.method === 'GET' && path === '/status') {
        return json(res, 200, { agents: { ...totals }, requests: publicMetrics(global),
          runs: [...runs.values()].map(run => ({ run_id: run.id, state: run.state,
            target_telemetry: run.target_telemetry })) });
      }
      if (req.method === 'GET' && path === '/metrics') {
        const metrics = publicMetrics(global);
        const lines = [
          ['agent_stress_agents_started_total', totals.started],
          ['agent_stress_agents_completed_total', totals.completed],
          ['agent_stress_agents_failed_total', totals.failed],
          ...['total', 'succeeded', 'failed', 'throttled', 'server_errors', 'expected_non_2xx',
            'expectation_failures', 'timeouts', 'retries', 'in_flight',
            'latency_avg_ms', 'latency_p95_ms'].map(key => [`agent_stress_requests_${key}`, metrics[key]]),
        ];
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4; charset=utf-8' });
        return res.end(lines.map(([name, value]) => `${name} ${value}\n`).join(''));
      }
      const runMatch = path.match(/^\/runs\/([a-f0-9-]+)$/);
      if (req.method === 'GET' && runMatch) {
        const run = runs.get(runMatch[1]);
        return run ? json(res, 200, publicRun(run)) : json(res, 404, { error: 'run not found' });
      }
      if (req.method === 'POST' && path === '/runs') {
        const config = validateRun(await readJson(req), hosts);
        if (directInFlight || [...runs.values()].some(item => item.state === 'running')) {
          return json(res, 409, { error: 'another workload is active' });
        }
        const run = { id: randomUUID(), state: 'running', config, agents: { started: 0, completed: 0, failed: 0 },
          metrics: metricState(), started_at: new Date().toISOString(), finished_at: null,
          target_telemetry: null };
        runs.set(run.id, run);
        if (runs.size > 100) runs.delete(runs.keys().next().value);
        void runFleet(run);
        return json(res, 202, { run_id: run.id, status_url: `/runs/${run.id}` });
      }
      const agentMatch = path.match(/^\/agents\/([a-zA-Z0-9_-]{1,64})\/invoke$/);
      if (req.method === 'POST' && agentMatch) {
        const body = await readJson(req);
        const id = agentMatch[1];
        const previous = agents.get(id);
        const profile = profiles([body.profile ?? previous?.profile ?? {}])[0];
        const steps = workflow(body.workflow ?? previous?.workflow ?? DEFAULT_WORKFLOW);
        const url = body.target_url === undefined ? null : targetUrl(body.target_url, hosts);
        const config = { timeout_ms: integer(body.timeout_ms, 5000, 1, 120000, 'timeout_ms'),
          retries: integer(body.retries, 1, 0, 5, 'retries') };
        if (busyAgents.has(id) || [...runs.values()].some(item => item.state === 'running')) {
          return json(res, 409, { error: 'agent or fleet is busy' });
        }
        if (url && directInFlight >= 256) return json(res, 429, { error: 'too many direct invocations' });
        const step = (previous?.step ?? 0) % steps.length;
        const workflowId = step === 0 ? randomUUID() : previous.workflowId;
        const event = eventFor({ id, profile }, step, workflowId, steps);
        let delivered = null;
        if (url) {
          busyAgents.add(id);
          directInFlight++;
          try { delivered = await dispatch(url, event, steps[step], config, [global], emit); }
          finally { directInFlight--; busyAgents.delete(id); }
        }
        const argumentsJson = JSON.stringify(event);
        if (delivered !== false) {
          agents.set(id, { profile, workflow: steps, step: (step + 1) % steps.length, workflowId });
          if (agents.size > 10000) agents.delete(agents.keys().next().value);
        }
        return json(res, 200, { agent_id: id, workflow_id: workflowId, step: step + 1,
          message: { role: 'assistant', content: null, tool_calls: [{ id: randomUUID(), type: 'function',
            function: { name: 'workflow_request', arguments: argumentsJson } }] }, delivered });
      }
      return json(res, 404, { error: 'not found' });
    } catch (error) {
      return json(res, error instanceof InputError ? 400 : 500,
        { error: error instanceof InputError ? error.message : 'internal error' });
    }
  });
}
