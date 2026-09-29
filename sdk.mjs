export function createClient(baseUrl = 'http://127.0.0.1:3000') {
  const base = new URL(baseUrl);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password ||
      base.pathname !== '/' || base.search || base.hash) {
    throw new TypeError('baseUrl must be an HTTP(S) server origin');
  }

  async function request(path, { method = 'GET', body, signal, text = false, raw = false } = {}) {
    const response = await fetch(new URL(path, base), {
      method,
      headers: method === 'POST' ? { 'content-type': 'application/json' } : undefined,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    });
    if (!response.ok) {
      const details = await response.json().catch(() => null);
      const error = new Error(details?.error ?? `HTTP ${response.status}`);
      error.status = response.status;
      throw error;
    }
    return raw ? response : text ? response.text() : response.json();
  }

  return {
    health: options => request('/health', options),
    getStatus: options => request('/status', options),
    getMetrics: options => request('/metrics', { ...options, text: true }),
    async *events(options) {
      const response = await request('/events', { ...options, raw: true });
      const decoder = new TextDecoder();
      let buffer = '';
      for await (const chunk of response.body) {
        buffer += decoder.decode(chunk, { stream: true });
        let separator;
        while ((separator = buffer.match(/\r?\n\r?\n/))) {
          const message = buffer.slice(0, separator.index);
          buffer = buffer.slice(separator.index + separator[0].length);
          const data = message.split(/\r?\n/).filter(line => line.startsWith('data:'))
            .map(line => line.slice(5).trimStart()).join('\n');
          if (data) yield JSON.parse(data);
        }
      }
    },
    startRun: (config, options) => request('/runs', { ...options, method: 'POST', body: config }),
    async getRun(id, options) {
      if (!/^[a-f0-9-]+$/.test(id)) throw new TypeError('invalid run ID');
      return request(`/runs/${id}`, options);
    },
    async invokeAgent(id, config = {}, options) {
      if (!/^[a-zA-Z0-9_-]{1,64}$/.test(id)) throw new TypeError('invalid agent ID');
      return request(`/agents/${id}/invoke`, { ...options, method: 'POST', body: config });
    },
  };
}
