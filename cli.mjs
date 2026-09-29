import { createApp } from './server.mjs';

export const server = createApp().listen(Number(process.argv[2]), '127.0.0.1', () => {
  process.stdout.write(`agent stress tester listening on http://127.0.0.1:${server.address().port}\n`);
});
