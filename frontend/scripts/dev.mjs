// `next dev` retries the next port only when it runs on its default
// (3000); an explicit --port fails with EADDRINUSE instead. This picks the
// first free port from PORT (default 3001) upward, then starts next dev.

import net from "node:net";
import { spawn } from "node:child_process";

const START = Number(process.env.PORT) || 3001;
const MAX_TRIES = 20;

// Only EADDRINUSE means taken; other errors (e.g. no IPv6) say nothing.
const freeOn = (port, host) =>
  new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", (err) => resolve(err.code !== "EADDRINUSE"));
    server.once("listening", () => server.close(() => resolve(true)));
    server.listen(port, host);
  });

// Next binds the IPv6 wildcard. A server on 127.0.0.1 alone does not block
// that bind, but it still answers http://localhost, so check both.
const isFree = async (port) =>
  (await freeOn(port, "::")) && (await freeOn(port, "127.0.0.1"));

let port = START;
while (!(await isFree(port))) {
  if (port - START >= MAX_TRIES) {
    console.error(`No free port in ${START}–${port}.`);
    process.exit(1);
  }
  port += 1;
}
if (port !== START) console.log(`Port ${START} is in use, using ${port}.`);

const child = spawn(
  "next",
  ["dev", "--port", String(port), ...process.argv.slice(2)],
  { stdio: "inherit" }
);
child.on("exit", (code) => process.exit(code ?? 0));
