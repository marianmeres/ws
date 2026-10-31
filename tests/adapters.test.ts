/**
 * Where the upgrade comes from. `handleUpgrade(req, upgrade?)` takes the
 * runtime's upgrade function — demino 2.0's `info.upgradeWebSocket` — and
 * falls back to `Deno.upgradeWebSocket`. The unit tests pin that contract;
 * the integration tests run the reference server through the demino adapters
 * — Node's everywhere (on Deno, over its `node:http` compat), Deno's and a
 * bare `Deno.serve(app)` (the fallback, for real) on Deno only. The rest of
 * the suite serves through the runtime's own adapter (`tests/_helpers.ts`).
 */

import assert from "node:assert/strict";
import { versions } from "node:process";
import { test } from "node:test";
import type { DeminoServeHandler } from "@marianmeres/demino";
import { type ServeHandle, type ServeOptions, serve as serveNode } from "@marianmeres/demino/node";

import { createWSClient } from "../src/mod.ts";
import type { WSDecoder, WSEncoder, WSRoomMessage } from "../src/protocol/frames.ts";
import { createWSApp, WSService, type WSUpgradeWebSocket } from "../src/server/mod.ts";
import { until } from "./_helpers.ts";

/** Enough of a socket for `handleUpgrade` to wire up and `close()` to close. */
function fakeSocket(): WebSocket {
	const socket = {
		readyState: WebSocket.CONNECTING as number,
		binaryType: "blob",
		close() {
			socket.readyState = WebSocket.CLOSED;
		},
	};
	return socket as unknown as WebSocket;
}

/** An upgrade function that records its calls and hands back a fake. */
function fakeUpgrade() {
	const calls: Request[] = [];
	const socket = fakeSocket();
	const response = new Response("upgraded");
	const upgrade: WSUpgradeWebSocket = (req) => {
		calls.push(req);
		return { socket, response };
	};
	return { upgrade, calls, socket, response };
}

const upgradeRequest = (headers: Record<string, string> = {}) =>
	new Request("http://localhost/ws", { headers: { upgrade: "websocket", ...headers } });

/**
 * Runs `fn` with `globalThis.Deno.upgradeWebSocket` set to `upgrade` — on Deno
 * by swapping the real one, elsewhere by installing a stand-in `Deno` global —
 * and puts back whatever was there.
 */
async function withDenoUpgrade(upgrade: unknown, fn: () => Promise<void>): Promise<void> {
	const g = globalThis as { Deno?: { upgradeWebSocket?: unknown } };
	const real = g.Deno;
	if (real) {
		const original = real.upgradeWebSocket;
		real.upgradeWebSocket = upgrade;
		try {
			await fn();
		} finally {
			real.upgradeWebSocket = original;
		}
	} else {
		g.Deno = { upgradeWebSocket: upgrade };
		try {
			await fn();
		} finally {
			delete g.Deno;
		}
	}
}

// ------------------------------------------------------------------ unit

test("handleUpgrade uses the upgrade function it is given", async () => {
	const service = new WSService({ logger: null });
	const fake = fakeUpgrade();
	const req = upgradeRequest();

	try {
		assert.equal(service.handleUpgrade(req, fake.upgrade), fake.response);
		assert.deepEqual(fake.calls.length, 1);
		assert.equal(fake.calls[0], req);
		// Set explicitly, whatever the runtime's default: the service decodes
		// `string | ArrayBuffer`, never a Blob or a Node Buffer.
		assert.deepEqual(fake.socket.binaryType, "arraybuffer");
		assert.deepEqual(service.stats().pending, 1);
	} finally {
		await service.close();
	}
});

test("handleUpgrade checks the origin before it upgrades", async () => {
	const service = new WSService({
		logger: null,
		allowedOrigins: ["https://app.example"],
	});
	const fake = fakeUpgrade();

	try {
		const res = service.handleUpgrade(
			upgradeRequest({ origin: "https://evil.example" }),
			fake.upgrade,
		);
		assert.deepEqual(res.status, 403);
		assert.deepEqual(fake.calls.length, 0);
	} finally {
		await service.close();
	}
});

test("handleUpgrade without an upgrade function falls back to Deno.upgradeWebSocket", async () => {
	const service = new WSService({ logger: null });
	const fake = fakeUpgrade();

	try {
		await withDenoUpgrade(fake.upgrade, async () => {
			assert.equal(service.handleUpgrade(upgradeRequest()), fake.response);
			assert.deepEqual(fake.calls.length, 1);
		});
	} finally {
		await service.close();
	}
});

test("handleUpgrade with no upgrade function anywhere says what is missing", async () => {
	const service = new WSService({ logger: null });

	try {
		await withDenoUpgrade(undefined, async () => {
			assert.throws(() => service.handleUpgrade(upgradeRequest()), {
				name: "Error",
				message: /WebSocket upgrade capability missing/,
			});
			assert.deepEqual(service.stats().pending, 0);
		});
	} finally {
		await service.close();
	}
});

// ----------------------------------------------------------- integration

/** Binary on the wire, so the socket's `binaryType` decides what `decode` gets. */
const encode: WSEncoder = (frame) => new TextEncoder().encode(JSON.stringify(frame));
const decode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

/** What `exercise` needs of a running server. */
type Served = Pick<ServeHandle, "port" | "shutdown">;

/** Listen options every serve below shares. */
const OPTIONS: ServeOptions = { port: 0, hostname: "127.0.0.1", onListen: () => {} };

/**
 * Serves the reference app through `serve` and drives it with the universal
 * client: auth, an echo through `onMessage`, a room round trip. `viaAdapter`
 * is whether the upgrade must come from the adapter's capability (`false`:
 * from the `Deno.upgradeWebSocket` fallback, no capability call at all).
 */
async function exercise(
	serve: (handler: DeminoServeHandler) => Promise<Served>,
	viaAdapter = true,
): Promise<void> {
	const rawSeen = new Set<string>();
	const { app, service } = createWSApp("/ws", [], {
		logger: null,
		deminoOptions: { logger: null },
		verify: (payload) => (payload === "secret" ? { clientId: "alice" } : null),
		onMessage: (_ctx, payload) => ({ echo: payload }),
		encode,
		decode: (raw) => {
			rawSeen.add(
				typeof raw === "string"
					? "string"
					: raw instanceof ArrayBuffer
					? "ArrayBuffer"
					: Object.prototype.toString.call(raw),
			);
			return decode(raw);
		},
	});

	// Counts the adapter's capability as the route calls it, so a silent
	// fallback to `Deno.upgradeWebSocket` cannot pass for the adapter path.
	let upgrades = 0;
	const handler: DeminoServeHandler = (req, info) => {
		const upgrade = info?.upgradeWebSocket;
		if (!upgrade) return app(req, info);
		return app(req, {
			...info,
			upgradeWebSocket: (r, options) => {
				upgrades++;
				return upgrade(r, options);
			},
		});
	};

	const server = await serve(handler);
	const c = createWSClient({
		url: `ws://127.0.0.1:${server.port}/ws`,
		logger: null,
		pingInterval: 0,
		connectTimeout: 3_000,
		auth: () => "secret",
		encode,
		decode,
	});

	try {
		await c.connect();
		assert.deepEqual(c.clientId, "alice");
		assert.deepEqual(upgrades, viaAdapter ? 1 : 0);

		assert.deepEqual(await c.send({ text: "hi" }, { ack: true }), {
			echo: { text: "hi" },
		});

		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));
		const { recipients } = await c.publish("chat", { text: "room" });
		assert.deepEqual(recipients, 1);
		await until(() => seen.length === 1, "room delivery");
		assert.deepEqual(seen[0].payload, { text: "room" });

		assert.deepEqual([...rawSeen], ["ArrayBuffer"]);
	} finally {
		c.dispose();
		await service.close();
		await server.shutdown();
	}
}

test("Node adapter (ws): auth, echo and rooms over info.upgradeWebSocket", async () => {
	await exercise((handler) => serveNode(handler, OPTIONS));
});

const DENO_ONLY = { skip: !versions.deno && "Deno only" };

test("Deno adapter: auth, echo and rooms over info.upgradeWebSocket", DENO_ONLY, async () => {
	const { serve } = await import("@marianmeres/demino/deno");
	await exercise((handler) => serve(handler, OPTIONS));
});

/** The slice of `Deno.serve` a bare host uses; declared here, tsc has no `Deno`. */
type BareDenoServe = (
	options: { port: number; hostname: string; onListen: () => void },
	handler: DeminoServeHandler,
) => { addr: { port: number }; shutdown(): Promise<void> };

test("bare Deno.serve(app): auth, echo and rooms over the Deno.upgradeWebSocket fallback", DENO_ONLY, async () => {
	const deno = (globalThis as unknown as { Deno: { serve: BareDenoServe } }).Deno;
	await exercise(async (handler) => {
		// No adapter, so `info` carries no `upgradeWebSocket`
		const server = deno.serve({ port: 0, hostname: "127.0.0.1", onListen: () => {} }, handler);
		return { port: server.addr.port, shutdown: () => server.shutdown() };
	}, false);
});
