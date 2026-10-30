/**
 * Where the upgrade comes from. `handleUpgrade(req, upgrade?)` takes the
 * runtime's upgrade function — demino 2.0's `info.upgradeWebSocket` — and
 * falls back to `Deno.upgradeWebSocket`. The unit tests pin that contract;
 * the integration tests run the reference server through both demino
 * adapters, Node's on Deno's `node:http` compat, so the service drives a
 * `ws`-backed socket there. The rest of the suite serves through a bare
 * `Deno.serve(app)`, which is the fallback.
 */

import { assertEquals, assertStrictEquals, assertThrows } from "@std/assert";
import type { DeminoServeHandler, ServeFn } from "@marianmeres/demino";
import { serve as serveDeno } from "@marianmeres/demino/deno";
import { serve as serveNode } from "@marianmeres/demino/node";

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

// ------------------------------------------------------------------ unit

Deno.test("handleUpgrade uses the upgrade function it is given", async () => {
	const service = new WSService({ logger: null });
	const fake = fakeUpgrade();
	const req = upgradeRequest();

	try {
		assertStrictEquals(service.handleUpgrade(req, fake.upgrade), fake.response);
		assertEquals(fake.calls.length, 1);
		assertStrictEquals(fake.calls[0], req);
		// Set explicitly, whatever the runtime's default: the service decodes
		// `string | ArrayBuffer`, never a Blob or a Node Buffer.
		assertEquals(fake.socket.binaryType, "arraybuffer");
		assertEquals(service.stats().pending, 1);
	} finally {
		await service.close();
	}
});

Deno.test("handleUpgrade checks the origin before it upgrades", async () => {
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
		assertEquals(res.status, 403);
		assertEquals(fake.calls.length, 0);
	} finally {
		await service.close();
	}
});

Deno.test("handleUpgrade without an upgrade function falls back to Deno.upgradeWebSocket", async () => {
	const service = new WSService({ logger: null });
	const fake = fakeUpgrade();
	const deno = Deno as unknown as { upgradeWebSocket: unknown };
	const original = deno.upgradeWebSocket;
	deno.upgradeWebSocket = fake.upgrade;

	try {
		assertStrictEquals(service.handleUpgrade(upgradeRequest()), fake.response);
		assertEquals(fake.calls.length, 1);
	} finally {
		deno.upgradeWebSocket = original;
		await service.close();
	}
});

Deno.test("handleUpgrade with no upgrade function anywhere says what is missing", async () => {
	const service = new WSService({ logger: null });
	const deno = Deno as unknown as { upgradeWebSocket: unknown };
	const original = deno.upgradeWebSocket;
	deno.upgradeWebSocket = undefined;

	try {
		assertThrows(
			() => service.handleUpgrade(upgradeRequest()),
			Error,
			"WebSocket upgrade capability missing",
		);
		assertEquals(service.stats().pending, 0);
	} finally {
		deno.upgradeWebSocket = original;
		await service.close();
	}
});

// ----------------------------------------------------------- integration

/** Binary on the wire, so the socket's `binaryType` decides what `decode` gets. */
const encode: WSEncoder = (frame) => new TextEncoder().encode(JSON.stringify(frame));
const decode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

/**
 * Serves the reference app through `serve` and drives it with the universal
 * client: auth, an echo through `onMessage`, a room round trip.
 */
async function exercise(serve: ServeFn): Promise<void> {
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

	const server = await serve(handler, {
		port: 0,
		hostname: "127.0.0.1",
		onListen: () => {},
	});
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
		assertEquals(c.clientId, "alice");
		assertEquals(upgrades, 1);

		assertEquals(await c.send({ text: "hi" }, { ack: true }), {
			echo: { text: "hi" },
		});

		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));
		const { recipients } = await c.publish("chat", { text: "room" });
		assertEquals(recipients, 1);
		await until(() => seen.length === 1, "room delivery");
		assertEquals(seen[0].payload, { text: "room" });

		assertEquals([...rawSeen], ["ArrayBuffer"]);
	} finally {
		c.dispose();
		await service.close();
		await server.shutdown();
	}
}

Deno.test("Deno adapter: auth, echo and rooms over info.upgradeWebSocket", async () => {
	await exercise(serveDeno);
});

Deno.test("Node adapter (ws, on Deno's node:http): auth, echo and rooms over info.upgradeWebSocket", async () => {
	await exercise(serveNode);
});
