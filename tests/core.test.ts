/**
 * The core protocol: messages between client and server, no rooms.
 *
 * Two halves. Against `await startCoreServer()` — a server implementing the core and
 * nothing else, which is what PROTOCOL.md asks of a third-party server — these
 * prove the core is genuinely enough. Against the reference server they cover
 * `onMessage` and `service.send()`.
 */

import assert from "node:assert/strict";
import { test } from "node:test";

import { createWSClient } from "../src/mod.ts";
import { ERROR_CODE } from "../src/protocol/constants.ts";
import { WSRemoteError, WSTerminatedError } from "../src/protocol/errors.ts";
import type { WSMessage, WSRoomMessage } from "../src/protocol/frames.ts";
import type { WSConnectionContext } from "../src/server/mod.ts";
import { rawConnect, rejection, sleep, startCoreServer, startServer, until } from "./_helpers.ts";

/** Quiet, heartbeat-free client — liveness has its own tests. */
const client = (url: string, options: Record<string, unknown> = {}) =>
	createWSClient({ url, logger: null, pingInterval: 0, ...options });

// ------------------------------------------------------- core-only server

test("core-only server: handshake without identity", async () => {
	const server = await startCoreServer();
	const c = client(server.url, { auth: () => ({ token: "ok" }) });

	try {
		const connected = new Promise<{ clientId: string | null }>((r) =>
			c.once("connected", r)
		);
		await c.connect();

		// The server assigned no identity, and the client does not invent one.
		assert.deepEqual(c.clientId, null);
		assert.deepEqual((await connected).clientId, null);
		assert.deepEqual(c.namespace, "default");

		// Nothing the application did not choose goes on the wire.
		assert.deepEqual(server.auths[0], {
			type: "auth",
			protocol: 2,
			payload: { token: "ok" },
		});
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("core-only server: an explicit namespace is still sent", async () => {
	const server = await startCoreServer();
	const c = client(server.url, { namespace: "org-1", clientId: "alice" });

	try {
		await c.connect();
		assert.deepEqual(server.auths[0].namespace, "org-1");
		assert.deepEqual(server.auths[0].clientId, "alice");
		// Requested, not assigned: the server said nothing about identity.
		assert.deepEqual(c.clientId, null);
		assert.deepEqual(c.namespace, "org-1");
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("core-only server: send, send with ack, and a pushed message", async () => {
	const server = await startCoreServer();
	const c = client(server.url);

	try {
		const messages: WSMessage[] = [];
		c.on("message", (m) => messages.push(m));

		// Fire-and-forget resolves once written, with nothing.
		assert.deepEqual(await c.send({ op: "typing" }), undefined);
		await until(() => server.received.length === 1, "server receives it");
		assert.deepEqual(server.received[0], { op: "typing" });

		// With an ack, the reply the server put in it comes back.
		const reply = await c.send<{ echo: unknown }>({ op: "load" }, { ack: true });
		assert.deepEqual(reply, { echo: { op: "load" } });

		// A direct message carries nothing but its payload.
		server.push({ op: "progress", done: 42 });
		await until(() => messages.length === 1, "client receives the push");
		assert.deepEqual(messages[0], { payload: { op: "progress", done: 42 } });
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("core-only server: rooms fail fast with unsupported, not a timeout", async () => {
	const server = await startCoreServer();
	// A long send timeout, so a pass can only mean the nack answered it.
	const c = client(server.url, { sendTimeout: 20_000 });

	try {
		await c.connect();
		const started = Date.now();

		const sub = await rejection(
			() => c.subscribe("chat", () => {}),
			WSRemoteError,
		);
		assert.deepEqual(sub.code, ERROR_CODE.UNSUPPORTED);
		// A refused subscription is not held locally.
		assert.deepEqual(c.isSubscribed("chat"), false);

		const pub = await rejection(() => c.publish("chat", 1), WSRemoteError);
		assert.deepEqual(pub.code, ERROR_CODE.UNSUPPORTED);

		assert(Date.now() - started < 2_000, "answered, not timed out");

		// The connection survives both refusals.
		assert.deepEqual(await c.send(1, { ack: true }), { echo: 1 });
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("core-only server: a nack's details reach WSRemoteError.details, an error frame's too", async () => {
	const server = await startCoreServer();
	const c = client(server.url);

	try {
		// Structured detail the application branches on — a countdown here —
		// rides along with the code; `message` stays for humans.
		const details = { time_left_seconds: 42 };
		const err = await rejection(
			() => c.send({ op: "refuse", details }, { ack: true }),
			WSRemoteError,
		);
		assert.deepEqual(err.code, "refused");
		assert.deepEqual(err.details, details);

		// Nothing attached, nothing invented.
		const bare = await rejection(
			() => c.send({ op: "refuse" }, { ack: true }),
			WSRemoteError,
		);
		assert.deepEqual(bare.details, undefined);

		// The uncorrelated `error` frame carries it the same way.
		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));
		await c.send({ op: "refuse", details: { capacity: "full" } });
		await until(() => errors.length === 1, "error event");
		assert(errors[0] instanceof WSRemoteError);
		assert.deepEqual(errors[0].details, { capacity: "full" });
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("core-only server: 4001 is terminal", async () => {
	const server = await startCoreServer();
	const c = client(server.url, { auth: () => ({ token: "bad" }) });

	try {
		const err = await rejection(() => c.connect(), WSTerminatedError);
		assert.deepEqual(err.code, 4001);
		// Nothing is buffered after a terminal close — it rejects at once.
		await rejection(() => c.send(1), WSTerminatedError);
	} finally {
		c.dispose();
		await server.stop();
	}
});

// -------------------------------------------------------- reference server

test("onMessage receives send() with its context; the return is the reply", async () => {
	const seen: Array<{ ctx: WSConnectionContext; payload: unknown }> = [];
	const server = await startServer({
		verify: () => ({ clientId: "alice", meta: { role: "admin" } }),
		onMessage: (ctx, payload) => {
			seen.push({ ctx, payload });
			return { ok: true, got: payload };
		},
	});
	const c = client(server.url);

	try {
		await c.send({ op: "fire" });
		const reply = await c.send({ op: "ask" }, { ack: true });

		assert.deepEqual(reply, { ok: true, got: { op: "ask" } });
		await until(() => seen.length === 2, "both reach the hook");
		assert.deepEqual(seen.map((s) => s.payload), [{ op: "fire" }, { op: "ask" }]);
		assert.deepEqual(seen[0].ctx.clientId, "alice");
		assert.deepEqual(seen[0].ctx.meta, { role: "admin" });
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("onMessage returning nothing is a bare ack", async () => {
	const server = await startServer({ onMessage: async () => {} });
	const c = client(server.url);

	try {
		assert.deepEqual(await c.send("x", { ack: true }), undefined);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("onMessage throwing: WSRemoteError speaks for itself, anything else is internal", async () => {
	const server = await startServer({
		onMessage: (_ctx, payload) => {
			if (payload === "invalid") {
				throw new WSRemoteError({ code: "invalid_doc", message: "no title" });
			}
			if (payload === "bug") throw new Error("secret stack detail");
			return "fine";
		},
	});
	const c = client(server.url);

	try {
		const refused = await rejection(
			() => c.send("invalid", { ack: true }),
			WSRemoteError,
		);
		assert.deepEqual(refused.code, "invalid_doc");
		assert.deepEqual(refused.message, "no title");

		const crashed = await rejection(
			() => c.send("bug", { ack: true }),
			WSRemoteError,
		);
		assert.deepEqual(crashed.code, ERROR_CODE.INTERNAL);
		assert(!crashed.message.includes("secret"), "internals must not leak");

		// An application error is not a broken connection.
		assert(c.connected);
		assert.deepEqual(await c.send("ok", { ack: true }), "fine");
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("onMessage throwing WSRemoteError with details delivers them unchanged", async () => {
	const details = { capacity: "full", time_left_seconds: 42 };
	const server = await startServer({
		onMessage: (_ctx, payload) => {
			if (payload === "busy") {
				throw new WSRemoteError({ code: "busy", message: "try later", details });
			}
			throw new WSRemoteError({ code: "nope", message: "no" });
		},
	});
	const c = client(server.url);

	try {
		const busy = await rejection(
			() => c.send("busy", { ack: true }),
			WSRemoteError,
		);
		assert.deepEqual(busy.code, "busy");
		assert.deepEqual(busy.message, "try later");
		assert.deepEqual(busy.details, details);

		const bare = await rejection(() => c.send("x", { ack: true }), WSRemoteError);
		assert.deepEqual(bare.details, undefined);

		// A fire-and-forget refusal travels as an `error` frame; same passthrough.
		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));
		await c.send("busy");
		await until(() => errors.length === 1, "error event");
		assert.deepEqual((errors[0] as WSRemoteError).details, details);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("a refusal whose details the encoder rejects is still answered", async () => {
	const server = await startServer({
		onMessage: () => {
			// `details` is the application's, so it can be anything — a BigInt
			// included, which JSON cannot encode.
			throw new WSRemoteError({ code: "odd", message: "odd", details: { n: 1n } });
		},
	});
	const c = client(server.url, { sendTimeout: 20_000 });

	try {
		const started = Date.now();
		const err = await rejection(() => c.send(1, { ack: true }), WSRemoteError);
		// Not the refusal the hook meant, but an answer — rather than the
		// client waiting out its whole send timeout for a nack that never left.
		assert.deepEqual(err.code, ERROR_CODE.INTERNAL);
		assert(Date.now() - started < 2_000, "answered, not timed out");
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("a fire-and-forget send the hook refuses surfaces as an error event", async () => {
	const server = await startServer({
		onMessage: () => {
			throw new WSRemoteError({ code: "invalid_doc", message: "no title" });
		},
	});
	const c = client(server.url);

	try {
		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));
		await c.send("invalid");
		await until(() => errors.length === 1, "error event");
		assert(errors[0] instanceof WSRemoteError);
		assert.deepEqual(errors[0].code, "invalid_doc");
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("without onMessage the reference server answers unsupported", async () => {
	const server = await startServer();
	const c = client(server.url);

	try {
		const err = await rejection(() => c.send(1, { ack: true }), WSRemoteError);
		assert.deepEqual(err.code, ERROR_CODE.UNSUPPORTED);

		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));
		await c.send(1);
		await until(() => errors.length === 1, "error event for fire-and-forget");
		assert.deepEqual((errors[0] as WSRemoteError).code, ERROR_CODE.UNSUPPORTED);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("service.send() reaches exactly one client, as a direct message", async () => {
	const server = await startServer();
	const alice = client(server.url, { clientId: "alice" });
	const bob = client(server.url, { clientId: "bob" });

	try {
		await alice.connect();
		await bob.connect();

		const aliceMessages: WSMessage[] = [];
		const bobMessages: WSMessage[] = [];
		alice.on("message", (m) => aliceMessages.push(m));
		bob.on("message", (m) => bobMessages.push(m));
		// A room handler must not see a direct message, whatever it listens to.
		const roomMessages: WSRoomMessage[] = [];
		await alice.subscribe("chat", (m) => roomMessages.push(m));

		assert.deepEqual(server.service.send("alice", { op: "hi" }), true);
		assert.deepEqual(server.service.send("nobody", { op: "hi" }), false);

		await until(() => aliceMessages.length === 1, "alice receives it");
		assert.deepEqual(aliceMessages[0], { payload: { op: "hi" } });
		await sleep(100);
		assert.deepEqual(bobMessages.length, 0);
		assert.deepEqual(roomMessages.length, 0);
	} finally {
		alice.dispose();
		bob.dispose();
		await server.stop();
	}
});

test("a reply the encoder refuses is still answered", async () => {
	const server = await startServer({ onMessage: () => ({ n: 1n }) });
	const c = client(server.url, { sendTimeout: 20_000 });

	try {
		const err = await rejection(() => c.send(1, { ack: true }), WSRemoteError);
		assert.deepEqual(err.code, ERROR_CODE.INTERNAL);
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("raw: an unknown frame type is unsupported — nack with an id, error without", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();

		a.send({ type: "teleport", id: "t1" });
		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "t1", "nack");
		assert.deepEqual(nack.error?.code, ERROR_CODE.UNSUPPORTED);

		a.send({ type: "teleport" });
		const error = await a.waitFor((f) => f.type === "error", "error");
		assert.deepEqual(error.error?.code, ERROR_CODE.UNSUPPORTED);

		// A non-string id asks for no answer — it cannot be correlated anyway.
		a.send({ type: "teleport", id: 42 });
		await a.waitFor(
			(f) => f.type === "error" && f !== error,
			"second error, not a nack",
		);
		assert.deepEqual(a.frames.filter((f) => f.type === "nack").length, 1);

		assert.deepEqual(a.closed, null);
	} finally {
		await a.close();
		await server.stop();
	}
});
