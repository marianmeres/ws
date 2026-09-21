/**
 * The core protocol: messages between client and server, no rooms.
 *
 * Two halves. Against `startCoreServer()` — a server implementing the core and
 * nothing else, which is what PROTOCOL.md asks of a third-party server — these
 * prove the core is genuinely enough. Against the reference server they cover
 * `onMessage` and `service.send()`.
 */

import { assert, assertEquals, assertRejects } from "@std/assert";

import { createWSClient } from "../src/mod.ts";
import { ERROR_CODE } from "../src/protocol/constants.ts";
import { WSRemoteError, WSTerminatedError } from "../src/protocol/errors.ts";
import type { WSMessage, WSRoomMessage } from "../src/protocol/frames.ts";
import type { WSConnectionContext } from "../src/server/mod.ts";
import { rawConnect, sleep, startCoreServer, startServer, until } from "./_helpers.ts";

/** Quiet, heartbeat-free client — liveness has its own tests. */
const client = (url: string, options: Record<string, unknown> = {}) =>
	createWSClient({ url, logger: null, pingInterval: 0, ...options });

// ------------------------------------------------------- core-only server

Deno.test("core-only server: handshake without identity", async () => {
	const server = startCoreServer();
	const c = client(server.url, { auth: () => ({ token: "ok" }) });

	try {
		const connected = new Promise<{ clientId: string | null }>((r) =>
			c.once("connected", r)
		);
		await c.connect();

		// The server assigned no identity, and the client does not invent one.
		assertEquals(c.clientId, null);
		assertEquals((await connected).clientId, null);
		assertEquals(c.namespace, "default");

		// Nothing the application did not choose goes on the wire.
		assertEquals(server.auths[0], {
			type: "auth",
			protocol: 2,
			payload: { token: "ok" },
		});
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("core-only server: an explicit namespace is still sent", async () => {
	const server = startCoreServer();
	const c = client(server.url, { namespace: "org-1", clientId: "alice" });

	try {
		await c.connect();
		assertEquals(server.auths[0].namespace, "org-1");
		assertEquals(server.auths[0].clientId, "alice");
		// Requested, not assigned: the server said nothing about identity.
		assertEquals(c.clientId, null);
		assertEquals(c.namespace, "org-1");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("core-only server: send, send with ack, and a pushed message", async () => {
	const server = startCoreServer();
	const c = client(server.url);

	try {
		const messages: WSMessage[] = [];
		c.on("message", (m) => messages.push(m));

		// Fire-and-forget resolves once written, with nothing.
		assertEquals(await c.send({ op: "typing" }), undefined);
		await until(() => server.received.length === 1, "server receives it");
		assertEquals(server.received[0], { op: "typing" });

		// With an ack, the reply the server put in it comes back.
		const reply = await c.send<{ echo: unknown }>({ op: "load" }, { ack: true });
		assertEquals(reply, { echo: { op: "load" } });

		// A direct message carries nothing but its payload.
		server.push({ op: "progress", done: 42 });
		await until(() => messages.length === 1, "client receives the push");
		assertEquals(messages[0], { payload: { op: "progress", done: 42 } });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("core-only server: rooms fail fast with unsupported, not a timeout", async () => {
	const server = startCoreServer();
	// A long send timeout, so a pass can only mean the nack answered it.
	const c = client(server.url, { sendTimeout: 20_000 });

	try {
		await c.connect();
		const started = Date.now();

		const sub = await assertRejects(
			() => c.subscribe("chat", () => {}),
			WSRemoteError,
		);
		assertEquals(sub.code, ERROR_CODE.UNSUPPORTED);
		// A refused subscription is not held locally.
		assertEquals(c.isSubscribed("chat"), false);

		const pub = await assertRejects(() => c.publish("chat", 1), WSRemoteError);
		assertEquals(pub.code, ERROR_CODE.UNSUPPORTED);

		assert(Date.now() - started < 2_000, "answered, not timed out");

		// The connection survives both refusals.
		assertEquals(await c.send(1, { ack: true }), { echo: 1 });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("core-only server: 4001 is terminal", async () => {
	const server = startCoreServer();
	const c = client(server.url, { auth: () => ({ token: "bad" }) });

	try {
		const err = await assertRejects(() => c.connect(), WSTerminatedError);
		assertEquals(err.code, 4001);
		// Nothing is buffered after a terminal close — it rejects at once.
		await assertRejects(() => c.send(1), WSTerminatedError);
	} finally {
		c.dispose();
		await server.stop();
	}
});

// -------------------------------------------------------- reference server

Deno.test("onMessage receives send() with its context; the return is the reply", async () => {
	const seen: Array<{ ctx: WSConnectionContext; payload: unknown }> = [];
	const server = startServer({
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

		assertEquals(reply, { ok: true, got: { op: "ask" } });
		await until(() => seen.length === 2, "both reach the hook");
		assertEquals(seen.map((s) => s.payload), [{ op: "fire" }, { op: "ask" }]);
		assertEquals(seen[0].ctx.clientId, "alice");
		assertEquals(seen[0].ctx.meta, { role: "admin" });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("onMessage returning nothing is a bare ack", async () => {
	const server = startServer({ onMessage: async () => {} });
	const c = client(server.url);

	try {
		assertEquals(await c.send("x", { ack: true }), undefined);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("onMessage throwing: WSRemoteError speaks for itself, anything else is internal", async () => {
	const server = startServer({
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
		const refused = await assertRejects(
			() => c.send("invalid", { ack: true }),
			WSRemoteError,
		);
		assertEquals(refused.code, "invalid_doc");
		assertEquals(refused.message, "no title");

		const crashed = await assertRejects(
			() => c.send("bug", { ack: true }),
			WSRemoteError,
		);
		assertEquals(crashed.code, ERROR_CODE.INTERNAL);
		assert(!crashed.message.includes("secret"), "internals must not leak");

		// An application error is not a broken connection.
		assert(c.connected);
		assertEquals(await c.send("ok", { ack: true }), "fine");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a fire-and-forget send the hook refuses surfaces as an error event", async () => {
	const server = startServer({
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
		assertEquals(errors[0].code, "invalid_doc");
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("without onMessage the reference server answers unsupported", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		const err = await assertRejects(() => c.send(1, { ack: true }), WSRemoteError);
		assertEquals(err.code, ERROR_CODE.UNSUPPORTED);

		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));
		await c.send(1);
		await until(() => errors.length === 1, "error event for fire-and-forget");
		assertEquals((errors[0] as WSRemoteError).code, ERROR_CODE.UNSUPPORTED);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("service.send() reaches exactly one client, as a direct message", async () => {
	const server = startServer();
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

		assertEquals(server.service.send("alice", { op: "hi" }), true);
		assertEquals(server.service.send("nobody", { op: "hi" }), false);

		await until(() => aliceMessages.length === 1, "alice receives it");
		assertEquals(aliceMessages[0], { payload: { op: "hi" } });
		await sleep(100);
		assertEquals(bobMessages.length, 0);
		assertEquals(roomMessages.length, 0);
	} finally {
		alice.dispose();
		bob.dispose();
		await server.stop();
	}
});

Deno.test("a reply the encoder refuses is still answered", async () => {
	const server = startServer({ onMessage: () => ({ n: 1n }) });
	const c = client(server.url, { sendTimeout: 20_000 });

	try {
		const err = await assertRejects(() => c.send(1, { ack: true }), WSRemoteError);
		assertEquals(err.code, ERROR_CODE.INTERNAL);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("raw: an unknown frame type is unsupported — nack with an id, error without", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();

		a.send({ type: "teleport", id: "t1" });
		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "t1", "nack");
		assertEquals(nack.error?.code, ERROR_CODE.UNSUPPORTED);

		a.send({ type: "teleport" });
		const error = await a.waitFor((f) => f.type === "error", "error");
		assertEquals(error.error?.code, ERROR_CODE.UNSUPPORTED);

		// A non-string id asks for no answer — it cannot be correlated anyway.
		a.send({ type: "teleport", id: 42 });
		await a.waitFor(
			(f) => f.type === "error" && f !== error,
			"second error, not a nack",
		);
		assertEquals(a.frames.filter((f) => f.type === "nack").length, 1);

		assertEquals(a.closed, null);
	} finally {
		await a.close();
		await server.stop();
	}
});
