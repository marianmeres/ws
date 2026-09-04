/**
 * Server input hardening, exercised with raw sockets.
 *
 * The stock client cannot produce these frames; a hostile or buggy one can, and
 * every case here used to be either a crash or a silently accepted nonsense
 * value.
 */

import { assert, assertEquals, assertNotEquals } from "@std/assert";

import { CLOSE, ERROR_CODE, PROTOCOL_VERSION } from "../src/protocol/constants.ts";
import type { WSFrame } from "../src/protocol/frames.ts";
import { rawConnect, sleep, startServer, until } from "./_helpers.ts";

Deno.test("a non-array `rooms` on sub is a bad_request, not a crash", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);
	const b = await rawConnect(server.url);

	try {
		await a.auth();
		await b.auth();

		// `for…of` over a number rejected the message promise, and an unhandled
		// rejection takes the whole Deno process down with it.
		a.send({ type: "sub", id: "x", rooms: 5 });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "x", "nack");
		assertEquals(nack.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "rooms must be an array",
		});

		// The offending socket survives its own mistake...
		a.send({ type: "ping" });
		await a.waitFor((f) => f.type === "pong", "pong");
		assertEquals(a.closed, null);

		// ...and so does everybody else.
		b.send({ type: "sub", id: "s", rooms: [{ room: "chat" }] });
		await b.waitFor((f) => f.type === "ack" && f.id === "s", "ack");
	} finally {
		await a.close();
		await b.close();
		await server.stop();
	}
});

Deno.test("a non-array `rooms` on unsub is a bad_request", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		a.send({ type: "unsub", id: "u", rooms: "chat" });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "u", "nack");
		assertEquals(nack.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "rooms must be an array",
		});

		a.send({ type: "ping" });
		await a.waitFor((f) => f.type === "pong", "pong");
	} finally {
		await a.close();
		await server.stop();
	}
});

Deno.test("sub skips malformed room entries and acks the rest", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);

	try {
		const hello = await a.auth();
		a.send({
			type: "sub",
			id: "s",
			rooms: [7, null, { room: "" }, { room: 42 }, { room: "chat" }],
		});

		await a.waitFor((f) => f.type === "ack" && f.id === "s", "ack");
		assertEquals(server.service.members("chat"), [hello.clientId]);
	} finally {
		await a.close();
		await server.stop();
	}
});

Deno.test("pub without a usable room is a bad_request", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();

		a.send({ type: "pub", id: "p1", payload: { x: 1 } });
		const missing = await a.waitFor(
			(f) => f.type === "nack" && f.id === "p1",
			"nack",
		);
		assertEquals(missing.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "missing room",
		});

		a.send({ type: "pub", id: "p2", room: "", payload: { x: 1 } });
		const empty = await a.waitFor((f) => f.type === "nack" && f.id === "p2", "nack");
		assertEquals(empty.error?.code, ERROR_CODE.BAD_REQUEST);

		a.send({ type: "pub", id: "p3", room: "chat", namespace: 42, payload: { x: 1 } });
		const ns = await a.waitFor((f) => f.type === "nack" && f.id === "p3", "nack");
		assertEquals(ns.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "namespace must be a string",
		});

		a.send({ type: "ping" });
		await a.waitFor((f) => f.type === "pong", "pong");
	} finally {
		await a.close();
		await server.stop();
	}
});

Deno.test("broadcast without a usable room is a bad_request", async () => {
	const server = startServer({ allowBroadcast: () => true });
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		a.send({ type: "broadcast", id: "b", payload: { x: 1 } });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "b", "nack");
		assertEquals(nack.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "missing room",
		});
	} finally {
		await a.close();
		await server.stop();
	}
});

Deno.test("auth ignores an unusable clientId or namespace", async () => {
	const server = startServer();
	const a = await rawConnect(server.url);
	const b = await rawConnect(server.url);

	try {
		// A number would have become a registry key verbatim.
		const helloA = await a.auth({ clientId: 123, namespace: "" });
		assertNotEquals(helloA.clientId, "123");
		assert(helloA.clientId, "a generated id is assigned instead");
		assertEquals(helloA.namespace, "default");

		const helloB = await b.auth({ clientId: "", namespace: 42 });
		assert(helloB.clientId);
		assertNotEquals(helloB.clientId, helloA.clientId);
		assertEquals(helloB.namespace, "default");
	} finally {
		await a.close();
		await b.close();
		await server.stop();
	}
});

/** A promise plus the handle that settles it, so a test can hold `verify` open. */
function gate(): { promise: Promise<void>; open: () => void } {
	let open!: () => void;
	const promise = new Promise<void>((resolve) => (open = resolve));
	return { promise, open };
}

Deno.test("two auth frames run verify once and answer with one hello", async () => {
	const verifying = gate();
	let calls = 0;
	const server = startServer({
		verify: async () => {
			calls++;
			await verifying.promise;
			return {};
		},
	});
	const a = await rawConnect(server.url);

	try {
		const auth = { type: "auth", protocol: PROTOCOL_VERSION, payload: null };
		a.send({ ...auth, id: "a1" });
		a.send({ ...auth, id: "a2" });
		// Answered synchronously while `verify` is still pending, so it proves
		// both auth frames have already been dispatched.
		a.send({ type: "ping" });
		await a.waitFor((f) => f.type === "error", "unauthorized error");

		verifying.open();
		await a.waitFor((f) => f.type === "hello", "hello");
		await sleep(50);

		assertEquals(calls, 1);
		assertEquals(a.frames.filter((f) => f.type === "hello").length, 1);
	} finally {
		verifying.open();
		await a.close();
		await server.stop();
	}
});

Deno.test("a socket closed during verify is never registered", async () => {
	const verifying = gate();
	let called = false;
	const server = startServer({
		// Nothing sweeps, so a ghost entry would be permanent rather than
		// merely long-lived.
		idleTimeout: 0,
		verify: async () => {
			called = true;
			await verifying.promise;
			return {};
		},
	});
	const a = await rawConnect(server.url);

	try {
		a.send({ type: "auth", id: "a1", protocol: PROTOCOL_VERSION, payload: null });
		await until(() => called, "verify called");

		await a.close();
		await until(() => server.service.stats().pending === 0, "server saw the close");

		verifying.open();
		await sleep(50);

		assertEquals(server.service.stats().connections, 0);
		assertEquals(server.service.stats().pending, 0);
	} finally {
		verifying.open();
		await a.close();
		await server.stop();
	}
});

Deno.test("an unexpected handler throw closes the socket, not the process", async () => {
	const server = startServer({
		decode: (raw) => {
			const frame = JSON.parse(raw as string);
			if (frame.type !== "boom") return frame;
			// A decoded frame that misbehaves only once the dispatch touches
			// it — the class of bug the backstop exists for.
			return {
				get type(): string {
					throw new Error("boom");
				},
			} as unknown as WSFrame;
		},
	});
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		a.send({ type: "boom" });

		const error = await a.waitFor((f) => f.type === "error", "error frame");
		assertEquals(error.error?.code, ERROR_CODE.INTERNAL);

		const closed = await a.waitClosed();
		assertEquals(closed.code, CLOSE.INTERNAL_ERROR);
		assertEquals(server.service.stats().connections, 0);
	} finally {
		await a.close();
		await server.stop();
	}
});
