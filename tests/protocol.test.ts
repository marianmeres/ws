/**
 * Server input hardening, exercised with raw sockets.
 *
 * The stock client cannot produce these frames; a hostile or buggy one can, and
 * every case here used to be either a crash or a silently accepted nonsense
 * value.
 */

import assert from "node:assert/strict";
import { test } from "node:test";

import { CLOSE, ERROR_CODE, PROTOCOL_VERSION } from "../src/protocol/constants.ts";
import type { WSFrame } from "../src/protocol/frames.ts";
import type { Logger } from "@marianmeres/clog";
import {
	connectNonReadingPeer,
	rawConnect,
	sleep,
	startServer,
	until,
} from "./_helpers.ts";

test("a non-array `rooms` on sub is a bad_request, not a crash", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);
	const b = await rawConnect(server.url);

	try {
		await a.auth();
		await b.auth();

		// `for…of` over a number rejected the message promise, and an unhandled
		// rejection takes the whole Node or Deno process down with it.
		a.send({ type: "sub", id: "x", rooms: 5 });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "x", "nack");
		assert.deepEqual(nack.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "rooms must be an array",
		});

		// The offending socket survives its own mistake...
		a.send({ type: "ping" });
		await a.waitFor((f) => f.type === "pong", "pong");
		assert.deepEqual(a.closed, null);

		// ...and so does everybody else.
		b.send({ type: "sub", id: "s", rooms: [{ room: "chat" }] });
		await b.waitFor((f) => f.type === "ack" && f.id === "s", "ack");
	} finally {
		await a.close();
		await b.close();
		await server.stop();
	}
});

test("a non-array `rooms` on unsub is a bad_request", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		a.send({ type: "unsub", id: "u", rooms: "chat" });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "u", "nack");
		assert.deepEqual(nack.error, {
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

test("sub skips malformed room entries and acks the rest", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);

	try {
		const hello = await a.auth();
		a.send({
			type: "sub",
			id: "s",
			rooms: [7, null, { room: "" }, { room: 42 }, { room: "chat" }],
		});

		await a.waitFor((f) => f.type === "ack" && f.id === "s", "ack");
		assert.deepEqual(server.service.members("chat"), [hello.clientId]);
	} finally {
		await a.close();
		await server.stop();
	}
});

test("pub without a usable room is a bad_request", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);

	try {
		await a.auth();

		a.send({ type: "pub", id: "p1", payload: { x: 1 } });
		const missing = await a.waitFor(
			(f) => f.type === "nack" && f.id === "p1",
			"nack",
		);
		assert.deepEqual(missing.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "missing room",
		});

		a.send({ type: "pub", id: "p2", room: "", payload: { x: 1 } });
		const empty = await a.waitFor((f) => f.type === "nack" && f.id === "p2", "nack");
		assert.deepEqual(empty.error?.code, ERROR_CODE.BAD_REQUEST);

		a.send({ type: "pub", id: "p3", room: "chat", namespace: 42, payload: { x: 1 } });
		const ns = await a.waitFor((f) => f.type === "nack" && f.id === "p3", "nack");
		assert.deepEqual(ns.error, {
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

test("broadcast without a usable room is a bad_request", async () => {
	const server = await startServer({ allowBroadcast: () => true });
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		a.send({ type: "broadcast", id: "b", payload: { x: 1 } });

		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "b", "nack");
		assert.deepEqual(nack.error, {
			code: ERROR_CODE.BAD_REQUEST,
			message: "missing room",
		});
	} finally {
		await a.close();
		await server.stop();
	}
});

test("auth ignores an unusable clientId or namespace", async () => {
	const server = await startServer();
	const a = await rawConnect(server.url);
	const b = await rawConnect(server.url);

	try {
		// A number would have become a registry key verbatim.
		const helloA = await a.auth({ clientId: 123, namespace: "" });
		assert.notDeepEqual(helloA.clientId, "123");
		assert(helloA.clientId, "a generated id is assigned instead");
		assert.deepEqual(helloA.namespace, "default");

		const helloB = await b.auth({ clientId: "", namespace: 42 });
		assert(helloB.clientId);
		assert.notDeepEqual(helloB.clientId, helloA.clientId);
		assert.deepEqual(helloB.namespace, "default");
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

test("two auth frames run verify once and answer with one hello", async () => {
	const verifying = gate();
	let calls = 0;
	const server = await startServer({
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

		assert.deepEqual(calls, 1);
		assert.deepEqual(a.frames.filter((f) => f.type === "hello").length, 1);
	} finally {
		verifying.open();
		await a.close();
		await server.stop();
	}
});

test("a socket closed during verify is never registered", async () => {
	const verifying = gate();
	let called = false;
	const server = await startServer({
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

		assert.deepEqual(server.service.stats().connections, 0);
		assert.deepEqual(server.service.stats().pending, 0);
	} finally {
		verifying.open();
		await a.close();
		await server.stop();
	}
});

test("an unexpected handler throw closes the socket, not the process", async () => {
	const server = await startServer({
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
		assert.deepEqual(error.error?.code, ERROR_CODE.INTERNAL);

		const closed = await a.waitClosed();
		assert.deepEqual(closed.code, CLOSE.INTERNAL_ERROR);
		assert.deepEqual(server.service.stats().connections, 0);
	} finally {
		await a.close();
		await server.stop();
	}
});

test("a sub past maxRoomsPerConnection is refused whole; re-subscribing is free", async () => {
	const server = await startServer({ maxRoomsPerConnection: 3 });
	const a = await rawConnect(server.url);

	try {
		const hello = await a.auth();
		a.send({ type: "sub", id: "s1", rooms: [{ room: "a" }, { room: "b" }] });
		await a.waitFor((f) => f.type === "ack" && f.id === "s1", "ack s1");

		// Two held plus two more is four: nothing of this frame is applied.
		a.send({ type: "sub", id: "s2", rooms: [{ room: "c" }, { room: "d" }] });
		const nack = await a.waitFor(
			(f) => f.type === "nack" && f.id === "s2",
			"nack s2",
		);
		assert.deepEqual(nack.error?.code, ERROR_CODE.FORBIDDEN);
		assert.deepEqual(nack.error?.details, { limit: 3 });
		assert.deepEqual(server.service.members("c"), []);
		assert.deepEqual(server.service.members("d"), []);

		// A room already held does not count again; a third fits exactly.
		a.send({
			type: "sub",
			id: "s3",
			rooms: [{ room: "a" }, { room: "a" }, { room: "c" }],
		});
		await a.waitFor((f) => f.type === "ack" && f.id === "s3", "ack s3");
		assert.deepEqual(server.service.members("c"), [hello.clientId]);

		a.send({ type: "sub", id: "s4", rooms: [{ room: "e" }] });
		await a.waitFor((f) => f.type === "nack" && f.id === "s4", "nack s4");

		// Leaving one makes room for one.
		a.send({ type: "unsub", id: "u", rooms: ["a"] });
		await a.waitFor((f) => f.type === "ack" && f.id === "u", "ack u");
		a.send({ type: "sub", id: "s5", rooms: [{ room: "e" }] });
		await a.waitFor((f) => f.type === "ack" && f.id === "s5", "ack s5");
		assert.deepEqual(a.closed, null);
	} finally {
		await a.close();
		await server.stop();
	}
});

test("a room name over maxRoomNameLength is a bad_request on sub, pub and broadcast", async () => {
	const server = await startServer({ maxRoomNameLength: 8, allowBroadcast: () => true });
	const a = await rawConnect(server.url);
	const long = "x".repeat(9);

	try {
		await a.auth();
		a.send({ type: "sub", id: "s", rooms: [{ room: "fine" }, { room: long }] });
		const s = await a.waitFor((f) => f.type === "nack" && f.id === "s", "nack s");
		assert.deepEqual(s.error?.code, ERROR_CODE.BAD_REQUEST);
		assert.deepEqual(s.error?.details, { limit: 8 });
		// Refused whole: the well-formed entry before it was not applied either.
		assert.deepEqual(server.service.members("fine"), []);

		a.send({ type: "pub", id: "p", room: long, payload: 1 });
		const p = await a.waitFor((f) => f.type === "nack" && f.id === "p", "nack p");
		assert.deepEqual(p.error?.code, ERROR_CODE.BAD_REQUEST);

		a.send({ type: "broadcast", id: "b", room: long, payload: 1 });
		const b = await a.waitFor((f) => f.type === "nack" && f.id === "b", "nack b");
		assert.deepEqual(b.error?.code, ERROR_CODE.BAD_REQUEST);

		a.send({ type: "sub", id: "ok", rooms: [{ room: "x".repeat(8) }] });
		await a.waitFor((f) => f.type === "ack" && f.id === "ok", "ack ok");
		assert.deepEqual(a.closed, null);
	} finally {
		await a.close();
		await server.stop();
	}
});

test("allowSubscribe: refused rooms are named, the rest are applied, a throw refuses", async () => {
	const server = await startServer({
		allowSubscribe: (_ctx, room) => {
			if (room === "boom") throw new Error("policy bug");
			return !room.startsWith("private");
		},
	});
	const a = await rawConnect(server.url);

	try {
		const hello = await a.auth();
		a.send({
			type: "sub",
			id: "s",
			rooms: [
				{ room: "public", presence: true },
				{ room: "private-1" },
				{ room: "boom" },
				{ room: "private-2" },
			],
		});
		const nack = await a.waitFor((f) => f.type === "nack" && f.id === "s", "nack s");
		assert.deepEqual(nack.error?.code, ERROR_CODE.FORBIDDEN);
		assert.deepEqual(nack.error?.details, {
			refused: ["private-1", "boom", "private-2"],
		});

		// The allowed room is live — its presence sync even preceded the nack.
		assert.deepEqual(server.service.members("public"), [hello.clientId]);
		assert.deepEqual(server.service.members("private-1"), []);
		assert.deepEqual(server.service.members("boom"), []);
		const sync = a.frames.find((f) => f.type === "presence");
		assert.deepEqual(sync?.room, "public");
		assert(a.frames.indexOf(sync!) < a.frames.indexOf(nack));

		// A room already held is not put to the policy again: a re-sub for
		// presence on it is acked even if the policy would now refuse.
		a.send({ type: "sub", id: "again", rooms: [{ room: "public", presence: true }] });
		await a.waitFor((f) => f.type === "ack" && f.id === "again", "ack again");
	} finally {
		await a.close();
		await server.stop();
	}
});

test("an async allowSubscribe still keeps a following pub behind the sub", async () => {
	const server = await startServer({
		allowSubscribe: async () => {
			await sleep(60);
			return true;
		},
	});
	const a = await rawConnect(server.url);

	try {
		await a.auth();
		// Back to back, as the client does after every reconnect: the sub's
		// policy is still deciding when the pub arrives.
		a.send({ type: "sub", id: "s", rooms: [{ room: "r" }] });
		a.send({ type: "pub", id: "p", room: "r", payload: { late: true } });

		const ack = await a.waitFor((f) => f.type === "ack" && f.id === "p", "ack p");
		assert.deepEqual(ack.recipients, 1, "the pub waited for the sub to be registered");
		const msg = await a.waitFor(
			(f) => f.type === "msg" && f.room === "r",
			"own echo",
		);
		assert.deepEqual(msg.payload, { late: true });
		// And the sub was answered first.
		assert(
			a.frames.findIndex((f) => f.id === "s") <
				a.frames.findIndex((f) => f.id === "p"),
		);
	} finally {
		await a.close();
		await server.stop();
	}
});

test("allowPublish: a refused pub is nacked forbidden and delivers nothing", async () => {
	const server = await startServer({ allowPublish: (_ctx, room) => room !== "readonly" });
	const a = await rawConnect(server.url);
	const b = await rawConnect(server.url);

	try {
		await a.auth();
		await b.auth();
		a.send({ type: "sub", id: "s", rooms: [{ room: "readonly" }, { room: "open" }] });
		await a.waitFor((f) => f.type === "ack" && f.id === "s", "ack s");

		b.send({ type: "pub", id: "p1", room: "readonly", payload: 1 });
		const nack = await b.waitFor(
			(f) => f.type === "nack" && f.id === "p1",
			"nack p1",
		);
		assert.deepEqual(nack.error?.code, ERROR_CODE.FORBIDDEN);

		b.send({ type: "pub", id: "p2", room: "open", payload: 2 });
		const ack = await b.waitFor((f) => f.type === "ack" && f.id === "p2", "ack p2");
		assert.deepEqual(ack.recipients, 1);

		await a.waitFor((f) => f.type === "msg" && f.room === "open", "open delivery");
		assert.deepEqual(a.frames.filter((f) => f.type === "msg").length, 1);
	} finally {
		await a.close();
		await b.close();
		await server.stop();
	}
});

test("a peer that stops reading is closed as a slow consumer, not buffered forever", async () => {
	const warnings: string[] = [];
	const server = await startServer({
		maxBufferedAmount: 256 * 1024,
		logger: { warn: (m: string) => warnings.push(String(m)) } as unknown as Logger,
	});
	const peer = await connectNonReadingPeer(server.url, { clientId: "sloth" });

	try {
		await until(() => server.service.stats().connections === 1, "authenticated");

		// The peer never reads again. The kernel absorbs the first few hundred
		// KiB; after that the server's own queue grows — until it does not.
		const chunk = "x".repeat(64 * 1024);
		let sends = 0;
		await until(
			() => {
				if (server.service.stats().connections === 0) return true;
				server.service.send("sloth", chunk);
				sends++;
				return false;
			},
			"slow consumer reaped",
			5_000,
			5,
		);

		assert.deepEqual(server.service.stats().connections, 0);
		assert(sends < 200, `gave up after ${sends} sends, not never`);
		assert(
			warnings.some((w) => w.includes("slow consumer sloth")),
			`warned about the slow consumer: ${warnings.join(" | ")}`,
		);
	} finally {
		peer.close();
		await server.stop();
	}
});
