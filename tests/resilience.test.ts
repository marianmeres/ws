import { assert, assertEquals, assertRejects } from "@std/assert";

import { createWSClient } from "../src/mod.ts";
import { CLOSE } from "../src/protocol/constants.ts";
import type { WSRoomMessage } from "../src/protocol/frames.ts";
import {
	WSConnectionLostError,
	WSConnectTimeoutError,
	WSDisposedError,
	WSNotConnectedError,
	WSOutboxDropError,
	WSRemoteError,
	WSTerminatedError,
	WSTimeoutError,
} from "../src/protocol/errors.ts";
import type { ClientFrame } from "../src/protocol/frames.ts";
import {
	freePort,
	sleep,
	startBadNackServer,
	startNoAckServer,
	startServer,
	startSilentServer,
	until,
} from "./_helpers.ts";

const client = (url: string, options: Record<string, unknown> = {}) =>
	createWSClient({ url, logger: null, pingInterval: 0, ...options });

Deno.test("reconnects after the server dies, and re-subscribes", async () => {
	const port = freePort();
	let server = startServer({}, port);
	const c = client(server.url, { reconnectDelay: 30, reconnectDelayMax: 120 });

	try {
		await c.connect();
		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		await server.stop();
		await until(() => !c.connected, "client notices the drop");

		server = startServer({}, port);
		await until(() => c.connected, "client reconnects", 8_000);

		// The room must be re-established without the caller doing anything.
		await until(
			() => server.service.members("chat").length === 1,
			"rooms are re-subscribed",
		);

		await server.service.publish("chat", { text: "after restart" });
		await until(() => seen.length === 1, "delivery resumes");
		assertEquals(seen[0].payload, { text: "after restart" });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("buffered publishes flush *after* re-subscribe, not before", async () => {
	const port = freePort();
	let server = startServer({}, port);
	const c = client(server.url, { reconnectDelay: 30, reconnectDelayMax: 120 });

	try {
		await c.connect();
		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		await server.stop();
		await until(() => !c.connected, "client notices the drop");

		// Issued while there is no connection at all.
		const pending = c.publish("chat", { text: "buffered" });

		server = startServer({}, port);

		// This assertion is the whole point: a recipient count of 1 can only
		// happen if the re-subscribe reached the server *before* the flushed
		// publish did. Flush-first would score 0 and deliver to nobody.
		const { recipients } = await pending;
		assertEquals(recipients, 1, "publish overtook its own re-subscribe");

		await until(() => seen.length === 1, "buffered message is delivered");
		assertEquals(seen[0].payload, { text: "buffered" });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a sub lost to a dropped socket resolves and keeps its room", async () => {
	const port = freePort();
	let server = startServer({}, port);
	// A short deadline on purpose: if the lost `sub` were left to its timeout,
	// this test would fail in 400ms instead of hanging for the default 30s.
	const c = client(server.url, {
		reconnectDelay: 30,
		reconnectDelayMax: 120,
		sendTimeout: 400,
	});

	try {
		await c.connect();

		// The socket is already on its way out, but the client still reads
		// "open", so the `sub` goes out and nobody is left to acknowledge it.
		const stopping = server.stop();
		const seen: WSRoomMessage[] = [];
		const subscribed = c.subscribe("chat", (m) => seen.push(m));
		await stopping;

		// The rejection this used to produce ran subscribe()'s catch, which
		// detaches the handler — behind the back of a reconnect that had already
		// re-subscribed the room.
		await subscribed;
		assertEquals(c.rooms, ["chat"]);

		server = startServer({}, port);
		await until(() => c.connected, "client reconnects", 8_000);
		await until(
			() => server.service.members("chat").length === 1,
			"the room is live on the server too",
		);

		await server.service.publish("chat", { text: "still here" });
		await until(() => seen.length === 1, "the handler survived");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("an in-flight publish rejects at the close, not at the timeout", async () => {
	// This server closes the socket instead of acking the `pub`.
	const noAck = startNoAckServer();
	const c = client(noAck.url, { sendTimeout: 10_000, reconnectDelay: 10_000 });

	try {
		await c.connect();

		const started = Date.now();
		await assertRejects(
			() => c.publish("chat", { n: 1 }),
			WSConnectionLostError,
		);
		const elapsed = Date.now() - started;
		assert(
			elapsed < 2_000,
			`waited ${elapsed}ms — that is the timeout, not the close`,
		);
	} finally {
		c.dispose();
		await noAck.stop();
	}
});

Deno.test("an in-flight acked send rejects at the close; an unacked one already resolved", async () => {
	// This server closes the socket instead of answering the `msg`.
	const noAck = startNoAckServer();
	const c = client(noAck.url, { sendTimeout: 10_000, reconnectDelay: 10_000 });

	try {
		await c.connect();

		const started = Date.now();
		await assertRejects(() => c.send({ n: 1 }, { ack: true }), WSConnectionLostError);
		const elapsed = Date.now() - started;
		assert(
			elapsed < 2_000,
			`waited ${elapsed}ms — that is the timeout, not the close`,
		);
	} finally {
		c.dispose();
		await noAck.stop();
	}
});

Deno.test("a send buffered while offline resolves once flushed, and arrives", async () => {
	const port = freePort();
	const received: unknown[] = [];
	const options = {
		onMessage: (_: unknown, payload: unknown) => void received.push(payload),
	};
	let server = startServer(options, port);
	const c = client(server.url, { reconnectDelay: 30, reconnectDelayMax: 120 });

	try {
		await c.connect();
		await server.stop();
		await until(() => !c.connected, "client notices the drop");

		// Issued while there is no connection at all: nothing can be written,
		// so neither can resolve yet.
		let settled = false;
		const fire = c.send({ n: 1 }).then(() => settled = true);
		const ask = c.send({ n: 2 }, { ack: true });
		await sleep(100);
		assertEquals(settled, false, "resolved before it was ever written");

		server = startServer(options, port);
		await fire;
		await ask;
		assertEquals(received, [{ n: 1 }, { n: 2 }], "flushed in order");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("an ignored fire-and-forget send that fails is not an unhandled rejection", async () => {
	// Nothing listens on this port, so the send can only time out in the queue.
	const c = client(`ws://127.0.0.1:${freePort()}/ws`, {
		sendTimeout: 50,
		reconnectDelay: 10_000,
	});

	try {
		// Deliberately neither awaited nor caught — the way fire-and-forget
		// invites it to be called. An unhandled rejection fails this test (and
		// would exit a Deno or Node process).
		c.send({ n: 1 });
		await sleep(150);

		// Awaiting one still reports the failure.
		await assertRejects(() => c.send({ n: 2 }), WSTimeoutError);
	} finally {
		c.dispose();
	}
});

Deno.test("a publish after a terminal close rejects at once", async () => {
	const server = startServer({ verify: () => null });
	const c = client(server.url, { sendTimeout: 10_000 });

	try {
		await assertRejects(() => c.connect(), WSTerminatedError);
		assertEquals(c.connectionState, "terminated");

		// Nothing restarts from `terminated`, so buffering this frame would
		// hand the caller a timeout 10s later instead of the actual reason.
		const started = Date.now();
		const error = await assertRejects(
			() => c.publish("chat", { n: 1 }),
			WSTerminatedError,
		);
		assertEquals(error.code, 4001);
		const elapsed = Date.now() - started;
		assert(elapsed < 1_000, `waited ${elapsed}ms for an answer already known`);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a payload that cannot be encoded rejects with the encoder's error", async () => {
	const server = startServer();
	const c = client(server.url, { sendTimeout: 10_000 });

	try {
		await c.connect();

		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));

		// The frame never left, so no ack was ever requested — waiting for one
		// would report a timeout and hide the real cause.
		const error = await assertRejects(
			() => c.publish("chat", { big: 10n }),
			Error,
		);
		assert(
			!(error instanceof WSTimeoutError),
			`expected the encoder's error, got ${error.constructor.name}`,
		);
		assert(
			/BigInt/i.test(error.message),
			`message should name the culprit, got "${error.message}"`,
		);
		assertEquals(errors.length, 1, "still surfaces as an error event");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("detects a half-open connection via the pong deadline", async () => {
	// The server completes the handshake and then answers nothing — no pong,
	// no close frame. Without an application-level probe the client would sit
	// here looking connected, forever.
	const silent = startSilentServer();
	const c = client(silent.url, {
		pingInterval: 80,
		pongTimeout: 120,
		reconnectDelay: 30,
		reconnectDelayMax: 60,
	});

	try {
		const closes: Array<{ code: number; willReconnect: boolean }> = [];
		c.on("close", (e) => closes.push(e));

		await c.connect();
		assert(c.connected, "handshake should succeed before going silent");

		await until(() => closes.length >= 2, "half-open detected repeatedly", 6_000);
		assertEquals(closes[0].code, 4008);
		assert(closes[0].willReconnect, "a dead socket must be retried");
	} finally {
		c.dispose();
		await silent.stop();
	}
});

Deno.test("server reaps a connection that stops pinging", async () => {
	const server = startServer({ idleTimeout: 400 });
	// pingInterval 0 makes this client a zombie by construction.
	const c = client(server.url, { pingInterval: 0, reconnectDelay: 10_000 });

	try {
		const closes: Array<{ code: number; willReconnect: boolean }> = [];
		c.on("close", (e) => closes.push(e));

		await c.connect();
		await until(() => closes.length >= 1, "server reaps the zombie", 5_000);

		assertEquals(closes[0].code, 4008);
		assert(closes[0].willReconnect, "an idle reap is recoverable, not terminal");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a local disconnect() emits close 4900, willReconnect false", async () => {
	const server = startServer();
	const c = client(server.url, { reconnectDelay: 30 });

	try {
		const closes: Array<{ code: number; reason: string; willReconnect: boolean }> =
			[];
		c.on("close", (e) => closes.push(e));

		await c.connect();
		c.disconnect();

		assertEquals(closes.length, 1, "a closed socket is one close event");
		assertEquals(closes[0].code, 4900);
		assertEquals(closes[0].willReconnect, false);

		c.disconnect();
		assertEquals(closes.length, 1, "nothing to close while idle, nothing to report");

		// The superseded socket's own onclose must not arrive late as a second one.
		await sleep(150);
		assertEquals(closes.length, 1);
	} finally {
		c.dispose();
		await server.stop();
	}
});

/** A logger that records every warning and error, for asserting there were none. */
function recordingLogger(): { logger: Record<string, unknown>; lines: string[] } {
	const lines: string[] = [];
	const record = (level: string) => (...args: unknown[]) =>
		lines.push(`${level}: ${args.map(String).join(" ")}`);
	return {
		logger: {
			debug: () => {},
			info: () => {},
			warn: record("warn"),
			error: record("error"),
		},
		lines,
	};
}

Deno.test("reconnect: false — a lost connection ends idle, quietly, and connect() resumes it", async () => {
	const port = freePort();
	let server = startServer({}, port);
	const { logger, lines } = recordingLogger();
	const c = client(server.url, { reconnect: false, reconnectDelay: 30, logger });

	try {
		const closes: Array<{ code: number; willReconnect: boolean }> = [];
		const reconnecting: unknown[] = [];
		const terminated: unknown[] = [];
		c.on("close", (e) => closes.push(e));
		c.on("reconnecting", (e) => reconnecting.push(e));
		c.on("terminated", (e) => terminated.push(e));

		await c.connect();
		await server.stop();
		await until(() => c.connectionState === "idle", "the client settles");

		// The close is reported honestly — nothing is coming — and classified as
		// neither a retry nor a failure.
		assertEquals(closes.map((e) => [e.code, e.willReconnect]), [[1001, false]]);
		assertEquals(reconnecting, [], "no retry may be scheduled");
		assertEquals(terminated, [], "declining to retry is not a terminal failure");
		await sleep(150);
		assertEquals(c.connectionState, "idle");
		assertEquals(c.dump().attempt, 0);
		// The whole point over `on("close", () => disconnect())`: no bogus
		// "illegal state transition" warning, and no error-level line either.
		assertEquals(lines, []);

		// Idle is resumable, like after disconnect(): the caller decides when.
		server = startServer({}, port);
		await c.connect();
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("reconnect: false — a pending connect() rejects when the close is not retried", async () => {
	// Nothing listens here. With retries the await would simply wait for the
	// server; without them there is nothing to wait for, and it must say so.
	const c = client(`ws://127.0.0.1:${freePort()}/ws`, { reconnect: false });

	try {
		const error = await assertRejects(() => c.connect(), WSTerminatedError);
		// The code of a refused connection is the runtime's to report (Deno
		// says 0, browsers 1006); what matters is that it is not a retry.
		assert(typeof error.code === "number");
		assertEquals(c.connectionState, "idle", "declined, not terminated");
	} finally {
		c.dispose();
	}
});

Deno.test("a reconnect function sees code, reason and attempt, and its verdict is honoured", async () => {
	const port = freePort();
	const server = startServer({}, port);
	const seen: Array<{ code: number; attempt: number }> = [];
	const c = client(server.url, {
		reconnectDelay: 30,
		reconnectDelayMax: 60,
		// One retry, then give up.
		reconnect: ({ code, attempt }: { code: number; attempt: number }) => {
			seen.push({ code, attempt });
			return attempt < 1;
		},
	});

	try {
		await c.connect();
		await server.stop();
		await until(() => c.connectionState === "idle", "the client gives up", 5_000);

		// First close: the working connection went away, no attempts failed yet,
		// so the policy allows one retry. That retry finds nobody listening, and
		// the policy, now at attempt 1, declines. (The code of the refused
		// connection is the runtime's: Deno reports 0, browsers 1006.)
		assertEquals(seen.length, 2);
		assertEquals(seen[0], { code: 1001, attempt: 0 });
		assertEquals(seen[1].attempt, 1);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("the reconnect policy is not consulted for a terminal close", async () => {
	const server = startServer({ verify: () => null });
	let consulted = 0;
	const c = client(server.url, {
		reconnect: () => {
			consulted++;
			return true;
		},
	});

	try {
		// `terminalCloseCodes` decides first; a policy cannot talk the client
		// into retrying rejected credentials.
		const error = await assertRejects(() => c.connect(), WSTerminatedError);
		assertEquals(error.code, 4001);
		assertEquals(c.connectionState, "terminated");
		assertEquals(consulted, 0);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a stale auth() rejection leaves the newer socket alone", async () => {
	const server = startServer();
	let attempt = 0;
	const c = client(server.url, {
		reconnectDelay: 30,
		auth: async () => {
			attempt++;
			// The first attempt is still awaiting its token when the socket it
			// belongs to is superseded; the rest answer at once.
			if (attempt === 1) {
				await sleep(300);
				throw new Error("stale token refresh failed");
			}
			return null;
		},
	});

	try {
		const closes: number[] = [];
		c.on("close", (e) => closes.push(e.code));
		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));

		// disconnect() below rejects this one; nothing else awaits it.
		c.connect().catch(() => {});
		await until(
			() => c.connectionState === "authenticating",
			"the first socket reaches auth",
		);

		c.disconnect();
		await c.connect();
		assert(c.connected, "the second socket completed its handshake");

		// The stale auth() rejects in here, two generations too late.
		await sleep(400);
		assertEquals(c.connectionState, "open", "the newer socket survived");
		assertEquals(closes, [4900], "only the disconnect() closed anything");
		assertEquals(errors, [], "a superseded attempt is not the caller's problem");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("outbox overflow drops the oldest and rejects it", async () => {
	// Nothing is listening here, so everything queues.
	const unreachable = `ws://127.0.0.1:${freePort()}/ws`;
	const dropped: ClientFrame[] = [];
	const c = client(unreachable, {
		autoConnect: false,
		outboxMaxSize: 2,
		sendTimeout: 3_000,
		onOutboxDrop: (frames: ClientFrame[]) => dropped.push(...frames),
	});

	try {
		const first = c.publish("chat", { n: 1 });
		const second = c.publish("chat", { n: 2 });
		const third = c.publish("chat", { n: 3 });

		await assertRejects(() => first, WSOutboxDropError);
		assertEquals(dropped.length, 1);
		assertEquals((dropped[0] as { payload: unknown }).payload, { n: 1 });

		// The survivors settle on dispose rather than pending to their timeout.
		c.dispose();
		await assertRejects(() => second, WSDisposedError);
		await assertRejects(() => third, WSDisposedError);
	} finally {
		c.dispose();
	}
});

Deno.test("outboxMaxSize 0 rejects immediately instead of buffering", async () => {
	const unreachable = `ws://127.0.0.1:${freePort()}/ws`;
	const c = client(unreachable, { autoConnect: false, outboxMaxSize: 0 });
	try {
		await assertRejects(
			() => c.publish("chat", { n: 1 }),
			WSNotConnectedError,
		);
	} finally {
		c.dispose();
	}
});

Deno.test("connectTimeout bounds the await but not the retrying", async () => {
	const unreachable = `ws://127.0.0.1:${freePort()}/ws`;
	const c = client(unreachable, {
		connectTimeout: 200,
		reconnectDelay: 40,
		reconnectDelayMax: 80,
	});

	try {
		await assertRejects(() => c.connect(), WSConnectTimeoutError);
		// Rejecting the await must not stop the machine.
		assert(
			["connecting", "reconnecting"].includes(c.connectionState),
			`still trying, got "${c.connectionState}"`,
		);
		await until(() => c.dump().attempt as number > 1, "attempts keep climbing");
	} finally {
		c.dispose();
	}
});

Deno.test("disconnect() is resumable — handlers and rooms survive", async () => {
	const server = startServer();
	const c = client(server.url, { reconnectDelay: 30 });

	try {
		await c.connect();
		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		c.disconnect();
		assertEquals(c.connected, false);
		// This is the SSEClient behaviour we deliberately did not copy: there,
		// disconnect() removed every listener and a later connect() delivered
		// to nobody.
		assertEquals(c.rooms, ["chat"]);

		await c.connect();
		await until(
			() => server.service.members("chat").length === 1,
			"rooms are restored",
		);

		await server.service.publish("chat", { text: "resumed" });
		await until(() => seen.length === 1, "the original handler still fires");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("unsub() then dispose() leaves no uncaught rejection", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		await c.connect();
		const unsub = await c.subscribe("chat", () => {});

		// The README's canonical sequence. The unsubscriber sends an `unsub` and
		// drops the promise; dispose() then rejects it with WSDisposedError.
		// Uncaught, that exits a Deno or Node process.
		unsub();
		c.dispose();

		// A checkpoint at which the runtime would report the rejection.
		await sleep(50);
		assertEquals(c.rooms, []);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("connect() is idempotent and shares one promise", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		const [a, b, d] = await Promise.all([
			c.connect(),
			c.connect(),
			c.connect(),
		]);
		assertEquals([a, b, d], [undefined, undefined, undefined]);
		assert(c.connected);
		// Already connected: resolves immediately rather than reconnecting.
		await c.connect();
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a replaced connection does not evict its replacement", async () => {
	const server = startServer();
	const first = client(server.url, { clientId: "same-id" });
	const second = client(server.url, { clientId: "same-id" });

	try {
		await first.connect();
		await second.connect();

		// The newcomer wins; the stale socket is closed. Without the identity
		// check on teardown, the loser's close would delete the winner.
		await until(
			() => server.service.stats().connections === 1,
			"exactly one connection survives",
		);

		const seen: WSRoomMessage[] = [];
		await second.subscribe("chat", (m) => seen.push(m));
		await server.service.publish("chat", { text: "to the survivor" });
		await until(() => seen.length === 1, "the survivor still works");
	} finally {
		first.dispose();
		second.dispose();
		await server.stop();
	}
});

Deno.test("state store emits immediately and on every change", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		const states: string[] = [];
		const unsub = c.state.subscribe((s) => states.push(s.state));

		// Svelte store contract: fires synchronously with the current value.
		assertEquals(states, ["idle"]);

		await c.connect();
		assert(states.includes("connecting"));
		assert(states.includes("open"));

		unsub();
		const count = states.length;
		c.disconnect();
		assertEquals(states.length, count, "unsubscribed observer still notified");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a replaced connection ends terminated with 4005 and does not fight back", async () => {
	const server = startServer();
	// Short backoff: were the loser to reconnect, it would do so many times
	// within the window below, and the survivor would not survive.
	const first = client(server.url, {
		clientId: "same-id",
		reconnectDelay: 10,
		reconnectDelayMax: 30,
	});
	const second = client(server.url, {
		clientId: "same-id",
		reconnectDelay: 10,
		reconnectDelayMax: 30,
	});

	try {
		await first.connect();
		const closed = new Promise<{ code: number; willReconnect: boolean }>((r) =>
			first.once("close", r)
		);
		const terminated = new Promise<{ code: number }>((r) =>
			first.once("terminated", r)
		);

		await second.connect();

		const ev = await closed;
		assertEquals(ev.code, CLOSE.REPLACED);
		assertEquals(ev.willReconnect, false);
		assertEquals((await terminated).code, CLOSE.REPLACED);
		assertEquals(first.connectionState, "terminated");

		// Several backoff periods later, the newcomer is still the one
		// connection — the loser did not come back to evict it.
		await sleep(250);
		assert(second.connected, "the newcomer keeps its connection");
		assertEquals(server.service.stats().connections, 1);
		assertEquals(first.connectionState, "terminated");

		// A later, explicit connect() is still allowed — the user may know
		// the other tab is gone — and evicts the other side in turn.
		await first.connect();
		await until(() => second.connectionState === "terminated", "roles swap");
		assertEquals(server.service.stats().connections, 1);
	} finally {
		first.dispose();
		second.dispose();
		await server.stop();
	}
});

Deno.test("a send from a connected handler or a state subscriber lands after the re-subscribe", async () => {
	const server = startServer();
	const c = client(server.url, { autoConnect: false });

	try {
		const seen: WSRoomMessage[] = [];
		// Registered while idle: established by the re-subscribe step.
		await c.subscribe("chat", (m) => seen.push(m));

		let fromHandler = -1;
		c.on("connected", () => {
			c.publish("chat", { via: "handler" }).then((
				r,
			) => (fromHandler = r.recipients));
		});
		let fromStore = -1;
		let armed = true;
		c.state.subscribe((s) => {
			if (!s.connected || !armed) return;
			armed = false;
			c.publish("chat", { via: "store" }).then((r) => (fromStore = r.recipients));
		});

		await c.connect();
		await until(() => fromHandler !== -1 && fromStore !== -1, "both publishes acked");

		// Had either gone out before the `sub`, the server would have had no
		// subscriber to hand it to — and the client would miss its own echo.
		assertEquals(fromHandler, 1);
		assertEquals(fromStore, 1);
		await until(() => seen.length === 2, "both echoes");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a throwing event handler is reported as `error`, and a throwing `error` handler does not recurse", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		const errors: Error[] = [];
		c.on("error", (e) => {
			errors.push(e);
			throw new Error("the error handler throws too");
		});
		c.on("connected", () => {
			throw new Error("boom");
		});

		await c.connect();
		assertEquals(errors.map((e) => e.message), ["boom"]);
		// The throw cost the handler, not the connection.
		assert(c.connected);
		assertEquals(await c.publish("x", 1), { recipients: 0 });
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a malformed nack or error frame costs that frame, not the process", async () => {
	const server = startBadNackServer();
	const c = client(server.url, { sendTimeout: 2_000 });

	try {
		const errors: Error[] = [];
		c.on("error", (e) => errors.push(e));

		// `nack` without `error`: the send still gets an answer — a rejection
		// with a usable WSRemoteError — instead of a throw out of `onmessage`.
		const err = await assertRejects(
			() => c.send({ op: "x" }, { ack: true }),
			WSRemoteError,
		);
		assertEquals(err.code, "unknown");
		assert(c.connected);

		// `error` without `error`: an `error` event, and the connection stays.
		await c.send({ op: "fire" });
		await until(() => errors.length === 1, "error event");
		assert(errors[0] instanceof WSRemoteError);
		assert(c.connected);
	} finally {
		c.dispose();
		await server.stop();
	}
});
