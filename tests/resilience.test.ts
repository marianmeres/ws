import { assert, assertEquals, assertRejects } from "@std/assert";

import { createWSClient } from "../src/mod.ts";
import type { WSMessage } from "../src/protocol/frames.ts";
import {
	WSConnectTimeoutError,
	WSDisposedError,
	WSNotConnectedError,
	WSOutboxDropError,
} from "../src/protocol/errors.ts";
import type { ClientFrame } from "../src/protocol/frames.ts";
import { freePort, sleep, startServer, startSilentServer, until } from "./_helpers.ts";

const client = (url: string, options: Record<string, unknown> = {}) =>
	createWSClient({ url, logger: null, pingInterval: 0, ...options });

Deno.test("reconnects after the server dies, and re-subscribes", async () => {
	const port = freePort();
	let server = startServer({}, port);
	const c = client(server.url, { reconnectDelay: 30, reconnectDelayMax: 120 });

	try {
		await c.connect();
		const seen: WSMessage[] = [];
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
		const seen: WSMessage[] = [];
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
		const seen: WSMessage[] = [];
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

		const seen: WSMessage[] = [];
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
