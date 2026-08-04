import { assert, assertEquals, assertRejects } from "@std/assert";
import { backoffDelay } from "../src/client/backoff.ts";
import { Outbox } from "../src/client/outbox.ts";
import { RoomRegistry } from "../src/client/rooms.ts";
import { Heartbeat } from "../src/client/heartbeat.ts";
import {
	WSDisposedError,
	WSOutboxDropError,
	WSTimeoutError,
} from "../src/protocol/errors.ts";
import type { ClientFrame, WSMessage } from "../src/protocol/frames.ts";

const frame = (id: string): ClientFrame => ({
	type: "pub",
	id,
	room: "r",
	payload: null,
});

Deno.test("backoff — grows exponentially and respects the cap", () => {
	const base = 500;
	const max = 30_000;

	// With rnd()=1 we get the upper bound of the equal-jitter window.
	const upper = (n: number) => backoffDelay(n, base, max, () => 1);
	assertEquals(upper(1), 500);
	assertEquals(upper(2), 1000);
	assertEquals(upper(3), 2000);
	assertEquals(upper(4), 4000);

	// The cap is the substantive fix over uncapped exponential growth.
	assertEquals(upper(50), max);
	assertEquals(upper(1000), max);
});

Deno.test("backoff — equal jitter keeps a floor of half the delay", () => {
	const base = 1000;
	const max = 30_000;

	// Full jitter would allow ~0 here, which hammers a recovering server.
	assertEquals(backoffDelay(3, base, max, () => 0), 2000);
	assertEquals(backoffDelay(3, base, max, () => 1), 4000);

	for (let i = 0; i < 200; i++) {
		const d = backoffDelay(3, base, max);
		assert(d >= 2000 && d <= 4000, `jitter escaped its window: ${d}`);
	}
});

Deno.test("outbox — queues while disconnected, drains in FIFO order", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 1_000 });

	const a = outbox.track("a", frame("a"), true);
	const b = outbox.track("b", frame("b"), true);
	assertEquals(outbox.queuedCount, 2);

	const drained = outbox.drain();
	assertEquals(drained.map((f) => (f as { id: string }).id), ["a", "b"]);
	assertEquals(outbox.queuedCount, 0);
	// Draining transmits; it does not resolve. They await acks now.
	assertEquals(outbox.pendingCount, 2);

	outbox.settle("a", 3);
	outbox.settle("b", 0);
	assertEquals((await a).recipients, 3);
	assertEquals((await b).recipients, 0);
	assertEquals(outbox.pendingCount, 0);
});

Deno.test("outbox — cap drops the oldest and rejects its promise", async () => {
	const dropped: ClientFrame[] = [];
	const outbox = new Outbox({
		maxSize: 2,
		sendTimeout: 1_000,
		onDrop: (frames) => dropped.push(...frames),
	});

	const a = outbox.track("a", frame("a"), true);
	const b = outbox.track("b", frame("b"), true);
	const c = outbox.track("c", frame("c"), true);

	// "a" is the oldest, so "a" goes — the freshest state is what survives.
	await assertRejects(() => a, WSOutboxDropError);
	assertEquals(dropped.map((f) => (f as { id: string }).id), ["a"]);
	assertEquals(outbox.droppedCount, 1);
	assertEquals(outbox.queuedCount, 2);

	outbox.settle("b", 1);
	outbox.settle("c", 1);
	await b;
	await c;
});

Deno.test("outbox — the timeout spans the queue, not just the flight", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 60 });
	// Never drained, i.e. never even sent: without a queue-spanning deadline
	// this promise would pend forever behind an infinite reconnect.
	await assertRejects(() => outbox.track("a", frame("a"), true), WSTimeoutError);
	assertEquals(outbox.pendingCount, 0);
});

Deno.test("outbox — failAll settles everything", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 5_000 });
	const a = outbox.track("a", frame("a"), true);
	const b = outbox.track("b", frame("b"), false);

	outbox.failAll(new WSDisposedError());

	await assertRejects(() => a, WSDisposedError);
	await assertRejects(() => b, WSDisposedError);
	assertEquals(outbox.pendingCount, 0);
});

Deno.test("heartbeat — a later ping cannot postpone an unanswered one", async () => {
	// Regression: arming the deadline on every ping used to reset it, so any
	// pingInterval <= pongTimeout reset the timer forever and silently
	// disabled half-open detection altogether.
	const pings: number[] = [];
	let timedOut = false;
	const hb = new Heartbeat({
		interval: 20,
		timeout: 100, // deliberately longer than the interval
		onPing: () => pings.push(Date.now()),
		onTimeout: () => {
			timedOut = true;
		},
	});

	hb.start();
	try {
		await new Promise((r) => setTimeout(r, 300));
		assert(pings.length > 3, "pings should keep firing");
		assert(timedOut, "the deadline must expire despite the ping cadence");
	} finally {
		hb.stop();
	}
});

Deno.test("heartbeat — inbound traffic clears the deadline", async () => {
	let timedOut = false;
	const hb = new Heartbeat({
		interval: 20,
		timeout: 80,
		// Any inbound frame proves liveness, not just a literal pong.
		onPing: () => hb.alive(),
		onTimeout: () => {
			timedOut = true;
		},
	});

	hb.start();
	try {
		await new Promise((r) => setTimeout(r, 250));
		assertEquals(timedOut, false, "a responsive peer must not be dropped");
	} finally {
		hb.stop();
	}
});

Deno.test("rooms — refcounted: N handlers, one wire subscription", () => {
	const rooms = new RoomRegistry();
	const h1 = () => {};
	const h2 = () => {};

	assertEquals(rooms.add("chat", h1).created, true);
	// Second handler must NOT ask for another `sub` frame.
	assertEquals(rooms.add("chat", h2).created, false);

	assertEquals(rooms.remove("chat", h1), false, "still has a handler");
	assertEquals(rooms.remove("chat", h2), true, "last one out unsubscribes");
	assertEquals(rooms.has("chat"), false);
});

Deno.test("rooms — presence upgrade re-subscribes", () => {
	const rooms = new RoomRegistry();
	rooms.add("chat", () => {});
	assertEquals(rooms.wantsPresence("chat"), false);

	const result = rooms.add("chat", () => {}, () => {});
	assertEquals(result.created, false);
	assertEquals(result.presenceUpgraded, true);
	assertEquals(rooms.wantsPresence("chat"), true);

	assertEquals(rooms.subRequests(), [{ room: "chat", presence: true }]);
});

Deno.test("rooms — a handler unsubscribing mid-delivery cannot corrupt it", () => {
	const rooms = new RoomRegistry();
	const seen: string[] = [];

	const first = () => {
		seen.push("first");
		rooms.remove("chat", second);
	};
	const second = () => seen.push("second");

	rooms.add("chat", first);
	rooms.add("chat", second);

	const msg: WSMessage = {
		room: "chat",
		namespace: "default",
		from: null,
		payload: null,
		timestamp: 0,
	};
	rooms.deliver("chat", msg, (e) => {
		throw e;
	});

	// Snapshotted before delivery, so the in-flight iteration is stable.
	assertEquals(seen, ["first", "second"]);
});

Deno.test("rooms — a throwing handler does not stop the others", () => {
	const rooms = new RoomRegistry();
	const errors: unknown[] = [];
	const seen: string[] = [];

	rooms.add("chat", () => {
		throw new Error("boom");
	});
	rooms.add("chat", () => seen.push("survivor"));

	rooms.deliver("chat", {
		room: "chat",
		namespace: "default",
		from: null,
		payload: null,
		timestamp: 0,
	}, (e) => errors.push(e));

	assertEquals(seen, ["survivor"]);
	assertEquals(errors.length, 1);
});
