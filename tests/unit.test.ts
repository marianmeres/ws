import assert from "node:assert/strict";
import { test } from "node:test";
import { backoffDelay } from "../src/client/backoff.ts";
import { Outbox } from "../src/client/outbox.ts";
import { RoomRegistry } from "../src/client/rooms.ts";
import { Heartbeat } from "../src/client/heartbeat.ts";
import {
	WSConnectionLostError,
	WSDisposedError,
	WSOutboxDropError,
	WSTimeoutError,
} from "../src/protocol/errors.ts";
import type { ClientFrame, WSRoomMessage } from "../src/protocol/frames.ts";
import { rejection } from "./_helpers.ts";

const frame = (id: string): ClientFrame => ({
	type: "pub",
	id,
	room: "r",
	payload: null,
});

test("backoff — grows exponentially and respects the cap", () => {
	const base = 500;
	const max = 30_000;

	// With rnd()=1 we get the upper bound of the equal-jitter window.
	const upper = (n: number) => backoffDelay(n, base, max, () => 1);
	assert.deepEqual(upper(1), 500);
	assert.deepEqual(upper(2), 1000);
	assert.deepEqual(upper(3), 2000);
	assert.deepEqual(upper(4), 4000);

	// The cap is the substantive fix over uncapped exponential growth.
	assert.deepEqual(upper(50), max);
	assert.deepEqual(upper(1000), max);
});

test("backoff — equal jitter keeps a floor of half the delay", () => {
	const base = 1000;
	const max = 30_000;

	// Full jitter would allow ~0 here, which hammers a recovering server.
	assert.deepEqual(backoffDelay(3, base, max, () => 0), 2000);
	assert.deepEqual(backoffDelay(3, base, max, () => 1), 4000);

	for (let i = 0; i < 200; i++) {
		const d = backoffDelay(3, base, max);
		assert(d >= 2000 && d <= 4000, `jitter escaped its window: ${d}`);
	}
});

test("outbox — queues while disconnected, drains in FIFO order", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 1_000 });

	const a = outbox.track("a", frame("a"), true);
	const b = outbox.track("b", frame("b"), true);
	assert.deepEqual(outbox.queuedCount, 2);

	const drained = outbox.drain();
	assert.deepEqual(drained.map((f) => (f as { id: string }).id), ["a", "b"]);
	assert.deepEqual(outbox.queuedCount, 0);
	// Draining transmits; it does not resolve. They await acks now.
	assert.deepEqual(outbox.pendingCount, 2);

	outbox.settle("a", 3);
	outbox.settle("b", 0);
	assert.deepEqual((await a).recipients, 3);
	assert.deepEqual((await b).recipients, 0);
	assert.deepEqual(outbox.pendingCount, 0);
});

test("outbox — cap drops the oldest and rejects its promise", async () => {
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
	await rejection(() => a, WSOutboxDropError);
	assert.deepEqual(dropped.map((f) => (f as { id: string }).id), ["a"]);
	assert.deepEqual(outbox.droppedCount, 1);
	assert.deepEqual(outbox.queuedCount, 2);

	outbox.settle("b", 1);
	outbox.settle("c", 1);
	await b;
	await c;
});

test("outbox — the timeout spans the queue, not just the flight", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 60 });
	// Never drained, i.e. never even sent: without a queue-spanning deadline
	// this promise would pend forever behind an infinite reconnect.
	await rejection(() => outbox.track("a", frame("a"), true), WSTimeoutError);
	assert.deepEqual(outbox.pendingCount, 0);
});

test("outbox — a per-frame timeout replaces the default for that frame alone", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 5_000 });
	const quick = outbox.track("q", frame("q"), true, true, 40);
	const patient = outbox.track("p", frame("p"), true);

	const started = Date.now();
	const error = await rejection(() => quick, WSTimeoutError);
	assert(Date.now() - started < 1_000, "the shorter deadline must apply");
	// The error names the deadline that applied, not the default.
	assert(error.message.includes("40ms"), error.message);

	// The default-deadline frame is untouched.
	assert.deepEqual(outbox.pendingCount, 1);
	assert.deepEqual(outbox.queuedCount, 1);

	outbox.failAll(new WSDisposedError());
	await rejection(() => patient, WSDisposedError);
});

test("outbox — failAll settles everything", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 5_000 });
	const a = outbox.track("a", frame("a"), true);
	const b = outbox.track("b", frame("b"), false);

	outbox.failAll(new WSDisposedError());

	await rejection(() => a, WSDisposedError);
	await rejection(() => b, WSDisposedError);
	assert.deepEqual(outbox.pendingCount, 0);
});

test("outbox — settleInFlight answers transmitted frames, spares queued ones", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 5_000 });
	const queued = outbox.track("q", frame("q"), true);
	const rejected = outbox.track("r", frame("r"), false);
	const resolved = outbox.track("s", frame("s"), false);

	outbox.settleInFlight((f) =>
		(f as { id: string }).id === "s" ? null : new WSConnectionLostError()
	);

	await rejection(() => rejected, WSConnectionLostError);
	assert.deepEqual((await resolved).recipients, 0);

	// The queued frame is untouched: it still waits for the next connection.
	assert.deepEqual(outbox.queuedCount, 1);
	assert.deepEqual(outbox.pendingCount, 1);

	outbox.failAll(new WSDisposedError());
	await rejection(() => queued, WSDisposedError);
});

test("outbox — an unacked frame completes when transmitted, an acked one waits", async () => {
	const outbox = new Outbox({ maxSize: 10, sendTimeout: 1_000 });
	const unacked = outbox.track("u", { type: "msg", payload: 1 }, true, false);
	const acked = outbox.track("a", { type: "msg", id: "a", payload: 2 }, true);

	const drained = outbox.drain();
	assert.deepEqual(drained.map((e) => e.id), ["u", "a"]);

	outbox.transmitted("u");
	outbox.transmitted("a");
	assert.deepEqual((await unacked).payload, undefined);
	// Being written is not being answered.
	assert.deepEqual(outbox.pendingCount, 1);

	outbox.settle("a", 0, { reply: true });
	assert.deepEqual((await acked).payload, { reply: true });
	assert.deepEqual(outbox.pendingCount, 0);
});

test("heartbeat — a later ping cannot postpone an unanswered one", async () => {
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

test("heartbeat — inbound traffic clears the deadline", async () => {
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
		assert.deepEqual(timedOut, false, "a responsive peer must not be dropped");
	} finally {
		hb.stop();
	}
});

test("rooms — refcounted: N handlers, one wire subscription", () => {
	const rooms = new RoomRegistry();
	const h1 = () => {};
	const h2 = () => {};

	assert.deepEqual(rooms.add("chat", h1).created, true);
	// Second handler must NOT ask for another `sub` frame.
	assert.deepEqual(rooms.add("chat", h2).created, false);

	assert.deepEqual(rooms.remove("chat", h1), false, "still has a handler");
	assert.deepEqual(rooms.remove("chat", h2), true, "last one out unsubscribes");
	assert.deepEqual(rooms.has("chat"), false);
});

test("rooms — presence upgrade re-subscribes", () => {
	const rooms = new RoomRegistry();
	rooms.add("chat", () => {});
	assert.deepEqual(rooms.wantsPresence("chat"), false);

	const result = rooms.add("chat", () => {}, () => {});
	assert.deepEqual(result.created, false);
	assert.deepEqual(result.presenceUpgraded, true);
	assert.deepEqual(rooms.wantsPresence("chat"), true);

	assert.deepEqual(rooms.subRequests(), [{ room: "chat", presence: true }]);
});

test("rooms — a handler unsubscribing mid-delivery cannot corrupt it", () => {
	const rooms = new RoomRegistry();
	const seen: string[] = [];

	const first = () => {
		seen.push("first");
		rooms.remove("chat", second);
	};
	const second = () => seen.push("second");

	rooms.add("chat", first);
	rooms.add("chat", second);

	const msg: WSRoomMessage = {
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
	assert.deepEqual(seen, ["first", "second"]);
});

test("rooms — a throwing handler does not stop the others", () => {
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

	assert.deepEqual(seen, ["survivor"]);
	assert.deepEqual(errors.length, 1);
});
