import { assert, assertEquals, assertRejects } from "@std/assert";
import type { DeminoHandler } from "@marianmeres/demino";

import { createWSClient } from "../src/mod.ts";
import type {
	WSMessage,
	WSPresenceEvent,
	WSRequestedIdentity,
} from "../src/protocol/frames.ts";
import { WSRemoteError, WSTerminatedError } from "../src/protocol/errors.ts";
import { startServer, until } from "./_helpers.ts";

/** Quiet, heartbeat-free client — liveness has its own tests. */
const client = (url: string, options: Record<string, unknown> = {}) =>
	createWSClient({ url, logger: null, pingInterval: 0, ...options });

Deno.test("connect, subscribe, publish, deliver", async () => {
	const server = startServer();
	const alice = client(server.url, { clientId: "alice" });
	const bob = client(server.url, { clientId: "bob" });

	try {
		await alice.connect();
		await bob.connect();
		assertEquals(alice.clientId, "alice");
		assertEquals(alice.namespace, "default");

		const received: WSMessage[] = [];
		await bob.subscribe("chat", (msg) => received.push(msg));

		const { recipients } = await alice.publish("chat", { text: "hi" });
		assertEquals(recipients, 1);

		await until(() => received.length === 1, "bob receives the message");
		assertEquals(received[0].payload, { text: "hi" });
		assertEquals(received[0].room, "chat");
		assertEquals(received[0].from, "alice");
		assert(received[0].timestamp > 0);
	} finally {
		alice.dispose();
		bob.dispose();
		await server.stop();
	}
});

Deno.test("namespaces isolate identically named rooms", async () => {
	const server = startServer();
	const one = client(server.url, { namespace: "org-1", clientId: "one" });
	const two = client(server.url, { namespace: "org-2", clientId: "two" });

	try {
		await one.connect();
		await two.connect();

		const seenByTwo: WSMessage[] = [];
		await two.subscribe("chat", (msg) => seenByTwo.push(msg));

		const seenByOne: WSMessage[] = [];
		await one.subscribe("chat", (msg) => seenByOne.push(msg));

		const { recipients } = await one.publish("chat", { text: "org-1 only" });

		// Only the publisher's own namespace sees it.
		assertEquals(recipients, 1);
		await until(() => seenByOne.length === 1, "same-namespace delivery");
		assertEquals(seenByTwo.length, 0, "namespace boundary leaked");
	} finally {
		one.dispose();
		two.dispose();
		await server.stop();
	}
});

Deno.test("publishing into someone else's namespace is refused", async () => {
	const server = startServer();
	const c = client(server.url, { namespace: "org-1" });

	try {
		await c.connect();
		// Otherwise the isolation boundary would be decorative.
		await assertRejects(
			() => c.publish("chat", { x: 1 }, "org-2"),
			WSRemoteError,
		);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("broadcast is denied by default", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		await c.connect();
		await assertRejects(() => c.broadcast("alerts", { x: 1 }), WSRemoteError);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("broadcast crosses namespaces when allowed", async () => {
	const server = startServer({ allowBroadcast: () => true });
	const one = client(server.url, { namespace: "org-1" });
	const two = client(server.url, { namespace: "org-2" });

	try {
		await one.connect();
		await two.connect();

		const seenByOne: WSMessage[] = [];
		const seenByTwo: WSMessage[] = [];
		await one.subscribe("alerts", (m) => seenByOne.push(m));
		await two.subscribe("alerts", (m) => seenByTwo.push(m));

		const { recipients } = await one.broadcast("alerts", { text: "maintenance" });
		assertEquals(recipients, 2);

		await until(
			() => seenByOne.length === 1 && seenByTwo.length === 1,
			"both namespaces receive the broadcast",
		);
		// Receivers always see the namespace they actually live in.
		assertEquals(seenByTwo[0].namespace, "org-2");
	} finally {
		one.dispose();
		two.dispose();
		await server.stop();
	}
});

Deno.test("presence: sync on subscribe, then join and leave deltas", async () => {
	const server = startServer();
	const alice = client(server.url, { clientId: "alice" });
	const bob = client(server.url, { clientId: "bob" });

	try {
		await alice.connect();
		const aliceEvents: WSPresenceEvent[] = [];
		await alice.subscribe("room", () => {}, {
			presence: (e) => aliceEvents.push(e),
		});

		// Alice is alone, and her snapshot says so.
		await until(() => aliceEvents.length === 1, "alice gets a sync");
		assertEquals(aliceEvents[0].event, "sync");
		assertEquals(aliceEvents[0].members, ["alice"]);

		await bob.connect();
		const bobEvents: WSPresenceEvent[] = [];
		await bob.subscribe("room", () => {}, {
			presence: (e) => bobEvents.push(e),
		});

		// Bob's own snapshot already includes both.
		await until(() => bobEvents.length === 1, "bob gets a sync");
		assertEquals(bobEvents[0].event, "sync");
		assertEquals(bobEvents[0].members.sort(), ["alice", "bob"]);

		// Alice is told about the arrival.
		await until(() => aliceEvents.length === 2, "alice sees bob join");
		assertEquals(aliceEvents[1].event, "join");
		assertEquals(aliceEvents[1].clientId, "bob");
		assertEquals(alice.members("room").sort(), ["alice", "bob"]);

		bob.dispose();

		await until(() => aliceEvents.length === 3, "alice sees bob leave");
		assertEquals(aliceEvents[2].event, "leave");
		assertEquals(aliceEvents[2].clientId, "bob");
		assertEquals(alice.members("room"), ["alice"]);
	} finally {
		alice.dispose();
		bob.dispose();
		await server.stop();
	}
});

Deno.test("server-injected messages arrive with from: null", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		await c.connect();
		const received: WSMessage[] = [];
		await c.subscribe("notifications", (m) => received.push(m));

		const recipients = await server.service.publish("notifications", { n: 1 });
		assertEquals(recipients, 1);

		await until(() => received.length === 1, "injected message arrives");
		// This is how a client tells server pushes from peer traffic.
		assertEquals(received[0].from, null);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("refcounting: one handler leaving does not unsubscribe the room", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		await c.connect();
		const a: WSMessage[] = [];
		const b: WSMessage[] = [];
		const unsubA = await c.subscribe("chat", (m) => a.push(m));
		await c.subscribe("chat", (m) => b.push(m));

		unsubA();
		assert(c.isSubscribed("chat"), "room must survive its first handler");

		await server.service.publish("chat", { n: 1 });
		await until(() => b.length === 1, "surviving handler still receives");
		assertEquals(a.length, 0, "detached handler still received");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("unsubscribe stops delivery", async () => {
	const server = startServer();
	const c = client(server.url);

	try {
		await c.connect();
		const seen: WSMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));
		await c.unsubscribe("chat");
		assertEquals(c.isSubscribed("chat"), false);

		const recipients = await server.service.publish("chat", { n: 1 });
		assertEquals(recipients, 0);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("autoConnect: publish works without an explicit connect()", async () => {
	const server = startServer();
	const producer = client(server.url);
	const consumer = client(server.url);

	try {
		await consumer.connect();
		const seen: WSMessage[] = [];
		await consumer.subscribe("chat", (m) => seen.push(m));

		// No connect() call: the send starts the machine and buffers until open.
		const { recipients } = await producer.publish("chat", { text: "lazy" });
		assertEquals(recipients, 1);
		await until(() => seen.length === 1, "buffered publish is delivered");
	} finally {
		producer.dispose();
		consumer.dispose();
		await server.stop();
	}
});

Deno.test("terminal auth failure rejects connect() and emits terminated", async () => {
	const server = startServer({ verify: () => null });
	const c = client(server.url, { reconnectDelay: 20 });

	try {
		const terminated: unknown[] = [];
		c.on("terminated", (e) => terminated.push(e));

		// The whole point of rejecting: a bad token throws where you await,
		// instead of firing an event you forgot to subscribe to.
		const error = await assertRejects(() => c.connect(), WSTerminatedError);
		assertEquals(error.code, 4001);

		assertEquals(terminated.length, 1);
		assertEquals(c.connectionState, "terminated");

		// And it must stay given-up rather than quietly retrying forever.
		const attemptsBefore = c.dump().attempt;
		await new Promise((r) => setTimeout(r, 300));
		assertEquals(c.dump().attempt, attemptsBefore);
		assertEquals(c.connectionState, "terminated");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("verify assigns clientId and namespace", async () => {
	const server = startServer({
		verify: (payload) => {
			const token = (payload as { token?: string })?.token;
			if (token !== "good") return null;
			return { clientId: "assigned-id", namespace: "assigned-ns" };
		},
	});
	const c = client(server.url, {
		namespace: "ignored",
		auth: () => ({ token: "good" }),
	});

	try {
		await c.connect();
		// The server's assignment wins over the client's request.
		assertEquals(c.clientId, "assigned-id");
		assertEquals(c.namespace, "assigned-ns");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("verify sees the identity the client requested", async () => {
	const seen: WSRequestedIdentity[] = [];
	const server = startServer({
		verify: (_payload, _request, requested) => {
			seen.push(requested);
			return {};
		},
	});
	const c = client(server.url, { clientId: "alice", namespace: "org-1" });

	try {
		await c.connect();
		assertEquals(seen, [{ clientId: "alice", namespace: "org-1" }]);
		// Unchanged fallback: nothing assigned, so the proposals are honoured.
		assertEquals(c.clientId, "alice");
		assertEquals(c.namespace, "org-1");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("verify can reject a namespace the client is not entitled to", async () => {
	const server = startServer({
		verify: (payload, _request, requested) => {
			const org = (payload as { org?: string })?.org;
			return requested.namespace === org ? {} : null;
		},
	});
	const intruder = client(server.url, {
		namespace: "org-2",
		auth: () => ({ org: "org-1" }),
	});
	const tenant = client(server.url, {
		namespace: "org-1",
		auth: () => ({ org: "org-1" }),
	});

	try {
		const error = await assertRejects(() => intruder.connect(), WSTerminatedError);
		assertEquals(error.code, 4001);

		await tenant.connect();
		assertEquals(tenant.namespace, "org-1");
	} finally {
		intruder.dispose();
		tenant.dispose();
		await server.stop();
	}
});

Deno.test("HTTP injection routes are absent without httpAuth", async () => {
	const server = startServer();
	try {
		// Mounting an unauthenticated "push into any room" endpoint by default
		// would be a real vulnerability.
		const res = await fetch(`${server.httpUrl}/publish/default/chat`, {
			method: "POST",
			body: JSON.stringify({ x: 1 }),
		});
		await res.body?.cancel();
		assertEquals(res.status, 404);
	} finally {
		await server.stop();
	}
});

Deno.test("HTTP injection works when guarded", async () => {
	const httpAuth: DeminoHandler = (req: Request) => {
		if (req.headers.get("x-key") !== "secret") {
			return new Response("unauthorized", { status: 401 });
		}
	};
	const server = startServer({ httpAuth, allowBroadcast: () => true });
	const c = client(server.url);

	try {
		await c.connect();
		const seen: WSMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		const denied = await fetch(`${server.httpUrl}/publish/default/chat`, {
			method: "POST",
			body: JSON.stringify({ x: 1 }),
		});
		await denied.body?.cancel();
		assertEquals(denied.status, 401);

		const res = await fetch(`${server.httpUrl}/publish/default/chat`, {
			method: "POST",
			headers: { "x-key": "secret", "content-type": "application/json" },
			body: JSON.stringify({ text: "from http" }),
		});
		assertEquals(res.status, 200);
		assertEquals(await res.json(), { ok: true, recipients: 1 });

		await until(() => seen.length === 1, "http-injected message arrives");
		assertEquals(seen[0].from, null);
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("stats reports connections, rooms and namespaces", async () => {
	const server = startServer();
	const one = client(server.url, { namespace: "org-1" });
	const two = client(server.url, { namespace: "org-1" });

	try {
		await one.connect();
		await two.connect();
		await one.subscribe("chat", () => {});

		const res = await fetch(`${server.httpUrl}/stats`);
		const stats = await res.json();
		assertEquals(stats.connections, 2);
		assertEquals(stats.rooms, 1);
		assertEquals(stats.namespaces, { "org-1": 2 });
	} finally {
		one.dispose();
		two.dispose();
		await server.stop();
	}
});

Deno.test("a plain GET on the upgrade route explains itself", async () => {
	const server = startServer();
	try {
		const res = await fetch(server.httpUrl);
		await res.body?.cancel();
		assertEquals(res.status, 426);
	} finally {
		await server.stop();
	}
});
