# @marianmeres/ws

[![NPM](https://img.shields.io/npm/v/@marianmeres/ws)](https://www.npmjs.com/package/@marianmeres/ws)
[![JSR](https://jsr.io/badges/@marianmeres/ws)](https://jsr.io/@marianmeres/ws)
[![License](https://img.shields.io/npm/l/@marianmeres/ws)](LICENSE)

A WebSocket client that survives real networks — reconnect, half-open
detection, buffered sends, token refresh — used either as a plain **message
channel to your server** or with **rooms and presence** on top. Plus a mountable
reference server, and a [protocol](./PROTOCOL.md) small enough to implement in
any language.

## Features

- **Reconnects forever** — capped exponential backoff with jitter, plus instant
  retry when the browser comes back online or the tab becomes visible again
- **Detects half-open connections** — the failure where the peer vanishes, no
  `onclose` ever fires, and a naive client sits "connected" receiving nothing
- **Buffered sends** — anything sent while offline is queued (capped, never
  unbounded) and flushed on reconnect, with one deadline per send
- **Token refresh for free** — the `auth()` callback runs before every
  (re)connect
- **Two ways to talk** — plain messages to and from the server, fire-and-forget
  or request/response; or namespaces, rooms, presence and broadcast
- **Runs everywhere** — `WebSocket` is a global in browsers, Deno, Node 22+, Bun
  and Workers, so there is no polyfill and no transport dependency
- **Svelte-store compatible** reactive connection state

## Installation

```bash
npm install @marianmeres/ws
```

```bash
deno add jsr:@marianmeres/ws
```

> **npm ships the client only.** The reference server needs
> `Deno.upgradeWebSocket`, so it exists solely as `jsr:@marianmeres/ws/server`.
> The client is fully runtime-agnostic, so npm consumers lose nothing they could
> have used.

## Two ways to use it

The protocol has a small required **core** — a handshake, messages both ways, a
heartbeat — and an optional **rooms extension**. Which one you use decides what
your server has to implement:

|                        | Messages                                                      | Rooms                                                                         |
| ---------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Who talks to whom      | the client and **the server**                                 | clients to **each other**, relayed by the server                              |
| Client API             | `send()`, `on("message")`                                     | `subscribe()`, `publish()`, `broadcast()`                                     |
| The server implements  | the core only — 8 small frame types                           | the core plus the rooms extension                                             |
| Server in any language | [PROTOCOL.md §8](PROTOCOL.md#8-python-a-core-server) (Python) | [PROTOCOL.md §9](PROTOCOL.md#9-python-the-full-reference-with-rooms) (Python) |
| Reference server       | `onMessage` + `service.send()`                                | works as is                                                                   |

Both get everything in the feature list above. They also mix freely on one
connection: a room-based app can still `send()` to the server, and a server can
still push directly to one client.

### 1. Messages — a data channel to your server

No rooms, no namespaces, no ceremony: the client sends messages to the server,
the server sends messages to the client.

```typescript
import { createWSClient, WSRemoteError } from "@marianmeres/ws";

const ws = createWSClient({
	url: "wss://api.example.com/ws",
	auth: () => session.token, // called on every (re)connect, so refresh works
});

// Everything the server pushes.
ws.on("message", (msg) => console.log(msg.payload));

// Fire-and-forget: resolves once written to the socket. Safe not to await.
ws.send({ op: "cursor", x: 10, y: 20 });

// Request/response: waits for the server's acknowledgement, resolves with its reply.
const doc = await ws.send<Doc>({ op: "load", id: 42 }, { ack: true });

// A refusal arrives as a typed error carrying the server's own code — and any
// structured `details` it attached, for your code rather than your user.
try {
	await ws.send({ op: "delete", id: 42 }, { ack: true });
} catch (e) {
	if (e instanceof WSRemoteError && e.code === "forbidden") showNotAllowed();
}

// One slow request, without stretching the client-wide `sendTimeout` for all.
const job = await ws.send<Job>({ op: "create" }, { ack: true, timeout: 60_000 });
```

**Fire-and-forget or acknowledged — your call, per message.** Without
`{ ack: true }` there is no delivery confirmation: a message written into a
connection that turns out to be dead is lost, exactly as with a plain
`WebSocket`. With it, the server confirms every message, can answer it, and a
message lost with its connection rejects with `WSConnectionLostError` instead of
silently vanishing.

**The server side** is small enough to write in any language:
[PROTOCOL.md](PROTOCOL.md) specifies it, section 8 is a complete Python
server, and Appendix A is a conformance script that drives this client against
yours. The reference server does it with one hook and one method:

```typescript
import { createWSApp, WSRemoteError } from "@marianmeres/ws/server";

const { app, service } = createWSApp("/ws", [], {
	verify: async (payload) => {
		const user = await authenticate(payload); // null closes with 4001
		return user ? { clientId: user.id } : null;
	},
	// Every ws.send() lands here. The return value is the reply for { ack: true }.
	onMessage: async (ctx, payload) => {
		const { op, id } = payload as { op: string; id: number };
		if (op === "load") return await loadDoc(id);
		throw new WSRemoteError({ code: "unknown_op", message: `unknown op ${op}` });
	},
});

// Push to one client, any time.
service.send(userId, { op: "progress", done: 42 });

Deno.serve(app);
```

### 2. Rooms — the server as a relay

Namespaces, rooms, presence and broadcast, for clients that talk to each other:
chat, collaboration, live cursors.

```typescript
import { createWSClient } from "@marianmeres/ws";

const ws = createWSClient({
	url: "/ws",
	namespace: "org-123",
	auth: () => session.token,
});

// Subscribe and handle in one call; the returned function detaches the handler
// and unsubscribes the room when it was the last one.
const unsub = await ws.subscribe("chat", (msg) => {
	console.log(msg.from, msg.payload, msg.timestamp);
});

const { recipients } = await ws.publish("chat", { text: "hello" });

unsub();
ws.dispose();
```

**Presence** is enabled by _providing a presence handler_, and is opt-in per
room — a room with thousands of subscribers does not want a join event per peer
every time the fleet reconnects.

```typescript
await ws.subscribe("room", onMessage, {
	presence: (e) => {
		// e.event is "sync" | "join" | "leave"
		// "sync" carries the full snapshot, and fires again after every reconnect
		console.log(e.event, e.clientId, e.members);
	},
});

ws.members("room"); // last known membership
```

**The server:**

```typescript
import { createWSApp } from "@marianmeres/ws/server";

const { app, service } = createWSApp("/ws", [], {
	// `requested` is what the client asked for — hints, never facts.
	verify: async (payload, req, requested) => {
		const user = await authenticate(payload?.token);
		// Returning null closes the socket with a terminal code.
		if (!user || !user.orgs.includes(requested.namespace)) return null;
		return { clientId: user.id, namespace: requested.namespace };
	},
});

// Push into a room from anywhere in your app.
await service.publish("notifications", { text: "deploy finished" }, "org-123");

Deno.serve(app);
```

Namespace is the isolation boundary, and it falls back to what the client asked
for when `verify` returns none — so in a multi-tenant deployment `verify` must
return `namespace` and `clientId`, validating `requested` rather than trusting
it.

### Either way

`connect()` is optional — the first `send()`, `subscribe()` or `publish()`
starts the connection. Call it explicitly when you want a readiness gate:

```typescript
await ws.connect(); // resolves once connected; rejects only if retrying cannot help
```

Connection state is reactive, through the Svelte store contract:

```svelte
<script>
    import { createWSClient } from "@marianmeres/ws";
    const ws = createWSClient({ url: "/ws" });
    const state = ws.state;
</script>

{#if $state.connected}
    <Online />
{:else if $state.attempt > 0}
    <p>Reconnecting… (attempt {$state.attempt})</p>
{/if}
```

If `verify` authenticates from cookies, set `allowedOrigins` on the server as
well: browsers attach cookies to a WebSocket opened from any site, and without
an `Origin` check that is cross-site WebSocket hijacking.

The reference server mounts these routes, relative to the mount path:

| Method | Path                          | Notes                                     |
| ------ | ----------------------------- | ----------------------------------------- |
| GET    | `/`                           | WebSocket upgrade                         |
| GET    | `/stats`                      | Requires `httpAuth`, else **not mounted** |
| POST   | `/publish/[namespace]/[room]` | Requires `httpAuth`, else **not mounted** |
| POST   | `/broadcast/[room]`           | Requires `httpAuth`, else **not mounted** |

## ws or sse?

`@marianmeres/sse` is the sibling package: same shape of API, different
transport, different strengths. In short —

- **Reach for `ws`** when traffic is genuinely bidirectional and chatty
  (collaborative editing, games, chat with typing indicators), when you need
  presence, or when you need binary frames.
- **Reach for `sse`** when traffic is mostly server → client (notifications,
  live dashboards, progress, activity feeds), or when losing messages across a
  reconnect is not acceptable — SSE resumes from `Last-Event-ID`, WebSocket has
  no equivalent.

They are not drop-in replacements for one another and are not meant to be. See
[COMPARISON.md](https://github.com/marianmeres/sse/blob/master/COMPARISON.md)
in the `sse` package for the full table.

## Example

A complete room chat — demino server plus a plain HTML client — lives in
[example/](https://github.com/marianmeres/ws/tree/master/example):

```bash
deno task example    # builds the client bundle, then serves on :8000
```

It exercises the rooms side end to end: the handshake, namespaces, rooms,
presence, acknowledged publishes, the broadcast gate, reconnect with buffered
sends, HTTP injection and the pub/sub adapter seam. See
[example/README.md](https://github.com/marianmeres/ws/blob/master/example/README.md).

## Concepts

**Message** — what the application sends and receives. Its `payload` is opaque:
never inspected, never mutated, free to carry its own `type` field. A message
the server sends directly has nothing else; one delivered through a room also
carries `room`, `namespace`, `from` and `timestamp` (a `WSRoomMessage`).

**Namespace** — the isolation boundary for rooms. Clients in different
namespaces can subscribe to identically named rooms without ever seeing each
other's messages. A client may only publish into its own namespace.

**Room** — a channel within a namespace. Subscribe to receive its messages.

**Broadcast** — the one operation that crosses namespaces. It is a separate
method rather than a flag on `publish()` precisely because crossing an isolation
boundary deserves its own name and its own server-side check: `allowBroadcast`
**denies by default**.

**Frame vs message** — a _frame_ is one JSON protocol envelope (`auth`, `msg`,
`ack`, `sub`, …), carried in exactly one WebSocket text message — not an RFC
6455 fragment. A `msg` frame minus its `type` field _is_ the message your
handlers receive.

## Behaviour worth knowing

**Reconnect classification.** Everything reconnects except a local
`disconnect()` and an explicit terminal close code (`4001 AUTH_FAILED`,
`4003 FORBIDDEN` by default). A server-sent `1000 Normal Closure` _does_
reconnect — a graceful shutdown or rolling deploy is exactly when clients must
come back. When reconnecting is pointless for your application rather than for
the protocol — a server that forgets the session with its socket — set
`reconnect: false` (or a function deciding per close): the client ends `idle`,
quietly, and a later `connect()` starts it again. That is a different thing
from a terminal code, which is a failure and is reported as one.

**Terminal failures are loud.** Giving up is the only non-retrying exit, so it
rejects any pending `connect()`, emits `terminated`, and logs at error level. A
silent one would be indistinguishable from a network that never recovered.

**Delivery is at-most-once.** An acknowledged send or a publish that was
transmitted but unacknowledged when the socket died is _not_ resent — that would
risk duplicates, and the server has no deduplication. It rejects immediately
with `WSConnectionLostError` rather than waiting out `sendTimeout`: the answer
is already known at the close. A fire-and-forget `send()` has no such answer to
give — it resolved when it was written. At-least-once would need server-side
replay, which this version does not do.

**Sends are bounded.** Every send carries one deadline covering queue, flight
_and_ acknowledgement — the client's `sendTimeout`, or the `timeout` of that
particular `send()`. Without it, a send issued while offline would pend forever
behind an infinite retry.

**A server that does not do rooms says so.** Against a core-only server,
`subscribe()` and `publish()` reject at once with `WSRemoteError` code
`unsupported` — they do not time out.

**`disconnect()` is resumable; `dispose()` is terminal.** Handlers, rooms and
buffered sends survive a `disconnect()`, so a later `connect()` picks up where
it left off.

## API

See [API.md](API.md) for complete API documentation.

## Protocol

The reference server is Deno-only; the protocol is not.
[PROTOCOL.md](PROTOCOL.md) specifies the wire format frame by frame, core first
and the rooms extension after it — with complete Python servers for both (a
core-only one, and the full reference) and a conformance script that drives the
real client against yours.

## License

[MIT](LICENSE)
