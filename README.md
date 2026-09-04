# @marianmeres/ws

[![NPM](https://img.shields.io/npm/v/@marianmeres/ws)](https://www.npmjs.com/package/@marianmeres/ws)
[![JSR](https://jsr.io/badges/@marianmeres/ws)](https://jsr.io/@marianmeres/ws)
[![License](https://img.shields.io/npm/l/@marianmeres/ws)](LICENSE)

A WebSocket client with namespaces, rooms, presence and reconnect that actually
survives real networks — plus a mountable reference server implementing the same
[protocol](./PROTOCOL.md).

## Features

- **Reconnects forever** — capped exponential backoff with jitter, plus instant
  retry when the browser comes back online or the tab regains focus
- **Detects half-open connections** — the failure where the peer vanishes, no
  `onclose` ever fires, and a naive client sits "connected" receiving nothing
- **Namespaces and rooms** — namespace isolates, rooms are channels within it
- **Presence** — opt-in per room; membership snapshot plus join/leave deltas,
  re-synced automatically after every reconnect
- **Buffered sends** — publishes issued while offline are queued (capped, never
  unbounded) and flushed after re-subscribe
- **Acknowledged publishes** — `publish()` resolves with a recipient count
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

## Usage

### Client

```typescript
import { createWSClient } from "@marianmeres/ws";

const ws = createWSClient({
	url: "/ws",
	namespace: "org-123",
	auth: () => session.token, // called on every (re)connect, so refresh works
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

`connect()` is optional — the first `subscribe()` or `publish()` starts the
connection. Call it explicitly when you want a readiness gate:

```typescript
await ws.connect(); // resolves once connected; rejects only if retrying cannot help
```

### Presence

Presence is enabled by _providing a presence handler_, and is opt-in per room —
a room with thousands of subscribers does not want a join event per peer every
time the fleet reconnects.

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

### Reactive state (Svelte)

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

### Server

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

// Push to connected clients from anywhere in your app.
await service.publish("notifications", { text: "deploy finished" }, "org-123");

Deno.serve(app);
```

Namespace is the isolation boundary, and it falls back to what the client asked
for when `verify` returns none — so in a multi-tenant deployment `verify` must
return `namespace` and `clientId`, validating `requested` rather than trusting
it.

Mounted routes, relative to the mount path:

| Method | Path                          | Notes                                     |
| ------ | ----------------------------- | ----------------------------------------- |
| GET    | `/`                           | WebSocket upgrade                         |
| GET    | `/stats`                      | Guarded by `httpAuth` when supplied       |
| POST   | `/publish/[namespace]/[room]` | Requires `httpAuth`, else **not mounted** |
| POST   | `/broadcast/[room]`           | Requires `httpAuth`, else **not mounted** |

## Example

A complete room chat — demino server plus a plain HTML client — lives in
[example/](https://github.com/marianmeres/ws/tree/master/example):

```bash
deno task example    # builds the client bundle, then serves on :8000
```

It exercises the handshake, namespaces, rooms, presence, acknowledged
publishes, the broadcast gate, reconnect with buffered sends, HTTP injection
and the pub/sub adapter seam. See
[example/README.md](https://github.com/marianmeres/ws/blob/master/example/README.md).

## Concepts

**Namespace** — the isolation boundary. Clients in different namespaces can
subscribe to identically named rooms without ever seeing each other's messages.
A client may only publish into its own namespace.

**Room** — a channel within a namespace. Subscribe to receive its messages.

**Broadcast** — the one operation that crosses namespaces. It is a separate
method rather than a flag on `publish()` precisely because crossing an isolation
boundary deserves its own name and its own server-side check: `allowBroadcast`
**denies by default**.

## Behaviour worth knowing

**Reconnect classification.** Everything reconnects except a local
`disconnect()` and an explicit terminal close code (`4001 AUTH_FAILED`,
`4003 FORBIDDEN` by default). A server-sent `1000 Normal Closure` _does_
reconnect — a graceful shutdown or rolling deploy is exactly when clients must
come back.

**Terminal failures are loud.** Giving up is the only non-retrying exit, so it
rejects any pending `connect()`, emits `terminated`, and logs at error level. A
silent one would be indistinguishable from a network that never recovered.

**Delivery is at-most-once.** A publish that was transmitted but unacknowledged
when the socket died is _not_ resent — that would risk duplicates, and the
server has no deduplication. It rejects immediately with `WSConnectionLostError`
rather than waiting out `sendTimeout`: the answer is already known at the close.
At-least-once would need server-side replay, which this version does not do.

**Sends are bounded.** Every publish carries one deadline covering queue, flight
_and_ acknowledgement. Without it, a publish issued while offline would pend
forever behind an infinite retry.

**`disconnect()` is resumable; `dispose()` is terminal.** Handlers, rooms and
buffered sends survive a `disconnect()`, so a later `connect()` picks up where
it left off.

## API

See [API.md](API.md) for complete API documentation.

## Protocol

The reference server is Deno-only; the protocol is not.
[PROTOCOL.md](PROTOCOL.md) specifies the wire format frame by frame — with a
complete Python implementation and a conformance script that drives the real
client — so a compatible server can be written in any language.

## License

[MIT](LICENSE)
