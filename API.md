# API

Three entry points:

| Import                     | Contains                                   | Runtime   |
| -------------------------- | ------------------------------------------ | --------- |
| `@marianmeres/ws`          | the client, plus everything from protocol  | any       |
| `@marianmeres/ws/server`   | the reference server, plus `WSRemoteError` | Deno only |
| `@marianmeres/ws/protocol` | wire definitions only, dependency-free     | any       |

---

## Client

### `createWSClient(options?)`

Creates a client. Nothing connects until the first `connect()`, `send()`,
`subscribe()` or `publish()`.

The client works two ways, freely mixed on one connection — **messages**
(`send()` / the `message` event; the server only has to implement the protocol
core) and **rooms** (`subscribe()` / `publish()` / `broadcast()` / presence;
the rooms extension). See [Messages](#messages) and [Rooms](#rooms) below.

`WSClient` is exported too — `new WSClient(options)` is the same thing,
following the `PubSub` / `createPubSub` precedent.

**Parameters**

| Name                 | Type                                | Default            | Description                                                                   |
| -------------------- | ----------------------------------- | ------------------ | ----------------------------------------------------------------------------- |
| `url`                | `string \| URL`                     | `"/ws"`            | `ws(s)://`, or `http(s)://` (upgraded), or a path resolved against `location` |
| `namespace`          | `string`                            | `"default"`        | Isolation boundary for rooms. Sent to the server only when set                |
| `clientId`           | `string`                            | —                  | Preferred id; the server may override or ignore it                            |
| `rooms`              | `string[]`                          | `[]`               | Rooms joined on every (re)connect                                             |
| `auth`               | `() => unknown \| Promise<unknown>` | —                  | Auth payload; called before _every_ (re)connect                               |
| `autoConnect`        | `boolean`                           | `true`             | First `send()`/`subscribe()`/`publish()` starts the connection                |
| `logger`             | `Logger \| null`                    | `createClog("ws")` | `null` silences                                                               |
| `reconnect`          | `boolean \| (close) => boolean`     | `true`             | Retry after a non-terminal close? `false` never; a function decides per close |
| `reconnectDelay`     | `number`                            | `500`              | Initial backoff, ms                                                           |
| `reconnectDelayMax`  | `number`                            | `30_000`           | Backoff ceiling, ms                                                           |
| `terminalCloseCodes` | `number[]`                          | `[4001, 4003]`     | Codes after which retrying stops                                              |
| `pingInterval`       | `number`                            | `25_000`           | Ping cadence, ms. `0` disables                                                |
| `pongTimeout`        | `number`                            | `10_000`           | Liveness deadline; also bounds the auth handshake                             |
| `connectTimeout`     | `number`                            | `0`                | Bounds the first `connect()` await. `0` waits indefinitely                    |
| `sendTimeout`        | `number`                            | `30_000`           | Per-send deadline covering queue + flight + ack                               |
| `outboxMaxSize`      | `number`                            | `100`              | Frames buffered while offline. `0` disables buffering                         |
| `onOutboxDrop`       | `(frames: ClientFrame[]) => void`   | —                  | Called with evicted frames                                                    |
| `encode` / `decode`  | `WSEncoder` / `WSDecoder`           | JSON               | Must match the server's                                                       |

`pingInterval: 0` disables the client's heartbeat, not the server's reaper: the
reference server still closes a connection that sent nothing for `idleTimeout`
(60 s) with `4008`, so a heartbeat-free client reconnects roughly every minute.
Disable both or neither.

`reconnect` and `terminalCloseCodes` are different knobs. A terminal close is a
_failure_: the client ends `terminated`, logs at error level, emits
`terminated`. A close that `reconnect` declines to retry is not: the client
ends `idle`, quietly, exactly as after `disconnect()`, and a later `connect()`
(or any send, with `autoConnect`) starts it again. Use `reconnect: false` for
a server that forgets the session when its socket closes, so that reconnecting
could only ever authenticate into nothing; use the function form to give up
after a number of tries — it receives a [`WSCloseInfo`](#wscloseinfo).
The policy is consulted only for non-terminal codes; `terminalCloseCodes`
decides first.

**Returns** `WSClient`

**Example**

```typescript
import { createWSClient } from "@marianmeres/ws";

const ws = createWSClient({
	url: "wss://example.com/ws",
	auth: () => session.token, // re-read on every reconnect
});

// messages
ws.on("message", (msg) => console.log(msg.payload));
const reply = await ws.send({ op: "load", id: 42 }, { ack: true });

// rooms
const unsub = await ws.subscribe("chat", (msg) => {
	console.log(msg.from, msg.payload, msg.timestamp);
});
const { recipients } = await ws.publish("chat", { text: "hello" });

unsub();
ws.dispose();
```

---

### `WSClient`

The type behind `createWSClient()`. Generic over the auth payload:
`WSClient<TAuth>`.

#### Lifecycle

##### `connect(): Promise<void>`

Starts the connection and resolves once authenticated.

Idempotent — concurrent calls share one promise, and it resolves immediately
when already connected. Optional when `autoConnect` is on; it is a readiness
gate, not a prerequisite.

Rejects **only** where retrying cannot help:

- `WSTerminatedError` — a terminal close code; a close the `reconnect` policy
  declined to retry (the client is `idle` then, not `terminated`); or code
  `4900` when `disconnect()` (or `dispose()`, which disconnects first) is
  called while this is still pending
- `WSConnectTimeoutError` — `connectTimeout` elapsed. Retrying continues in the
  background, so this bounds _your await_, not the connection attempt
- `WSDisposedError` — called on an already disposed client. A `dispose()`
  _during_ a pending connect settles it with the `4900` `WSTerminatedError`
  above, not with this

Ordinary network failure never rejects while the client retries; that is what
the infinite retry is for.

##### `disconnect(): void`

Stops retrying and closes the socket. **Resumable** — handlers, room
subscriptions and buffered sends all survive, so a later `connect()` picks up
where it left off.

Emits `close` with code `4900` (`CLOSE.CLIENT_GONE`) and `willReconnect: false`
when there was a socket to close; nothing when already idle, reconnecting or
terminated.

##### `dispose(): void`

Terminal teardown: disconnects, then drops every handler, room, timer and
pending promise. Pending sends reject with `WSDisposedError`. The instance is
unusable afterwards.

#### Messages

The core of the protocol: the client and the server talk to each other
directly. Incoming messages arrive through the [`message`](#wsevents) event.

##### `send<T>(payload, options?): Promise<void>` / `send<R, T>(payload, { ack: true }): Promise<R>`

Sends a message to the server — all a core-only server has to understand.

**Fire-and-forget** by default: resolves as soon as the frame is written to the
socket. There is no delivery confirmation — a frame written into a connection
that turns out to be dead is lost, exactly as with a plain `WebSocket`. Calling
it without `await` is safe: the promise is marked handled, so a failure nobody
awaits (the queue timing out while offline, say) is not an unhandled rejection.
Await it to learn about one.

**With `{ ack: true }`** the frame carries an id and the promise waits for the
server's acknowledgement. It resolves with the reply the server put in the ack —
which makes this a request/response call — or with `undefined` for a bare ack. A
refusal (`nack`) rejects with `WSRemoteError` carrying the server's `code`, which
can be one of the server application's own, and any structured `details` it
attached. An acknowledged send in flight when the socket closes rejects there
and then with `WSConnectionLostError`; it is not resent.

Either way, while disconnected the frame is buffered and flushed after the next
connect, in order — bounded by one deadline spanning queue, flight and (with an
ack) acknowledgement: the client's `sendTimeout`, or `options.timeout` for this
send alone. After a terminal close nothing is buffered: the promise rejects
immediately with `WSTerminatedError`.

A third overload, `send<T>(payload, options?: WSSendOptions): Promise<unknown>`,
covers an `ack` flag decided at runtime.

**Parameters**

- `payload` (`T`) — opaque application data; never inspected or mutated
- `options.ack` (boolean, optional) — wait for the server's acknowledgement and
  its reply. Default `false`
- `options.timeout` (number, optional) — deadline for this send in ms,
  replacing the client's `sendTimeout`. Lets one slow request coexist with a
  short default instead of one `sendTimeout` sized for the slowest

**Throws** `WSRemoteError` (ack only), `WSTimeoutError`, `WSConnectionLostError`
(ack only), `WSOutboxDropError`, `WSNotConnectedError`, `WSTerminatedError`,
`WSDisposedError`

**Example**

```typescript
ws.send({ op: "cursor", x: 10, y: 20 }); // fire-and-forget

const doc = await ws.send<Doc>({ op: "load", id: 42 }, { ack: true });

// A slow one, without stretching the client-wide default for everything else.
const job = await ws.send<Job>({ op: "create" }, { ack: true, timeout: 60_000 });

try {
	await ws.send({ op: "delete", id: 42 }, { ack: true });
} catch (e) {
	if (e instanceof WSRemoteError && e.code === "forbidden") showNotAllowed();
	if (e instanceof WSRemoteError && e.code === "busy") {
		// Structured detail the server attached, for code rather than for humans.
		const { retryAfter } = e.details as { retryAfter: number };
		showCountdown(retryAfter);
	}
}
```

#### Rooms

The rooms extension: namespaces, rooms, presence and broadcast. A server that
does not implement it answers these with `unsupported`, so they fail with
`WSRemoteError` code `"unsupported"` at once rather than after `sendTimeout`.

##### `subscribe<T>(room, handler, options?): Promise<Unsubscriber>`

Subscribes to a room and attaches a handler.

The handler is attached **synchronously**, before any frame goes out, so
nothing arriving between the request and its acknowledgement is lost.

Rooms are refcounted: N handlers produce one wire subscription, and the returned
unsubscriber detaches only this handler — the `unsub` frame goes out when it was
the last one. The unsubscriber is idempotent and `Symbol.dispose`-compatible.

When connected this awaits the server's acknowledgement, so a refused
subscription rejects here. When not connected it resolves once the room is
registered; the subscription is then established by the re-subscribe on the next
connect, and a failure there surfaces as an `error` event.

**Parameters**

- `room` (string) — room name, scoped to this client's namespace
- `handler` (`MessageHandler<T>`) — receives every message published to the
  room, as a `WSRoomMessage<T>`
- `options.presence` (`PresenceHandler`, optional) — enables presence for this
  room

**Example**

```typescript
const unsub = await ws.subscribe("room", (msg) => render(msg.payload), {
	presence: (e) => {
		// e.event is "sync" | "join" | "leave"
		setMembers(e.members);
	},
});
```

##### `unsubscribe(room): Promise<void>`

Removes **every** handler for a room and unsubscribes it — the blunt
counterpart to the refcounted unsubscriber above. Unknown rooms are a no-op.

##### `isSubscribed(room): boolean`

Whether the room is held locally. Reflects local intent, not server state: a
room registered while offline reads `true` before the wire subscription exists.

##### `members(room): string[]`

Last known membership of a presence-enabled room. Empty for rooms without
presence.

##### `publish<T>(room, payload, namespace?): Promise<WSPublishResult>`

Publishes to a room within this client's namespace. Resolves with the recipient
count once the server acknowledges.

While disconnected the frame is buffered and the promise stays pending until it
flushes — bounded by `sendTimeout`, never indefinitely. A frame already in
flight when the socket closes rejects there and then with
`WSConnectionLostError`; it is not resent. After a terminal close nothing is
buffered at all: the promise rejects immediately with `WSTerminatedError`,
because only an explicit `connect()` leaves that state.

`namespace` must equal the client's own; the server rejects anything else, so it
is only useful for asserting the expected one.

A payload `encode` refuses — a `BigInt` is enough for the JSON default — rejects
with the encoder's own error, unwrapped, and also surfaces as an `error` event.

**Throws** `WSTimeoutError`, `WSConnectionLostError`, `WSOutboxDropError`,
`WSNotConnectedError`, `WSTerminatedError`, `WSRemoteError`, `WSDisposedError`

##### `broadcast<T>(room, payload): Promise<WSPublishResult>`

Publishes to a room across **all** namespaces.

A separate method rather than a flag on `publish()` because crossing an
isolation boundary deserves its own name and its own server-side check:
`allowBroadcast` **denies by default**, and a refusal arrives as a
`WSRemoteError` with code `"forbidden"`.

#### Events

##### `on<K>(event, cb): Unsubscriber` / `once<K>(event, cb): Unsubscriber`

Subscribe to a lifecycle event; see [`WSEvents`](#wsevents). The returned
unsubscriber is `Symbol.dispose`-compatible.

#### Properties

| Member            | Type                      | Notes                                                                     |
| ----------------- | ------------------------- | ------------------------------------------------------------------------- |
| `state`           | Svelte store of `WSState` | Fires immediately, then on every change                                   |
| `connected`       | `boolean`                 | `true` only in `open` — not merely socket-open                            |
| `connectionState` | `WSConnectionState`       |                                                                           |
| `clientId`        | `string \| null`          | Server-assigned; `null` until connected, and when the server assigns none |
| `namespace`       | `string`                  | The server's assignment wins over the request; else the requested one     |
| `rooms`           | `string[]`                | Rooms currently held                                                      |
| `socket`          | `WebSocket \| null`       | Escape hatch; sending on it bypasses the outbox                           |
| `url`             | `URL`                     | A copy — mutating it does nothing                                         |
| `logger`          | `Logger \| null`          | Assignable; set to `null` to silence                                      |
| `dump()`          | `Record<string, unknown>` | Debug snapshot; shape is not stable API                                   |

##### `WSClient.resolveUrl(input): URL` (static)

Normalizes an endpoint: relative paths resolve against `location`, and
`http(s)` is upgraded to `ws(s)`. Throws `WSError` when it cannot resolve —
outside a browser there is no `location` for a relative path.

---

### `backoffDelay(attempt, base, max, rnd?)`

Exponential backoff with **equal jitter**. Returns a delay in `[d/2, d]` where
`d = min(max, base * 2^(attempt-1))`.

Equal jitter rather than full jitter: full jitter can produce near-zero waits,
which means a server coming back up gets hammered by the very clients it just
dropped.

**Parameters**

- `attempt` (number) — 1-based attempt number
- `base` (number) — initial delay in ms
- `max` (number) — ceiling in ms
- `rnd` (`() => number`, optional) — randomness source. Default `Math.random`

**Returns** `number` — delay in ms

Exported mainly so the curve is testable.

---

## Client types

### `WSClientOptions<TAuth>`

The options object documented under
[`createWSClient`](#createwsclientoptions).

### `WSSendOptions`

```typescript
{
	ack?: boolean; // default false
	timeout?: number; // default: the client's sendTimeout
}
```

Options for [`send()`](#messages). `ack: true` waits for the server's
acknowledgement and resolves with its reply. `timeout` replaces the client's
`sendTimeout` for this send, with the same semantics — one deadline spanning
queue, flight and acknowledgement — and `WSTimeoutError` then reports the value
that applied.

### `WSCloseInfo`

```typescript
{
	code: number; // WebSocket close code
	reason: string; // close reason from the peer; possibly empty
	attempt: number; // consecutive reconnect attempts that have failed so far
}
```

What the function form of [`reconnect`](#createwsclientoptions) is told about
the close it decides on. `attempt` is `0` after a close that ended a working
connection and climbs by one per failed retry, so `({ attempt }) => attempt < 5`
gives up after five tries, and `({ code }) => code !== 1000` declines to come
back after a deliberate server-side close. A policy that throws is reported
through the `error` event and the default — retry — applies.

### `SubscribeOptions`

```typescript
{
	presence?: PresenceHandler;
}
```

Presence is enabled by _providing a handler_ rather than by a separate boolean —
one way to express the intent instead of two that can disagree. It is opt-in
per room because a 10k-subscriber room does not want a join event per peer every
time the fleet reconnects.

### `MessageHandler<T>` / `PresenceHandler`

```typescript
type MessageHandler<T = unknown> = (msg: WSRoomMessage<T>) => void;
type PresenceHandler = (event: WSPresenceEvent) => void;
```

A room handler receives a `WSRoomMessage` — every routing field present. The
[`message`](#wsevents) event receives the looser `WSMessage`, because it also
carries direct messages from the server.

A throwing handler is caught, reported through the `error` event, and does not
stop delivery to the others.

### `WSEvents`

| Event          | Payload                                                      |
| -------------- | ------------------------------------------------------------ |
| `open`         | `void` — socket open, pre-auth                               |
| `connected`    | `{ clientId: string \| null, namespace }`                    |
| `message`      | `WSMessage` — every inbound message, direct or from any room |
| `presence`     | `WSPresenceEvent`                                            |
| `close`        | `{ code, reason, willReconnect }`                            |
| `reconnecting` | `{ attempt, delay }`                                         |
| `terminated`   | `{ code, reason }` — gave up                                 |
| `error`        | `Error`                                                      |

`message` is the receiving side of [messages](#messages): a direct message from
the server carries only `payload`; a room delivery also carries `room`,
`namespace`, `from` and `timestamp`. Check `msg.room` to tell them apart.

`connected.clientId` is `null` when the server assigns no identity — a
core-only server need not.

`error` means something failed but the client carried on (a decode failure, a
throwing handler, an `error` frame from the server — for instance a
fire-and-forget `send()` the server refused). `terminated` is the only
non-retrying exit.

A local `disconnect()` is a `close` too: code `4900`, `willReconnect: false` —
that pair is how a deliberate teardown is told apart from a lost connection.
`willReconnect` is also `false` for a close the `reconnect` policy declined;
the client is `idle` afterwards.

### `WSState`

```typescript
{
	state: WSConnectionState;
	connected: boolean; // true only in "open"
	connecting: boolean; // connecting | authenticating | reconnecting
	attempt: number; // consecutive failures; resets to 0 on success
	lastError: Error | null;
}
```

Delivered through the Svelte store contract, so `$state` works directly in a
component and any other store-compatible consumer works too:

```svelte
<script>
    const state = ws.state;
</script>

{#if $state.connected}<Online />{:else if $state.attempt > 0}
    <p>Reconnecting… (attempt {$state.attempt})</p>
{/if}
```

### `WSConnectionState`

`"idle" | "connecting" | "authenticating" | "open" | "reconnecting" | "terminated" | "disposed"`

---

## Server

Import from `@marianmeres/ws/server`. **JSR only** — it needs
`Deno.upgradeWebSocket`, so the npm package ships the client alone.

### `createWSApp(mountPath?, middlewares?, options?)`

Creates a mountable demino app plus the service it is wired to.

**Parameters**

| Name                         | Type                                              | Default                   | Description                                                                                                                                                                   |
| ---------------------------- | ------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mountPath`                  | `string`                                          | `"/ws"`                   | Demino mount path                                                                                                                                                             |
| `middlewares`                | `DeminoHandler[]`                                 | `[]`                      | Applied to all routes                                                                                                                                                         |
| `options.verify`             | `(payload, req, requested) => AuthResult \| null` | —                         | Return `null` (or throw) to reject with `4001`. Absent means no authentication. `requested` is the identity the client asked for — see [below](#security-namespace-isolation) |
| `options.allowedOrigins`     | `string[] \| (origin, req) => boolean`            | — (no check)              | Origins allowed to upgrade → `403` — see [below](#security-cross-site-websocket-hijacking)                                                                                    |
| `options.onMessage`          | `(ctx, payload) => unknown`                       | — (**unsupported**)       | Receives every client `send()`; the return value is the reply — see [below](#messages-onmessage)                                                                              |
| `options.allowBroadcast`     | `(ctx, room) => boolean`                          | **deny**                  | Gate for cross-namespace broadcast                                                                                                                                            |
| `options.httpAuth`           | `DeminoHandler`                                   | —                         | Guards the HTTP routes. **Without it they are not mounted**                                                                                                                   |
| `options.deminoOptions`      | `DeminoOptions`                                   | —                         | Passed through to `demino()`                                                                                                                                                  |
| `options.authTimeout`        | `number`                                          | `5_000`                   | Deadline for the `auth` frame → `4002`                                                                                                                                        |
| `options.idleTimeout`        | `number`                                          | `60_000`                  | Reap silent connections → `4008`                                                                                                                                              |
| `options.maxFrameSize`       | `number`                                          | `262144`                  | Oversized frames → `4013`                                                                                                                                                     |
| `options.maxFramesPerSecond` | `number`                                          | `100`                     | Rate cap → `4009`                                                                                                                                                             |
| `options.adapter`            | `WSPubSubAdapter`                                 | `WSPubSubLocal`           | Cross-instance fan-out                                                                                                                                                        |
| `options.logger`             | `Logger \| null`                                  | `createClog("ws:server")` | `null` silences                                                                                                                                                               |
| `options.encode` / `.decode` | `WSEncoder` / `WSDecoder`                         | JSON                      | Must match the client's                                                                                                                                                       |

**Returns** `WSApp` — `{ app: Demino, service: WSService }`

**Routes**, relative to `mountPath`:

| Method | Path                          | Returns                    | Notes                                     |
| ------ | ----------------------------- | -------------------------- | ----------------------------------------- |
| GET    | `/`                           | 101, 426 without upgrade   | WebSocket upgrade, `403` on a bad origin  |
| GET    | `/stats`                      | `WSStats`                  | Requires `httpAuth`, else **not mounted** |
| POST   | `/publish/[namespace]/[room]` | `{ ok: true, recipients }` | Requires `httpAuth`, else **not mounted** |
| POST   | `/broadcast/[room]`           | `{ ok: true, recipients }` | Requires `httpAuth`, else **not mounted** |

The POST routes take the JSON request body as the message payload; a body that
is not valid JSON answers `400`.

**Example**

```typescript
import { createWSApp } from "@marianmeres/ws/server";

const { app, service } = createWSApp("/ws", [], {
	verify: async (payload, req) => {
		const user = await authenticate((payload as any)?.token);
		// Returning null closes the socket with 4001.
		return user ? { clientId: user.id, namespace: user.orgId } : null;
	},
	allowBroadcast: (ctx, room) => room === "announcements" && ctx.meta.admin === true,
});

await service.publish("notifications", { text: "deploy finished" }, "org-123");

Deno.serve(app);
```

#### Messages: `onMessage`

Every client `send()` reaches `onMessage(ctx, payload)`, with the sender's
[`WSConnectionContext`](#wsconnectioncontext).

- **The return value is the reply.** For a send with `{ ack: true }` it travels
  back in the `ack` and resolves the client's promise (`undefined` makes a bare
  ack). For a fire-and-forget send it is discarded. It may be a promise.
- **Throw a `WSRemoteError`** to refuse the message with your own `code` and
  `message` — the client's `send()` rejects with exactly those. Add `details`
  (any JSON value) when the application needs more than a code to act on: a
  retry-after, the field that failed; it arrives on the client as
  `WSRemoteError.details`, untouched. Any other throw is logged and answered
  `internal`, without its text. The connection stays open either way.
- **Called in arrival order, not awaited before the next frame.** An async hook
  may finish out of order; chain the work yourself where order matters.
- **Unset, the server accepts no messages**: every `send()` is answered
  `unsupported`.

To send a client a message of your own — now or later — use
[`service.send()`](#sendclientid-payload-boolean).

```typescript
import { createWSApp, WSRemoteError } from "@marianmeres/ws/server";

const { app, service } = createWSApp("/ws", [], {
	verify: (payload) => authenticate(payload),
	onMessage: async (ctx, payload) => {
		const { op, id } = payload as { op: string; id: number };
		if (op !== "load") {
			throw new WSRemoteError({ code: "unknown_op", message: `unknown op ${op}` });
		}
		if (loading.size >= MAX) {
			throw new WSRemoteError({
				code: "busy",
				message: "too many documents loading",
				details: { retryAfter: 5 }, // for the client's code, not its user
			});
		}
		service.send(ctx.clientId, { op: "progress", stage: "loading" });
		return await loadDoc(id); // the reply
	},
});
```

#### Security: namespace isolation

Namespace is the isolation boundary and `clientId` is the identity peers see in
`from` — and a claimed id evicts whoever holds it. Both fall back to what the
client asked for:

```
assigned by verify  →  requested by the client  →  generated
```

**In any multi-tenant deployment `verify` must return `namespace` and
`clientId`.** Return neither and the client's proposals are honoured verbatim,
so any authenticated user can enter any tenant. The third argument,
[`WSRequestedIdentity`](#wsrequestedidentity), carries those proposals, so they
can be validated there rather than duplicated into the auth payload:

```typescript
createWSApp("/ws", [], {
	verify: async (payload, req, requested) => {
		const user = await authenticate((payload as any)?.token);
		if (!user) return null;
		// The namespace is checked, not trusted — and assigned either way.
		if (!user.orgs.includes(requested.namespace)) return null;
		return { clientId: user.id, namespace: requested.namespace };
	},
});
```

#### Security: cross-site WebSocket hijacking

The upgrade request is an ordinary browser request, so the browser attaches its
cookies for your origin no matter which site opened the socket. A `verify` that
authenticates from cookies therefore authenticates the attacker's page too —
same-origin policy does not apply to WebSockets, and there is no preflight.

`allowedOrigins` closes that, and is **opt-in**: unset, nothing is checked.

```typescript
createWSApp("/ws", [], {
	allowedOrigins: ["https://app.example"],
	verify: (_payload, req) => sessionFromCookie(req),
});
```

An unlisted `Origin` is answered `403 Origin not allowed` and never reaches
`verify`. The array form **permits a missing `Origin`**, because only browsers
send the header and non-browser clients (the stock Deno client included) send
none — the check exists to stop browsers. Pass a function instead when that is
too lax, or when the allowed set is dynamic:

```typescript
allowedOrigins: (origin, req) => origin !== null && isTenantOrigin(origin),
```

Token authentication in the `auth` payload is not exposed this way: another
site's page cannot read your token, only ride your cookies.

---

### `WSService`

Owns every connection, direct messages, the room index, presence and delivery.
Usable standalone
— `new WSService(options)`, driven from any `Deno.serve` handler — or through
`createWSApp`, which mounts it as a demino app.

##### `handleUpgrade(request): Response`

Upgrades an HTTP request and takes ownership of the socket. Return the 101
response from your route handler unmodified. With `allowedOrigins` set, a
disallowed request is answered `403` instead and nothing is upgraded.

##### `send(clientId, payload): boolean`

Sends a direct message to one connected client — the server-to-client half of
the protocol core. It arrives with nothing but `payload`, through the client's
`message` event; no room handler sees it.

Returns `true` when handed to an open socket, `false` when no such client is
connected **to this instance** (or the payload could not be encoded).
Instance-local: nothing is propagated through the adapter.

##### `publish(room, payload, namespace?, from?): Promise<number>`

Injects a message into a room from server-side code. Delivered messages carry `from: null`
unless you pass one, which is how clients tell server pushes from peer traffic.

`namespace` defaults to `"default"`. Resolves with the recipients on **this
instance**; peers are propagated to but not counted.

##### `broadcast(room, payload, from?): Promise<number>`

Publishes into a room across every namespace. `allowBroadcast` does not apply —
that gate exists to stop _clients_ crossing the boundary, and code calling this
is already inside the trust boundary.

##### `members(room, namespace?): string[]`

Every subscriber of the room, whether or not they asked for presence — presence
controls who gets _told_ about membership, not who counts as a member.
Instance-local.

##### `stats(): WSStats`

Counts only, never client ids — but `namespaces` is keyed by namespace name, so
in a multi-tenant deployment it enumerates the tenants that are online. Hence
the `/stats` route only exists behind `httpAuth`; this method is for in-process
use.

##### `close(): Promise<void>`

Closes every connection and releases all timers. Idempotent. Sockets close with
`1001 GOING_AWAY`, which is _recoverable_ — clients reconnect, which is what you
want for a rolling deploy.

##### `logger`

Assignable. Set to `null` to silence.

---

## Server types

### `WSApp`

```typescript
{
	app: Demino; // mount it, or serve it directly
	service: WSService; // inject messages, read stats, shut down
}
```

### `WSAppOptions`

`WSServiceOptions` plus `httpAuth` and `deminoOptions` — see the
[`createWSApp` table](#createwsappmountpath-middlewares-options).

### `WSServiceOptions`

Everything in that table except `httpAuth` and `deminoOptions`.

### `WSConnectionContext`

```typescript
{
	clientId: string;
	namespace: string;
	meta: Record<string, unknown>; // whatever verify() returned
	request: Request; // the original upgrade request
}
```

Passed to `onMessage` and `allowBroadcast`.

### `WSStats`

```typescript
{
	connections: number; // authenticated
	pending: number; // not yet authenticated
	rooms: number; // distinct room names in use
	namespaces: Record<string, number>; // connections per namespace
}
```

### `WSPubSubAdapter`

```typescript
publish(envelope: WSBroadcastEnvelope): Promise<void>
onRemote(cb: (envelope: WSBroadcastEnvelope) => void): () => void
close(): Promise<void>
```

Local delivery is always the service's job; an adapter only propagates to peer
instances and receives what peers send. That division is why `recipients` counts
are instance-local and documented as best-effort telemetry.

A rejection from `publish()` is logged and swallowed — failed gossip must not
fail a publish that already succeeded locally.

### `WSPubSubLocal`

The default adapter, and the only one that ships today: there are no peer
instances, so propagation is a no-op. Everything still works — the service
delivers locally regardless of adapter. Redis / Deno-KV adapters are an
unimplemented seam.

### `WSBroadcastEnvelope`

```typescript
{
	namespace: string | null; // null for a cross-namespace broadcast
	message: WSRoomMessage;
}
```

Room messages only — direct messages (`service.send()`) never cross instances.

---

## Protocol

Also re-exported from `@marianmeres/ws`. Dependency-free, for anyone
implementing this protocol against a different server or client.

### `WSMessage<T>`

```typescript
{
	payload: T;
	room?: string;        // present on a room delivery only
	namespace?: string;   // 〃
	from?: string | null; // 〃
	timestamp?: number;   // 〃
}
```

Any message, as the [`message`](#wsevents) event delivers it. Only `payload` is
guaranteed: a direct message from the server carries nothing else, a room
delivery carries all of it (see `WSRoomMessage`). `room` tells them apart.

`payload` is **opaque**: never inspected, never mutated. Your payload may carry
its own `type` field and nothing collides.

### `WSRoomMessage<T>`

```typescript
{
	payload: T;
	room: string;
	namespace: string;
	from: string | null;
	timestamp: number; // server-assigned epoch ms
}
```

A message delivered through a room — what room handlers receive, and a
`WSMessage` with every routing field present.

`from` is `null` when the message was injected server-side. For a broadcast,
`namespace` is the receiver's own — not the sender's.

### `WSPresenceEvent`

```typescript
{
	event: "sync" | "join" | "leave";
	room: string;
	namespace: string;
	clientId: string | null; // who joined/left; null for sync
	members: string[];       // full membership after this event
	timestamp: number;
}
```

`sync` carries the full snapshot and fires on every (re)subscribe, including
after a reconnect — membership may have changed entirely while the client was
away.

### `WSPublishResult`

```typescript
{
	recipients: number;
}
```

Sockets the message was handed to **on the receiving server instance**, as
reported for `publish()` / `broadcast()`. Best-effort telemetry, never a
delivery guarantee.

### `AuthResult`

What the server's `verify()` hook returns. `null` rejects the connection.

```typescript
{
	clientId?: string;                // default: generated
	namespace?: string;               // overrides the client's request
	meta?: Record<string, unknown>;   // surfaces on WSConnectionContext
}
```

### `WSRequestedIdentity`

The third argument to `verify()`: what the client proposed in its `auth` frame.
Hints, not facts — see
[Security: namespace isolation](#security-namespace-isolation).

```typescript
{
	clientId?: string;   // absent unless the frame carried a usable one
	namespace: string;   // DEFAULT_NAMESPACE when the frame carried none
}
```

### `WSErrorInfo`

```typescript
{
	code: string; // machine-readable — see ERROR_CODE
	message: string; // human-readable. Never parse this
	details?: unknown; // optional structured detail for the application; any JSON
}
```

The error carried by `nack` and `error` frames. `details` is the server
application's to define and the client application's to interpret — the
protocol never inspects it. Where `message` tells a person what went wrong,
`details` tells code what to do about it.

### `SubRequest`

```typescript
{
	room: string;
	presence?: boolean;
}
```

### `ClientFrame` / `ServerFrame` / `WSFrame`

Discriminated unions over `FRAME`, keyed on `type`. `WSFrame` is either
direction. You need these only to write a custom `encode`/`decode` or a
third-party implementation.

| Direction       | Core                                           | Rooms extension                    |
| --------------- | ---------------------------------------------- | ---------------------------------- |
| client → server | `auth`, `msg`, `ping`                          | `sub`, `unsub`, `pub`, `broadcast` |
| server → client | `hello`, `msg`, `ack`, `nack`, `pong`, `error` | `presence`                         |

A `msg` frame minus its `type` field _is_ a `WSMessage` — no translation layer,
no divergence between wire names and API names. See [PROTOCOL.md](PROTOCOL.md)
for every frame's fields.

### `PresenceEventType`

`"sync" | "join" | "leave"` — the value union of `PRESENCE`.

### `WSEncoder` / `WSDecoder`

```typescript
type WSEncoder = (frame: WSFrame) => string | ArrayBufferView | ArrayBuffer;
type WSDecoder = (raw: string | ArrayBuffer) => WSFrame;
```

Default to JSON on both sides. Override both ends together — a mismatch closes
the socket with `4400 PROTOCOL_ERROR`.

---

## Errors

All extend `WSError`, so callers can branch on `instanceof` rather than
string-matching messages.

| Error                   | Thrown when                                           | Extra             |
| ----------------------- | ----------------------------------------------------- | ----------------- |
| `WSTerminatedError`     | Terminal close code                                   | `code`, `reason`  |
| `WSConnectTimeoutError` | `connectTimeout` elapsed (retrying continues)         |                   |
| `WSTimeoutError`        | The send's deadline elapsed — still queued, or no ack |                   |
| `WSConnectionLostError` | Socket closed while the frame awaited its ack         |                   |
| `WSOutboxDropError`     | Evicted from a full outbox                            |                   |
| `WSRemoteError`         | Server sent a `nack` (or an `error` frame)            | `code`, `details` |
| `WSNotConnectedError`   | Sent while disconnected with `outboxMaxSize: 0`       |                   |
| `WSDisposedError`       | Client was disposed                                   |                   |

`WSTerminatedError` also rejects a `connect()` left pending by a close the
`reconnect` policy declined to retry; the client is `idle` then, not
`terminated`. `WSTimeoutError`'s deadline is the client's `sendTimeout`, or the
`timeout` passed to that `send()`; its message names the one that applied.

`WSRemoteError` is also what a server-side `onMessage` throws to refuse a
message: `new WSRemoteError({ code, message, details? })`. Its `code`, `message`
and `details` reach the client unchanged — `details` is `undefined` when the
server sent none. It is re-exported from `@marianmeres/ws/server` for that.

---

## Constants

### `PROTOCOL_VERSION`

`2`. Announced by the server in `hello`; a mismatch warns rather than fails.
Version 2 split the protocol into a required core and the optional rooms
extension; a version-1 server still works for rooms.

### `DEFAULT_NAMESPACE`

`"default"` — used when the client does not specify one.

### `CLOSE`

WebSocket close codes. The `4xxx` range is reserved for application use by
RFC 6455.

| Name              | Code | Reconnects?          |
| ----------------- | ---- | -------------------- |
| `NORMAL`          | 1000 | yes (server restart) |
| `GOING_AWAY`      | 1001 | yes                  |
| `ABNORMAL`        | 1006 | yes                  |
| `INTERNAL_ERROR`  | 1011 | yes                  |
| `AUTH_FAILED`     | 4001 | **no**               |
| `AUTH_TIMEOUT`    | 4002 | yes                  |
| `FORBIDDEN`       | 4003 | **no**               |
| `IDLE_TIMEOUT`    | 4008 | yes                  |
| `RATE_LIMITED`    | 4009 | yes                  |
| `FRAME_TOO_LARGE` | 4013 | yes                  |
| `PROTOCOL_ERROR`  | 4400 | yes                  |
| `CLIENT_GONE`     | 4900 | n/a (local)          |

### `DEFAULT_TERMINAL_CLOSE_CODES`

`[4001, 4003]` — the default for `terminalCloseCodes`. Everything _not_ listed
reconnects, including a server-sent `1000`.

### `FRAME`

Frame type discriminators — the `type` field of every frame. See
[`ClientFrame` / `ServerFrame`](#clientframe--serverframe--wsframe).

### `ERROR_CODE`

`"unauthorized" | "forbidden" | "bad_request" | "rate_limited" | "unsupported" | "internal"`
— the standard `code` values on `WSErrorInfo` and `WSRemoteError`.
`"unsupported"` answers a frame type the server does not implement, e.g. rooms
against a core-only server. A server application may also use codes of its own
when it refuses a message.

### `PRESENCE`

`"sync" | "join" | "leave"` — presence event discriminators.
