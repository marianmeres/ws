# API

Three entry points:

| Import                     | Contains                                  | Runtime   |
| -------------------------- | ----------------------------------------- | --------- |
| `@marianmeres/ws`          | the client, plus everything from protocol | any       |
| `@marianmeres/ws/server`   | the reference server                      | Deno only |
| `@marianmeres/ws/protocol` | wire definitions only, dependency-free    | any       |

---

## Client

### `createWSClient(options?)`

Creates a client. Nothing connects until the first `connect()`, `subscribe()`
or `publish()`.

`WSClient` is exported too — `new WSClient(options)` is the same thing,
following the `PubSub` / `createPubSub` precedent.

**Parameters**

| Name                 | Type                                | Default            | Description                                                                   |
| -------------------- | ----------------------------------- | ------------------ | ----------------------------------------------------------------------------- |
| `url`                | `string \| URL`                     | `"/ws"`            | `ws(s)://`, or `http(s)://` (upgraded), or a path resolved against `location` |
| `namespace`          | `string`                            | `"default"`        | Isolation boundary                                                            |
| `clientId`           | `string`                            | generated          | Preferred id; the server may override                                         |
| `rooms`              | `string[]`                          | `[]`               | Rooms joined on every (re)connect                                             |
| `auth`               | `() => unknown \| Promise<unknown>` | —                  | Auth payload; called before _every_ (re)connect                               |
| `autoConnect`        | `boolean`                           | `true`             | First `subscribe()`/`publish()` starts the connection                         |
| `logger`             | `Logger \| null`                    | `createClog("ws")` | `null` silences                                                               |
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

**Returns** `WSClient`

**Example**

```typescript
import { createWSClient } from "@marianmeres/ws";

const ws = createWSClient({
	url: "wss://example.com/ws",
	namespace: "org-123",
	auth: () => session.token, // re-read on every reconnect
});

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

- `WSTerminatedError` — a terminal close code
- `WSConnectTimeoutError` — `connectTimeout` elapsed. Retrying continues in the
  background, so this bounds _your await_, not the connection attempt
- `WSDisposedError` — the client was disposed

Ordinary network failure never rejects; that is what the infinite retry is for.

##### `disconnect(): void`

Stops retrying and closes the socket. **Resumable** — handlers, room
subscriptions and buffered sends all survive, so a later `connect()` picks up
where it left off.

##### `dispose(): void`

Terminal teardown: disconnects, then drops every handler, room, timer and
pending promise. Pending sends reject with `WSDisposedError`. The instance is
unusable afterwards.

#### Subscriptions

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
- `handler` (`MessageHandler<T>`) — receives every message published to the room
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

#### Sending

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

| Member            | Type                      | Notes                                           |
| ----------------- | ------------------------- | ----------------------------------------------- |
| `state`           | Svelte store of `WSState` | Fires immediately, then on every change         |
| `connected`       | `boolean`                 | `true` only in `open` — not merely socket-open  |
| `connectionState` | `WSConnectionState`       |                                                 |
| `clientId`        | `string \| null`          | Server-assigned; `null` until connected         |
| `namespace`       | `string`                  | The server's assignment wins over the request   |
| `rooms`           | `string[]`                | Rooms currently held                            |
| `socket`          | `WebSocket \| null`       | Escape hatch; sending on it bypasses the outbox |
| `url`             | `URL`                     | A copy — mutating it does nothing               |
| `logger`          | `Logger \| null`          | Assignable; set to `null` to silence            |
| `dump()`          | `Record<string, unknown>` | Debug snapshot; shape is not stable API         |

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
type MessageHandler<T = unknown> = (msg: WSMessage<T>) => void;
type PresenceHandler = (event: WSPresenceEvent) => void;
```

A throwing handler is caught, reported through the `error` event, and does not
stop delivery to the others.

### `WSEvents`

| Event          | Payload                            |
| -------------- | ---------------------------------- |
| `open`         | `void` — socket open, pre-auth     |
| `connected`    | `{ clientId, namespace }`          |
| `message`      | `WSMessage` — firehose, every room |
| `presence`     | `WSPresenceEvent`                  |
| `close`        | `{ code, reason, willReconnect }`  |
| `reconnecting` | `{ attempt, delay }`               |
| `terminated`   | `{ code, reason }` — gave up       |
| `error`        | `Error`                            |

`error` means something failed but the client carried on (a decode failure, a
throwing handler). `terminated` is the only non-retrying exit.

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

| Method | Path                          | Returns                     | Notes                                     |
| ------ | ----------------------------- | --------------------------- | ----------------------------------------- |
| GET    | `/`                           | 101, or 426 without upgrade | WebSocket upgrade                         |
| GET    | `/stats`                      | `WSStats`                   | Guarded by `httpAuth` when supplied       |
| POST   | `/publish/[namespace]/[room]` | `{ ok: true, recipients }`  | Requires `httpAuth`, else **not mounted** |
| POST   | `/broadcast/[room]`           | `{ ok: true, recipients }`  | Requires `httpAuth`, else **not mounted** |

The POST routes take the JSON request body as the message payload.

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

---

### `WSService`

Owns every connection, the room index, presence and delivery. Usable standalone
— `new WSService(options)`, driven from any `Deno.serve` handler — or through
`createWSApp`, which mounts it as a demino app.

##### `handleUpgrade(request): Response`

Upgrades an HTTP request and takes ownership of the socket. Return the 101
response from your route handler unmodified.

##### `publish(room, payload, namespace?, from?): Promise<number>`

Injects a message from server-side code. Delivered messages carry `from: null`
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

Counts only, never client ids, so it stays safe to expose unguarded in
development.

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

Passed to `allowBroadcast`.

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
	message: WSMessage;
}
```

---

## Protocol

Also re-exported from `@marianmeres/ws`. Dependency-free, for anyone
implementing this protocol against a different server or client.

### `WSMessage<T>`

```typescript
{
	room: string;
	namespace: string;
	from: string | null;
	payload: T;
	timestamp: number; // server-assigned epoch ms
}
```

`from` is `null` when the message was injected server-side. For a broadcast,
`namespace` is the receiver's own — not the sender's.

`payload` is **opaque**: never inspected, never mutated. Your payload may carry
its own `type` field and nothing collides.

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

Sockets the message was handed to **on the receiving server instance**.
Best-effort telemetry, never a delivery guarantee.

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
}
```

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

| Direction       | Frames                                                     |
| --------------- | ---------------------------------------------------------- |
| client → server | `auth`, `sub`, `unsub`, `pub`, `broadcast`, `ping`         |
| server → client | `hello`, `ack`, `nack`, `msg`, `presence`, `pong`, `error` |

A `msg` frame minus its `type` field _is_ a `WSMessage` — no translation layer,
no divergence between wire names and API names.

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

| Error                   | Thrown when                                     | Extra            |
| ----------------------- | ----------------------------------------------- | ---------------- |
| `WSTerminatedError`     | Terminal close code                             | `code`, `reason` |
| `WSConnectTimeoutError` | `connectTimeout` elapsed (retrying continues)   |                  |
| `WSTimeoutError`        | `sendTimeout` elapsed with no acknowledgement   |                  |
| `WSConnectionLostError` | Socket closed while the frame was in flight     |                  |
| `WSOutboxDropError`     | Evicted from a full outbox                      |                  |
| `WSRemoteError`         | Server sent a `nack`                            | `code`           |
| `WSNotConnectedError`   | Sent while disconnected with `outboxMaxSize: 0` |                  |
| `WSDisposedError`       | Client was disposed                             |                  |

---

## Constants

### `PROTOCOL_VERSION`

`1`. Announced by the server in `hello`; a mismatch warns rather than fails.

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

`"unauthorized" | "forbidden" | "bad_request" | "rate_limited" | "internal"` —
the `code` on `WSErrorInfo` and `WSRemoteError`.

### `PRESENCE`

`"sync" | "join" | "leave"` — presence event discriminators.
