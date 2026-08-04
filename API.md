# API

## Functions

### `createWSClient(options?)`

Creates a client. `WSClient` is also exported for `new WSClient(options)` — same
thing, following the `PubSub` / `createPubSub` precedent.

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

---

### `createWSApp(mountPath?, middlewares?, options?)`

Creates the reference server. Import from `@marianmeres/ws/server` (JSR only).

**Parameters**

| Name                         | Type                                   | Default                   | Description                                                                    |
| ---------------------------- | -------------------------------------- | ------------------------- | ------------------------------------------------------------------------------ |
| `mountPath`                  | `string`                               | `"/ws"`                   | Demino mount path                                                              |
| `middlewares`                | `DeminoHandler[]`                      | `[]`                      | Applied to all routes                                                          |
| `options.verify`             | `(payload, req) => AuthResult \| null` | —                         | Return `null` (or throw) to reject with `4001`. Absent means no authentication |
| `options.allowBroadcast`     | `(ctx, room) => boolean`               | **deny**                  | Gate for cross-namespace broadcast                                             |
| `options.httpAuth`           | `DeminoHandler`                        | —                         | Guards the HTTP routes. **Without it they are not mounted**                    |
| `options.deminoOptions`      | `DeminoOptions`                        | —                         | Passed through to `demino()`                                                   |
| `options.authTimeout`        | `number`                               | `5_000`                   | Deadline for the `auth` frame → `4002`                                         |
| `options.idleTimeout`        | `number`                               | `60_000`                  | Reap silent connections → `4008`                                               |
| `options.maxFrameSize`       | `number`                               | `262144`                  | Oversized frames → `4013`                                                      |
| `options.maxFramesPerSecond` | `number`                               | `100`                     | Rate cap → `4009`                                                              |
| `options.adapter`            | `WSPubSubAdapter`                      | `WSPubSubLocal`           | Cross-instance fan-out                                                         |
| `options.logger`             | `Logger \| null`                       | `createClog("ws:server")` | `null` silences                                                                |

**Returns** `{ app: Demino, service: WSService }`

---

### `backoffDelay(attempt, base, max, rnd?)`

Exponential backoff with equal jitter. Returns a delay in `[d/2, d]` where
`d = min(max, base * 2^(attempt-1))`. Exported mainly so the curve is testable.

## Types

### `WSClient`

```typescript
connect(): Promise<void>
disconnect(): void
dispose(): void

subscribe<T>(room, handler, options?): Promise<Unsubscriber>
unsubscribe(room): Promise<void>
isSubscribed(room): boolean
members(room): string[]

publish<T>(room, payload, namespace?): Promise<{ recipients: number }>
broadcast<T>(room, payload): Promise<{ recipients: number }>

on<K>(event, cb): Unsubscriber
once<K>(event, cb): Unsubscriber

get state(): { subscribe(cb): Unsubscriber }   // Svelte store contract
get connected(): boolean
get connectionState(): WSConnectionState
get clientId(): string | null
get namespace(): string
get rooms(): string[]
get socket(): WebSocket | null
get url(): URL
dump(): Record<string, unknown>
```

`connect()` is idempotent — concurrent calls share one promise, and it resolves
immediately when already connected. It rejects **only** where retrying cannot
help: `WSTerminatedError` (terminal close code) or `WSConnectTimeoutError`
(`connectTimeout` elapsed; retrying continues in the background). Ordinary
network failure never rejects.

`subscribe()` attaches the handler **synchronously**, before any frame goes out,
so nothing arriving between the request and its acknowledgement is lost. When
connected it awaits the server's acknowledgement, so a refused subscription
rejects here; when not connected it resolves once registered, and the
subscription is established by the re-subscribe on the next connect.

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

### `WSState`

```typescript
{
	state: WSConnectionState;
	connected: boolean;
	connecting: boolean;
	attempt: number;
	lastError: Error | null;
}
```

### `WSConnectionState`

`"idle" | "connecting" | "authenticating" | "open" | "reconnecting" | "terminated" | "disposed"`

### `WSMessage<T>`

```typescript
{
	room: string;
	namespace: string;
	from: string | null;
	payload: T;
	timestamp: number;
}
```

`from` is `null` when the message was injected server-side, which is how a
client distinguishes server pushes from peer traffic.

### `WSPresenceEvent`

```typescript
{ event: "sync" | "join" | "leave"; room: string; namespace: string;
  clientId: string | null; members: string[]; timestamp: number }
```

`sync` carries the full snapshot and has a `null` `clientId`. It fires on every
(re)subscribe, including after a reconnect — membership may have changed
entirely while the client was away.

### `WSService`

```typescript
handleUpgrade(request): Response
publish(room, payload, namespace?, from?): Promise<number>
broadcast(room, payload, from?): Promise<number>
members(room, namespace?): string[]
stats(): WSStats
close(): Promise<void>
```

### `WSPubSubAdapter`

```typescript
publish(envelope: WSBroadcastEnvelope): Promise<void>
onRemote(cb: (envelope: WSBroadcastEnvelope) => void): () => void
close(): Promise<void>
```

Local delivery is always the service's job; an adapter only propagates to peer
instances. Only `WSPubSubLocal` (a no-op) ships today.

### Errors

All extend `WSError`.

| Error                   | Thrown when                                      |
| ----------------------- | ------------------------------------------------ |
| `WSTerminatedError`     | Terminal close code; carries `code` and `reason` |
| `WSConnectTimeoutError` | `connectTimeout` elapsed (retrying continues)    |
| `WSTimeoutError`        | `sendTimeout` elapsed with no acknowledgement    |
| `WSOutboxDropError`     | Evicted from a full outbox                       |
| `WSRemoteError`         | Server sent a `nack`; carries `code`             |
| `WSNotConnectedError`   | Sent while disconnected with `outboxMaxSize: 0`  |
| `WSDisposedError`       | Client was disposed                              |

## Constants

### `PROTOCOL_VERSION`

`1`. Announced by the server in `hello`; a mismatch warns rather than fails.

### `CLOSE`

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

### `FRAME`, `ERROR_CODE`, `PRESENCE`, `DEFAULT_NAMESPACE`

Frame type, error code and presence event discriminators, and the `"default"`
namespace. See `@marianmeres/ws/protocol`.
