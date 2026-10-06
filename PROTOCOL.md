# Wire protocol and server implementation guide

`@marianmeres/ws` ships a client that runs anywhere and a reference server that
runs only on Deno. This document describes the wire protocol precisely enough to
implement a compatible server in **any language**, so that the stock client works
against it unchanged.

The protocol has two layers:

- **Core — required.** A handshake, messages in both directions (optionally
  acknowledged, optionally answered with a reply), and a heartbeat. This is a
  plain data channel between the client and _the server_, and it is all a server
  has to implement.
- **Rooms extension — optional.** Namespaces, rooms, publishing between clients,
  presence and broadcast: the server as a relay. The client sends these frames
  only when the application calls the rooms API (`subscribe()`, `publish()`,
  `broadcast()`), so a core-only server never sees them — and if one arrives
  anyway, it answers `unsupported` and the client fails fast.

**Implementing only the core?** Read sections 1–4, 6–8 and Appendix A; skip 5
and 9. Section 8 is a complete core server in Python: about 135 lines of code,
plus the comments explaining them.

Both Python servers (sections 8 and 9) pass the conformance script in Appendix
A, which drives the real client against them. Use them as the executable half of
this specification.

Written against `@marianmeres/ws` 0.5.0, **protocol version 2**. The normative
definitions live in the package's `src/protocol/` (`constants.ts`, `frames.ts`),
also published dependency-free as `@marianmeres/ws/protocol`.

---

## 1. The short version

### Core — every server

Seven rules. Everything else in the core is detail.

1. Transport is a standard WebSocket. Every **frame** is one JSON object with a
   string `type` field, sent as one UTF-8 **text** WebSocket message.
2. The first frame the client sends is `auth`, carrying the application's
   credentials in `payload`. Reply `{"type": "hello", "protocol": 2}` within
   **10 s**, or the client drops the socket and retries.
3. Reject authentication by closing with code **4001**. That, and 4003, are the
   only codes after which the client stops reconnecting. Every other close code
   — including a plain 1000 — makes it reconnect.
4. A client message is `{"type": "msg", "payload": …}`. **Without an `id`** it
   is fire-and-forget: handle it, send nothing back. **With an `id`** the client
   is waiting: answer with exactly one `ack` — optionally carrying a reply in
   `payload` — or one `nack`, echoing the `id`. The client gives up after 30 s.
5. Send the client a message at any time with `{"type": "msg", "payload": …}`.
6. Answer every `ping` with `pong`, **promptly — even while a message is still
   being handled.** The client pings every 25 s and drops the socket 10 s after
   an unanswered ping.
7. Any other frame: reply `nack` with code `unsupported` if it carries an `id`,
   an `error` with the same code if it does not. Never silently ignore a frame
   with an `id` — the client would wait 30 s for an answer.

### Rooms extension — only if you want rooms

8. `hello` also carries the connection's `clientId` and `namespace`.
9. A room is scoped by `(room, namespace)`. A `pub` is delivered as a `msg` —
   with `room`, `namespace`, `from` and `timestamp` — to every subscriber of
   that pair, **including the publisher** when it is subscribed itself.
10. A client may publish only into its own namespace. `broadcast` crosses
    namespaces and must be denied (`nack`, code `forbidden`) unless you
    explicitly allow it.
11. Presence is opt-in per subscription: a `sync` snapshot to the subscriber,
    then `join`/`leave` deltas to the other presence subscribers of the room.
12. Handle a connection's rooms frames strictly in order. Right after `hello` the
    client re-sends all its subscriptions and then flushes buffered publishes
    without waiting for the ack — if a `pub` overtakes the `sub`, it lands in a
    room the server has not registered yet.
13. When a second connection authenticates with a client id that is already
    connected, close the old one with 1001 and keep the new one.

---

## 2. Transport and encoding

**Terminology.** _Frame_ in this document means one JSON protocol envelope,
carried in exactly one WebSocket text message — not an RFC 6455 fragment.
_Message_ means the application-level object a `msg` frame carries: its
`payload`, plus the routing fields `room`, `namespace`, `from` and `timestamp`
when it was delivered through a room.

- WebSocket, RFC 6455. Any path — the client is configured with the full URL
  (the default is `/ws` on the page origin). Subprotocols are not used.
- One frame = one JSON object = one WebSocket text message. No batching, no
  delimiters. The client sets `binaryType` to `arraybuffer` and the reference
  server's sockets already default to it, so a binary WebSocket message decodes
  as UTF-8 JSON by default and a custom decoder receives an `ArrayBuffer` — but
  send text.
- `type` is the discriminator. Unknown fields must be ignored. Optional fields are
  simply absent.
- `payload` is **opaque**. Never inspect, validate or mutate it beyond a size
  limit — unless it is addressed to your own application (a client `msg`), in
  which case it is yours to interpret. Any JSON value is legal, including
  `null`. The application puts its own protocol inside, and that protocol may
  have its own `type` field — the two never collide.
- Correlation ids (`id`) are opaque strings generated by the client (currently
  12 base-36 characters, but never rely on that). Echo them verbatim.
- Unknown frame types received by the client are logged and ignored, so a server
  cannot rely on any frame not listed in section 4.

---

## 3. The core protocol

```
client                                            server
  │── WebSocket upgrade ───────────────────────────▶│
  │◀─ 101 Switching Protocols ─────────────────────│
  │── auth {payload} ──────────────────────────────▶│  authenticate payload
  │◀─ hello {protocol} ────────────────────────────│  (or close 4001)
  │── msg {payload} ───────────────────────────────▶│  fire-and-forget: handle it
  │── msg {id, payload} ───────────────────────────▶│  handle it, then answer:
  │◀─ ack {id, payload?} ──────────────────────────│  (or nack {id, error})
  │◀─ msg {payload} ───────────────────────────────│  push, whenever you like
  │── ping ────────────────────────────────────────▶│  every 25 s
  │◀─ pong ────────────────────────────────────────│  within 10 s, or the client drops
```

### 3.1 Handshake

The client opens the socket and sends `auth` as its very first frame:

```json
{ "type": "auth", "protocol": 2, "payload": { "token": "eyJhbGciOi…" } }
```

| Field       | Type             | Notes                                                                                                          |
| ----------- | ---------------- | -------------------------------------------------------------------------------------------------------------- |
| `protocol`  | number           | `2`. Informational; nothing to negotiate.                                                                      |
| `payload`   | any              | Whatever the application's `auth()` callback returned; `null` when it has none. This is what you authenticate. |
| `clientId`  | string, optional | The id the client would like — rooms extension (5.1). Sent only when the application configured one.           |
| `namespace` | string, optional | The namespace it would like — rooms extension (5.1). Sent only when the application configured one.            |

A core server ignores `clientId` and `namespace`. A protocol-1 client also sends
an `id` here and always sends `namespace`; ignore both — `auth` is never
acknowledged, `hello` is the reply.

The server then:

1. Authenticates `payload`, together with anything on the upgrade request
   (cookies, headers, query string).
2. On failure closes the socket with **4001** (`AUTH_FAILED`). Nothing needs to
   be sent first. Use **4003** (`FORBIDDEN`) for "valid credentials, but not
   allowed here". Both are terminal for the client: it stops retrying, rejects a
   pending `connect()` and emits `terminated`.
3. On success sends:

```json
{ "type": "hello", "protocol": 2 }
```

`protocol` is required. `clientId` and `namespace` belong to the rooms extension
(5.1): a core server may omit them, and the client then reports `clientId` as
`null` and `namespace` as whatever it requested (`"default"` if nothing).

**Timing.**

- The client waits **10 s** for `hello` after sending `auth`. Then it closes
  with 4002 and reconnects. If your authentication calls something slow, `hello`
  must still go out within that budget.
- The reference servers give the client **5 s** to send `auth`, then close with
  4002 (`AUTH_TIMEOUT`). The stock client sends `auth` as soon as its `auth()`
  callback resolves, so this only ever affects a misbehaving client.
- Any non-`auth` frame before authentication: reply with an `error` frame, code
  `unauthorized`, and keep the socket open. The stock client never does this.
- A second `auth` is ignored — both on an authenticated connection and on one
  whose first handshake is still in flight — so authentication runs at most once
  per socket and at most one `hello` goes out.
- A socket that closes while authentication is pending is never registered.
- A `protocol` mismatch in `hello` only produces a client-side warning, so bump
  the version only for a genuinely breaking change.

**Token refresh** needs nothing from the server: the client calls its `auth()`
callback again before every reconnect, so each new socket authenticates with a
fresh payload.

### 3.2 Messages

**Client → server.** What the application's `ws.send(payload)` produces:

```json
{ "type": "msg", "payload": { "op": "typing" } }
```

```json
{ "type": "msg", "id": "k3j9x0a1b2c3", "payload": { "op": "load", "doc": 42 } }
```

- **No `id`** — fire-and-forget (`ws.send(payload)`). Handle it and send nothing
  back. The client considered it done the moment it wrote it to the socket. If
  handling fails you _may_ report it with an `error` frame (section 6); the
  client surfaces that as an `error` event, uncorrelated.
- **With an `id`** — the client is waiting (`ws.send(payload, { ack: true })`).
  Answer with exactly one of:

```json
{ "type": "ack", "id": "k3j9x0a1b2c3", "payload": { "title": "Doc 42" } }
```

```json
{
	"type": "nack",
	"id": "k3j9x0a1b2c3",
	"error": { "code": "not_found", "message": "no such document" }
}
```

The ack's `payload` is the reply: any JSON value, and the client's `send()`
resolves with it. Omit it for a bare acknowledgement; the client then resolves
with `undefined` (while an explicit `null` resolves with `null`). A `nack`
rejects the client's `send()` with a `WSRemoteError` carrying `error.code` and
`error.message` unchanged — so besides the standard codes of section 6 you can
use codes of your own (`not_found`, `invalid_doc`, …) and the application can
branch on them. `message` is for humans; clients should never parse it. When a
code is not enough to act on, add `error.details` — any JSON value, for the
application's code rather than its user (section 6); it arrives as
`WSRemoteError.details`, untouched.

The client waits **30 s** (its `sendTimeout`, or a per-send `timeout` the
application chose) for the answer, counted from the moment the application
called `send()` — so a message buffered while offline spends part of that
budget in the queue.

**Server → client.** Send a message at any time after `hello`:

```json
{ "type": "msg", "payload": { "op": "progress", "done": 42 } }
```

It reaches the application through the client's `message` event. Nothing but
`payload` is needed. Do not add `room` unless you implement the rooms extension:
a `msg` carrying `room` is a room delivery (5.4) and is also routed to that
room's handlers.

**Order.** Frames of one connection arrive in the order they were sent. The
protocol does not require you to _handle_ messages in that order — replies are
correlated by `id` — but a data channel is generally expected to behave like
one, and both Python servers handle a connection's messages one at a time, in
arrival order. Whatever you choose, never let message handling delay a `pong`
(3.3).

**Delivery** is at-most-once. The client buffers messages while it is offline
(up to 100 by default) and flushes them right after the next `hello`, in order,
without waiting for anything. A message that was already written when the
socket died is not resent: the client rejects an acknowledged one with
`WSConnectionLostError`, and cannot even know about a fire-and-forget one. If
the application needs stronger guarantees, it builds them on top (idempotency
keys, a resync request after reconnect).

### 3.3 Heartbeat

Client → `{"type":"ping"}` every 25 s by default. Server → `{"type":"pong"}`,
immediately. Neither carries an id.

The client counts **any** inbound frame as proof of life, not just `pong`. It
drops the socket (close 4008, then reconnects) when 10 s pass after a ping with
no inbound frame at all.

That makes the pong the one thing that must never wait behind application
work. A server that reads a frame, awaits a 15-second request handler and only
then reads the next frame answers the ping queued behind it too late: the client
closes the socket mid-request and the reply is lost. Answer `ping` in the read
loop and run handlers elsewhere — a task or a per-connection queue, as the
Python servers do.

On the server, reap connections that have been silent for longer than the
client's ping interval plus margin. The reference uses **60 s** and closes with
**4008** (`IDLE_TIMEOUT`). Any inbound frame counts as activity.

Do not substitute WebSocket protocol-level ping/pong: browsers do not expose
them, which is why liveness lives at the application level.

### 3.4 Unsupported frames

Anything else an authenticated client sends — the rooms frames of section 5 on
a core-only server, or a frame type from a future protocol version — is
answered, never ignored:

```json
{
	"type": "nack",
	"id": "p8q2r5s7t9u1",
	"error": { "code": "unsupported", "message": "not supported here" }
}
```

…when it carries a string `id`, and an uncorrelated `error` frame with the same
code when it does not. Keep the socket open.

This single rule is what makes the rooms extension optional: a `subscribe()`
against a core-only server rejects at once with `WSRemoteError` code
`unsupported`, instead of timing out 30 s later.

### 3.5 Closing

| Code | Name              | Sent by | Meaning                                             | Client reaction               |
| ---- | ----------------- | ------- | --------------------------------------------------- | ----------------------------- |
| 1000 | `NORMAL`          | server  | Normal closure, e.g. restart                        | reconnects                    |
| 1001 | `GOING_AWAY`      | server  | Shutdown, replaced connection                       | reconnects                    |
| 1006 | `ABNORMAL`        | —       | No close handshake (network drop)                   | reconnects                    |
| 1011 | `INTERNAL_ERROR`  | server  | Unexpected server-side condition                    | reconnects                    |
| 4001 | `AUTH_FAILED`     | server  | Authentication rejected                             | **terminal** — stops retrying |
| 4002 | `AUTH_TIMEOUT`    | both    | No `auth` in time / no `hello` in time              | reconnects                    |
| 4003 | `FORBIDDEN`       | server  | Authenticated but not permitted                     | **terminal** — stops retrying |
| 4008 | `IDLE_TIMEOUT`    | both    | Silent connection reaped / pong deadline            | reconnects                    |
| 4009 | `RATE_LIMITED`    | server  | Too many frames per second                          | reconnects                    |
| 4013 | `FRAME_TOO_LARGE` | server  | Frame exceeded the size limit                       | reconnects                    |
| 4400 | `PROTOCOL_ERROR`  | both    | Malformed frame; client-side, its `auth` hook threw | reconnects                    |
| 4900 | `CLIENT_GONE`     | client  | Application called `disconnect()`/`dispose()`       | n/a — deliberate              |

Reconnect backoff on the client: 500 ms doubling up to a 30 s ceiling, with
jitter, plus an immediate retry when the browser reports `online` or the tab
becomes visible again. Expect a fleet to come back within seconds of a restart.

Graceful shutdown: close every socket with 1001 (or 1000). Clients reconnect,
which is what a rolling deploy wants.

From the server's point of view a reconnect is a brand-new connection: the
client runs `auth` again with a freshly produced payload, receives `hello`, and
flushes whatever it buffered meanwhile. Nothing carries over from the previous
socket; whenever a connection closes, for any reason, forget it (with rooms,
also 5.2).

---

## 4. Frame reference

### 4.1 Client → server

| `type`      | Layer | Fields                                           | Reply                                              |
| ----------- | ----- | ------------------------------------------------ | -------------------------------------------------- |
| `auth`      | core  | `protocol`, `payload`, `clientId?`, `namespace?` | `hello`, or close 4001/4003                        |
| `msg`       | core  | `payload`, `id?`                                 | with `id`: `ack` (with an optional reply) / `nack` |
| `ping`      | core  | —                                                | `pong`                                             |
| `sub`       | rooms | `id`, `rooms: [{room, presence?}]`               | `presence` sync (per presence room), `ack`         |
| `unsub`     | rooms | `id`, `rooms: [string]`                          | `ack`                                              |
| `pub`       | rooms | `id`, `room`, `payload`, `namespace?`            | `ack` with `recipients`, or `nack`                 |
| `broadcast` | rooms | `id`, `room`, `payload`                          | `ack` with `recipients`, or `nack`                 |

A core-only server answers every `rooms` frame with `nack` `unsupported` (3.4).

**`auth`** — see 3.1.

```json
{ "type": "auth", "protocol": 2, "payload": { "token": "…" }, "namespace": "org-1" }
```

**`msg`** — see 3.2. `id` present only when the client awaits an answer.

```json
{ "type": "msg", "id": "k3j9x0a1b2c3", "payload": { "op": "load", "doc": 42 } }
```

**`ping`** — liveness probe, no id.

```json
{ "type": "ping" }
```

**`sub`** — join one or more rooms in the connection's namespace. Idempotent:
re-subscribing an already joined room is legal and is how the client turns
presence on for a room it already holds. `presence` may be `true`, `false` or
absent; the latest `sub` wins for that flag.

```json
{
	"type": "sub",
	"id": "p8q2r5s7t9u1",
	"rooms": [{ "room": "chat", "presence": true }, { "room": "feed" }]
}
```

**`unsub`** — leave rooms. Unknown rooms are ignored silently.

```json
{ "type": "unsub", "id": "v2w4x6y8z0a1", "rooms": ["chat"] }
```

**`pub`** — publish into a room of the connection's own namespace. `namespace`
is optional and, when present, must equal the connection's namespace (the stock
client only sends it when the application passes one explicitly).

```json
{ "type": "pub", "id": "b3c5d7e9f1g2", "room": "chat", "payload": { "text": "hello" } }
```

**`broadcast`** — publish into a room across every namespace. Gated.

```json
{
	"type": "broadcast",
	"id": "h4i6j8k0l2m3",
	"room": "announcements",
	"payload": { "text": "maintenance at 22:00" }
}
```

### 4.2 Server → client

| `type`     | Layer | Fields                                                              | What the client does                                                   |
| ---------- | ----- | ------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `hello`    | core  | `protocol`, `clientId?`, `namespace?`                               | Becomes connected; adopts any id and namespace; re-subscribes; flushes |
| `ack`      | core  | `id`, `payload?`, `recipients?`                                     | Resolves the pending request — with `payload`, or with `recipients`    |
| `nack`     | core  | `id`, `error: {code, message, details?}`                            | Rejects the pending request with `code`, `message` and `details`       |
| `msg`      | core  | `payload`; with rooms also `room`, `namespace`, `from`, `timestamp` | Emits a `message` event; with `room`, also calls that room's handlers  |
| `pong`     | core  | —                                                                   | Nothing beyond proof of life                                           |
| `error`    | core  | `error: {code, message, details?}`                                  | Emits an `error` event; the connection stays up                        |
| `presence` | rooms | `event`, `room`, `namespace`, `clientId`, `members`, `timestamp`    | Updates cached membership of `room`; calls presence handlers           |

An `ack`/`nack` for an unknown `id` is ignored. Unknown frame types are ignored.

**`hello`** — `protocol` required; `clientId` and `namespace` required by the
rooms extension, optional otherwise.

```json
{ "type": "hello", "protocol": 2 }
```

```json
{ "type": "hello", "protocol": 2, "clientId": "alice", "namespace": "org-1" }
```

**`ack`** — echoes the request's `id`. On a `msg` ack, `payload` is the optional
reply. On a `pub`/`broadcast` ack, `recipients` is an integer ≥ 0. Omit both on
`sub`/`unsub` acks.

```json
{ "type": "ack", "id": "k3j9x0a1b2c3", "payload": { "title": "Doc 42" } }
```

```json
{ "type": "ack", "id": "b3c5d7e9f1g2", "recipients": 2 }
```

**`nack`** — `error.code` is one of the codes in section 6 or, for a `msg`, one
of your own; `error.message` is free-form text for humans and is never parsed.

```json
{
	"type": "nack",
	"id": "h4i6j8k0l2m3",
	"error": { "code": "forbidden", "message": "broadcast not permitted" }
}
```

**`msg`** — a message for the application. Everything except `type` is handed to
it as-is, so the field names are the API.

A direct message (core) carries only `payload`:

```json
{ "type": "msg", "payload": { "op": "progress", "done": 42 } }
```

A room delivery (rooms extension) carries the routing fields too. The client
routes it by `room` alone, which must be exactly the string the client
subscribed with. `from` is the publisher's client id, or `null` for a message
injected by server-side code. `timestamp` is the server clock, integer epoch
milliseconds.

```json
{
	"type": "msg",
	"room": "chat",
	"namespace": "org-1",
	"from": "bob",
	"payload": { "text": "hello" },
	"timestamp": 1725000000000
}
```

**`presence`** — a membership change. `event` is `sync`, `join` or `leave`.
`clientId` is the joiner/leaver, or `null` for `sync`. `members` is the full
membership of `(room, namespace)` _after_ the event.

```json
{
	"type": "presence",
	"event": "join",
	"room": "chat",
	"namespace": "org-1",
	"clientId": "bob",
	"members": ["alice", "bob"],
	"timestamp": 1725000000000
}
```

**`pong`** and **`error`**:

```json
{ "type": "pong" }
```

```json
{ "type": "error", "error": { "code": "unauthorized", "message": "not authenticated" } }
```

---

## 5. The rooms extension

Everything in this section is optional. Skip it if the application only talks
to the server.

### 5.1 Identity

With rooms, `hello` carries the connection's identity:

```json
{ "type": "hello", "protocol": 2, "clientId": "alice", "namespace": "org-1" }
```

Resolve it from the `auth` frame, first match wins:

| Field       | 1st                         | 2nd                               | 3rd                     |
| ----------- | --------------------------- | --------------------------------- | ----------------------- |
| `clientId`  | assigned by your auth logic | `clientId` from the `auth` frame  | generated by the server |
| `namespace` | assigned by your auth logic | `namespace` from the `auth` frame | `"default"`             |

The client adopts whatever `hello` says — the server's values win over its own
request.

**Security.** The client's proposals are hints. In production derive both from
the verified identity (`clientId` from the user id, `namespace` from the tenant),
otherwise any client can claim any id — and evict its rightful owner, see 5.2 —
or join any namespace.

### 5.2 One connection per client id

When a connection authenticates with an id that is already registered: close the
existing connection with **1001** (reason, e.g., `replaced by new connection`),
remove it from every room — emitting `leave` presence events — and register the
newcomer. Make sure the old socket's close handler does not remove the _new_
registration when it eventually fires.

Why: after a half-open drop (laptop lid, mobile handover) the client reconnects
with the same id while the server still believes the old socket is alive.
Newcomer-wins is what makes that recover instead of accumulating ghosts.

Consequence: ids must be unique per connection **by contract**. Two tabs sharing
an id will evict each other forever, because 1001 is a reconnecting code. The
usual scheme is `"<user-id>#<per-tab-suffix>"`.

Whenever a connection closes, for any reason: remove it from every room, send
`leave` to the presence subscribers of those rooms, and forget it — but only
drop the registry entry if it still points at this socket.

### 5.3 Subscriptions, and the order after every reconnect

Immediately after `hello`, the client does the following, in this order, without
waiting in between:

1. Sends **one** `sub` frame listing every room it holds, with `presence: true`
   where a presence handler is attached.
2. Flushes everything it buffered while disconnected — `pub`, `broadcast` and
   `msg` frames alike.

Server obligation: process the rooms frames of one connection in arrival order,
and have the rooms from a `sub` registered before you look at the next frame. A
design that handles frames concurrently (a task per frame) violates this and
loses the flushed publishes. Handling `sub`/`unsub`/`pub`/`broadcast` inline in
the read loop is the simplest correct choice; the Python reference does that,
and queues only `msg` frames for its message worker.

A reconnect is a brand-new connection: presence subscribers of each room see a
`leave` for the old socket followed by a `join` for the new one.

### 5.4 Rooms, namespaces and publishing

Keep an index `(room, namespace) → set of client ids`. Rooms exist implicitly —
there is no creation step and no listing.

**Namespace** is the isolation boundary. Two connections in different namespaces
may subscribe to identically named rooms and never see each other's traffic. A
connection lives in exactly one namespace, fixed at `hello`.

Handling `pub`:

1. If the frame carries `namespace` and it differs from the connection's, reply
   `nack` with code `forbidden`. Nothing is delivered.
2. Build the message: `room` from the frame, `namespace` = the connection's,
   `from` = the connection's client id, `payload` verbatim, `timestamp` = now in
   epoch milliseconds.
3. Send `{"type": "msg", …}` to every connection in `(room, namespace)`. There is
   **no self-exclusion**: a publisher that is subscribed to the room receives its
   own message, and applications rely on that echo.
4. Reply `ack` with `recipients` = the number of sockets you handed the message
   to. The reference sends the messages first and the ack last.

Publishing into a room the sender is not subscribed to is allowed. A room with no
subscribers is simply acknowledged with `recipients: 0`.

`recipients` is best-effort telemetry, never a delivery guarantee, and it counts
only sockets on the instance that handled the frame.

**Server-side injection.** Your own code can push into rooms with the same `msg`
frame and `from: null`. That is how applications tell server pushes from peer
traffic in a room. (A direct message, 3.2, is the other way to reach a client:
one connection, no room.)

**Broadcast.** Deliver to `(room, ns)` for _every_ namespace `ns` that has
subscribers to `room`. Each receiver sees `namespace` = **its own** namespace,
not the sender's. `from` is the sender's id. Broadcast must be gated and denies
by default: reply `nack` with code `forbidden` and message such as
`broadcast not permitted` unless your policy allows this sender into this room.
On success, `ack` with `recipients` counted across all namespaces.

**Delivery guarantee** is at-most-once, as in the core. The server keeps no
history and never replays. If the application needs a backlog, that is an
application-level concern (an HTTP history endpoint, for instance) — not part of
this protocol.

### 5.5 Presence

Presence is per subscription, not per room: each connection declares in its
`sub` whether it wants to be _told_ about membership. Membership itself counts
every subscriber, presence or not.

On `sub` of room `R` by client `C` in namespace `N`:

1. Add `C` to the index for `(R, N)`; remember whether `C` wants presence
   (latest `sub` wins).
2. If `C` was not already a member: send `presence` `join` (`clientId: C`,
   `members` including `C`) to every **other** member of `(R, N)` that wants
   presence.
3. If `C` asked for presence: send `presence` `sync` (`clientId: null`, full
   `members`) to `C`. Also on a re-subscribe of a room `C` already held — that is
   how the client gets a snapshot when it turns presence on late, and after every
   reconnect.
4. Send `ack`.

That order — deltas to the others, then the snapshot, then the ack — is
deliberate. The snapshot precedes the ack so that the client's `subscribe()`
resolves with membership already populated.

On `unsub` of `R` by `C`, and on any close of `C`'s connection: remove `C` from
`(R, N)`, then send `presence` `leave` (`clientId: C`, `members` without `C`) to
the presence subscribers of `(R, N)`. `unsub` is then acknowledged.

The subject of a change never receives a delta about itself — it gets the `sync`
instead. Presence never crosses namespaces, even though broadcast does.

---

## 6. Errors

Two shapes, one rule: `nack` answers a specific request (it carries the `id`),
`error` is uncorrelated. Neither closes the connection on the client side; a
`nack` surfaces to the application as a rejected promise carrying `code`, an
`error` frame as an `error` event.

Both carry the same `error` object: `code` (machine-readable), `message` (for a
human to read, never to parse) and, optionally, `details` — any JSON value, for
the application. Where `message` says what went wrong, `details` lets code act
on it: a retry-after, the limit that was hit, the field that failed validation.
The protocol never inspects it; the client hands it to the application as
`WSRemoteError.details`, `undefined` when the server sent none. Send it only
when there is something to send.

```json
{
	"type": "nack",
	"id": "k3j9x0a1b2c3",
	"error": {
		"code": "busy",
		"message": "an avatar is being prepared, try again shortly",
		"details": { "time_left_seconds": 42 }
	}
}
```

| `code`         | Layer | Use it for                                                                     |
| -------------- | ----- | ------------------------------------------------------------------------------ |
| `unauthorized` | core  | Any non-`auth` frame before the handshake completed                            |
| `bad_request`  | core  | Malformed frame: missing `type`; with rooms, missing `room`, non-array `rooms` |
| `unsupported`  | core  | A frame type this server does not implement (3.4)                              |
| `rate_limited` | core  | Frame rate cap exceeded                                                        |
| `internal`     | core  | Unexpected server-side failure — never with the exception's text               |
| `forbidden`    | rooms | Publishing into a foreign namespace; a denied broadcast                        |
| _your own_     | core  | Refusing a client `msg` for an application reason (`not_found`, …)             |

Reference behaviour for malformed input:

- Not valid JSON: send `error` `bad_request`, then close with **4400**.
- Valid JSON that is not an object, or has no string `type`: send `error`
  `bad_request`, keep the socket.
- An unknown `type`: `unsupported`, as in 3.4. Keep the socket.
- A `msg` whose `id` is not a non-empty string: treat it as fire-and-forget.
- Your message handler fails: answer `internal` — `nack` when the `msg` had an
  `id`, `error` when not — and keep the socket. It is an application failure,
  not a broken connection. Log the details; do not send them.
- `sub`/`unsub` whose `rooms` is not an array, or `pub`/`broadcast` without a
  non-empty string `room`: send `nack` `bad_request`, keep the socket. Malformed
  entries _inside_ a well-formed `rooms` array are skipped silently.
- An unexpected failure in the server's own frame handling (not the
  application's handler): send `error` `internal`, then close with **1011**. The
  connection's bookkeeping may be half-applied; 1011 is recoverable, so the
  client reconnects into clean state.

---

## 7. Limits

These are the reference servers' values. Implementing them is recommended, not
required; if you do, reuse the close codes. All of them are recoverable, so the
client reconnects.

| Limit               | Reference | Close code             |
| ------------------- | --------- | ---------------------- |
| Time to send `auth` | 5 s       | 4002 `AUTH_TIMEOUT`    |
| Idle (no frame)     | 60 s      | 4008 `IDLE_TIMEOUT`    |
| Frame size          | 256 KiB   | 4013 `FRAME_TOO_LARGE` |
| Frames per second   | 100       | 4009 `RATE_LIMITED`    |

The core server in section 8 implements the first two and leaves frame size to
the `websockets` library (`max_size`, 1 MiB by default, closing with 1009 —
also recoverable).

---

## 8. Python: a core server

Everything a server needs when the application talks to the server and has no
use for rooms. Built on the [`websockets`](https://websockets.readthedocs.io/)
library, Python 3.10+. The protocol logic (`CoreServer`, `Client` and the wire
helpers) does not depend on the framework — to use it under FastAPI/Starlette,
aiohttp or anything else, replace the three socket calls it makes (`recv`,
`send`, `close`) with your framework's equivalents.

```bash
pip install "websockets>=13"
python ws_core_server.py                                    # ws://127.0.0.1:8765/ws
WS_AUTH_TIMEOUT=1 WS_IDLE_TIMEOUT=2 python ws_core_server.py  # timeouts Appendix A expects
```

Your application supplies two functions — everything below the "example app"
line is a stand-in for them:

- `authenticate(payload)` — receives what the client's `auth()` option returned;
  returns the user (anything but `None`) or `None` to reject the connection.
- `on_message(client, payload)` — called for every `ws.send(payload)`. Its return
  value is the reply the client receives when it sent with `{ ack: true }`
  (`None` means "no reply"). Raise `Reject(code, message, details=None)` to
  refuse the message with your own error code — and, when a code is not enough
  to act on, structured `details` for the client's code (section 6). Call
  `client.push(payload)` — now or any time later — to send the client a message
  of your own.

On the client side, that is all used like this:

```typescript
const ws = createWSClient({ url: "ws://127.0.0.1:8765/ws", auth: () => ({ token }) });

ws.on("message", (msg) => console.log("pushed:", msg.payload));

ws.send({ op: "push", data: 1 }); // fire-and-forget
const reply = await ws.send({ op: "echo", n: 1 }, { ack: true }); // { op: "echo", n: 1 }
```

It was verified against the real client (`@marianmeres/ws` 0.6.0) with the
script in Appendix A: all 11 core checks pass, on `websockets` 17.2 and Python
3.11 with deprecation warnings promoted to errors.

```python
"""
Core server for the @marianmeres/ws wire protocol, version 2.

This is everything a server needs to be compatible with the stock client when
the application talks to the server itself and has no use for rooms:

    client -> server          server -> client
    ----------------          ----------------
    auth   (first frame)      hello             or close 4001 to reject
    ping                      pong              answered at once, never queued
    msg    {payload}          -                 fire-and-forget: handled, no reply
    msg    {payload, id}      ack {id, payload} ...or nack {id, error}
    -                         msg {payload}     a push, at any time: client.push()
    anything else             nack / error      code "unsupported"

Rooms, namespaces, presence and broadcast belong to an optional extension. A
client that never calls subscribe(), publish() or broadcast() never sends one
of those frames; one that does gets an immediate "unsupported" from here
instead of a 30 s timeout.

The protocol logic does not depend on the framework; `websockets` only supplies
the socket. To run it elsewhere (FastAPI/Starlette, aiohttp, ...) replace the
three socket calls it makes: `recv`, `send` and `close`.

    pip install "websockets>=13"        # Python 3.10+
    python ws_core_server.py            # ws://127.0.0.1:8765/ws
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("ws")

PROTOCOL_VERSION = 2

# Close codes. The client stops reconnecting after 4001 and 4003, and only
# after those: every other code, a plain 1000 included, makes it come back.
CLOSE_AUTH_FAILED = 4001  # bad credentials                 -> client gives up
CLOSE_AUTH_TIMEOUT = 4002  # no `auth` frame in time         -> client retries
CLOSE_FORBIDDEN = 4003  # valid credentials, not allowed    -> client gives up
CLOSE_IDLE_TIMEOUT = 4008  # silent connection reaped        -> client retries
CLOSE_PROTOCOL_ERROR = 4400  # frame is not JSON             -> client retries


class Reject(Exception):
    """Raise from your message handler to refuse a message on purpose.

    The client's `send(payload, { ack: true })` then rejects with a
    `WSRemoteError` whose `code`, `message` and `details` are exactly these.
    `details` is optional and for the client's *code* — any JSON value, say a
    retry-after — where `message` is for its user. Any *other* exception is
    logged here and reported to the client as `internal`, without its text — a
    stack trace is not something to hand to a browser.
    """

    def __init__(self, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


@dataclass(eq=False)  # identity equality: every Client is a distinct connection
class Client:
    """One authenticated connection."""

    ws: ServerConnection
    user: Any  # whatever your `authenticate` returned
    # Messages waiting for the handler, in arrival order. See `_work`.
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue)

    async def push(self, payload: Any) -> bool:
        """Sends `payload` to this client; it arrives through the client's
        `message` event. Returns False when the connection is already gone.

        Callable at any time, from anywhere: inside your handler, from a
        background task, after the handler returned.
        """
        return await _send(self.ws, {"type": "msg", "payload": payload})


# Returns the authenticated user (anything but None), or None to reject.
Authenticate = Callable[[Any], Awaitable[Any]]
# Returns the reply for an acknowledged message; None means "no reply".
OnMessage = Callable[[Client, Any], Awaitable[Any]]


class CoreServer:
    """The protocol, minus everything that is optional."""

    def __init__(
        self,
        *,
        authenticate: Authenticate,
        on_message: OnMessage,
        auth_timeout: float = 5.0,
        idle_timeout: float = 60.0,
    ) -> None:
        self.authenticate = authenticate
        self.on_message = on_message
        # How long a fresh socket may take to send `auth`.
        self.auth_timeout = auth_timeout
        # How long an authenticated socket may stay silent. The client pings
        # every 25 s, so 60 s means "missed two pings": the connection is dead.
        self.idle_timeout = idle_timeout
        # Live, authenticated connections — iterate it to push to everyone.
        self.clients: set[Client] = set()
        # Strong references to running workers; asyncio keeps only weak ones.
        self._workers: set[asyncio.Task] = set()

    async def handler(self, ws: ServerConnection) -> None:
        """Owns one socket for its whole life. Pass it to `serve()`.

        Reads frames strictly in order. Everything here is quick — a `ping` in
        particular must never wait behind your handler, or the client, which
        drops a connection 10 s after an unanswered ping, reconnects in the
        middle of a slow request. Messages are therefore only *queued* here
        and handled by a separate worker (`_work`).
        """
        client: Optional[Client] = None
        try:
            while True:
                # Before the handshake the deadline is the auth deadline;
                # after it, the idle deadline.
                timeout = self.idle_timeout if client else self.auth_timeout
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout)
                except asyncio.TimeoutError:
                    if client:
                        await ws.close(CLOSE_IDLE_TIMEOUT, "idle timeout")
                    else:
                        await ws.close(CLOSE_AUTH_TIMEOUT, "auth timeout")
                    return

                try:
                    frame = json.loads(raw)
                except ValueError:
                    # Almost always a codec mismatch; nothing sensible follows.
                    await _error(ws, "bad_request", "malformed frame")
                    await ws.close(CLOSE_PROTOCOL_ERROR, "malformed frame")
                    return
                if not isinstance(frame, dict):
                    await _error(ws, "bad_request", "frame must be a JSON object")
                    continue
                kind = frame.get("type")

                if kind == "auth":
                    if client is None:  # a repeated `auth` is ignored
                        client = await self._handshake(ws, frame)
                        if client is None:
                            return  # rejected, socket closed
                    continue

                if client is None:
                    # The stock client never does this; tell whoever did.
                    await _error(ws, "unauthorized", "not authenticated")
                elif kind == "ping":
                    await _send(ws, {"type": "pong"})
                elif kind == "msg":
                    client.inbox.put_nowait(frame)
                else:
                    # sub, unsub, pub, broadcast — or anything newer. Answering
                    # (instead of ignoring) is what lets the client fail fast.
                    await _refuse(ws, frame.get("id"), "unsupported", "not supported here")
        except ConnectionClosed:
            pass
        finally:
            if client is not None:
                self.clients.discard(client)
                # Let the worker finish what was already read, then stop. A
                # message the server received gets handled even if its sender
                # is gone; only the reply has nowhere to go.
                client.inbox.put_nowait(None)

    async def _handshake(self, ws: ServerConnection, frame: dict) -> Optional[Client]:
        """Authenticates the `auth` frame and answers `hello`.

        Returns None when the socket was closed instead.
        """
        # `payload` is whatever the client's `auth()` option returned — a
        # token, typically. The client runs it again on every reconnect, which
        # is all that token refresh needs.
        try:
            user = await self.authenticate(frame.get("payload"))
        except Exception:
            log.exception("authenticate failed")
            user = None
        if user is None:
            # Terminal: the client stops retrying and reports why. Nothing
            # needs to be sent before the close.
            await ws.close(CLOSE_AUTH_FAILED, "authentication failed")
            return None

        client = Client(ws=ws, user=user)
        self.clients.add(client)
        worker = asyncio.create_task(self._work(client))
        self._workers.add(worker)
        worker.add_done_callback(self._workers.discard)

        # Must go out within 10 s of the `auth` frame, or the client gives up
        # on this socket and reconnects. `clientId` and `namespace` are part
        # of the rooms extension and not needed here.
        await _send(ws, {"type": "hello", "protocol": PROTOCOL_VERSION})
        return client

    async def _work(self, client: Client) -> None:
        """Runs `on_message` for one connection's messages: one at a time, in
        arrival order.

        Sequential per connection because that is what a data channel is
        expected to be: message 2 is not processed before message 1 is done.
        Replies are correlated by `id`, so the protocol does not require it —
        to handle a connection's messages concurrently, replace the `await`
        below with a task per message.
        """
        while (frame := await client.inbox.get()) is not None:
            # Only a non-empty string id asks for an answer. Anything else is
            # fire-and-forget — and could not be correlated anyway.
            frame_id = frame.get("id")
            if not isinstance(frame_id, str) or not frame_id:
                frame_id = None
            try:
                reply = await self.on_message(client, frame.get("payload"))
                if frame_id is None:
                    continue
                ack = {"type": "ack", "id": frame_id}
                if reply is not None:
                    ack["payload"] = reply
                # Encoded here, inside the try: a reply json cannot encode
                # must still produce an answer, or the client waits 30 s.
                wire = json.dumps(ack)
            except Reject as e:
                await _refuse(client.ws, frame_id, e.code, e.message, e.details)
            except Exception:
                log.exception("on_message failed")
                await _refuse(client.ws, frame_id, "internal", "internal error")
            else:
                await _send_raw(client.ws, wire)


# ---------------------------------------------------------------- wire helpers


async def _send(ws: ServerConnection, frame: dict) -> bool:
    """Sends one frame as one JSON text message. False if the socket is gone."""
    return await _send_raw(ws, json.dumps(frame))


async def _send_raw(ws: ServerConnection, wire: str) -> bool:
    try:
        await ws.send(wire)
        return True
    except ConnectionClosed:
        return False


async def _error(ws: ServerConnection, code: str, message: str) -> None:
    """An uncorrelated error: the client emits it as an `error` event."""
    await _send(ws, {"type": "error", "error": {"code": code, "message": message}})


async def _refuse(
    ws: ServerConnection, frame_id: Any, code: str, message: str, details: Any = None
) -> None:
    """Answers a frame the server will not act on: a `nack` when the client is
    waiting for this frame (it carried an id), an `error` when nobody is.

    `details` is the application's — any JSON value, sent only when given. One
    that json cannot encode is answered as `internal` instead, so the client is
    never left waiting for a refusal that could not be sent."""
    error: dict = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    try:
        json.dumps(error)
    except (TypeError, ValueError):
        log.exception("error details could not be encoded")
        error = {"code": "internal", "message": "error could not be encoded"}
    if isinstance(frame_id, str) and frame_id:
        await _send(ws, {"type": "nack", "id": frame_id, "error": error})
    else:
        await _send(ws, {"type": "error", "error": error})


# ----------------------------------------------------------------- example app
#
# Everything below is application code: replace it with yours. The contract
# matches the conformance script in Appendix A.


async def authenticate(payload: Any) -> Any:
    """Checks the client's `auth()` payload. Return None to reject (4001)."""
    if isinstance(payload, dict) and payload.get("token") == "dev-secret":
        return {"name": payload.get("user", "anonymous")}
    return None


async def on_message(client: Client, payload: Any) -> Any:
    """Handles one `ws.send(payload)`. The return value is the reply the
    client receives when it sent with `{ ack: true }`; it is discarded for a
    fire-and-forget send."""
    op = payload.get("op") if isinstance(payload, dict) else None
    if op == "echo":
        return payload
    if op == "push":
        # A reply and a push are different things: the reply answers this
        # message, a push can go out at any time — here, right away.
        await client.push(payload.get("data"))
        return None
    # `details` is for the client's code: here, the op it got wrong.
    raise Reject("unknown_op", f"unknown op: {op!r}", {"op": op})


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    server = CoreServer(
        authenticate=authenticate,
        on_message=on_message,
        auth_timeout=float(os.environ.get("WS_AUTH_TIMEOUT", 5)),
        idle_timeout=float(os.environ.get("WS_IDLE_TIMEOUT", 60)),
    )
    port = int(os.environ.get("WS_PORT", 8765))
    async with serve(server.handler, "127.0.0.1", port) as ws_server:
        print(f"listening on ws://127.0.0.1:{port}/ws", flush=True)
        await ws_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
```

Notes on the design choices, in the order they matter:

- **The read loop only reads.** `ping` is answered inline; `msg` frames are
  queued for a per-connection worker. However slow your handler, the pong goes
  out on time — otherwise the client would drop the connection 10 s into a slow
  request and the reply would be lost (3.3).
- **One worker per connection, messages handled one at a time.** Message 2 is
  not processed before message 1 is done, which is what a data channel is
  expected to do. To handle one connection's messages concurrently, start a task
  per message in `_work` instead of awaiting the handler — replies are
  correlated by `id`, so the protocol allows it.
- **The worker outlives its socket, briefly.** On disconnect it still finishes
  the messages already received — a message the server read gets handled even if
  the reply has nowhere to go — and then stops.
- **`asyncio.wait_for(recv, timeout)` doubles as the auth timer and the idle
  timer.** No sweeper task, no bookkeeping of "last seen".
- **Errors never leak.** A `Reject` is the only way an error text — or
  structured `details` — reaches the client; any other exception is logged and
  answered `internal`. `details` the application hands over but json cannot
  encode are answered `internal` too, rather than leaving the client waiting
  for a `nack` that never went out.
- **No registry.** `server.clients` is the set of live connections — iterate it
  to push to everyone. Keep your own `user → client` map in `authenticate` /
  `on_message` if you need to push to a particular user; a real deployment with
  several instances needs a shared bus for that (Redis pub/sub or similar).

---

## 9. Python: the full reference, with rooms

A complete server — the core plus the rooms extension — on the same library.
The protocol logic is in the `Hub` class and, as above, does not depend on the
framework beyond `recv`, `send` and `close`.

```bash
pip install "websockets>=13"
python ws_server.py                                       # ws://127.0.0.1:8765/ws
WS_AUTH_TIMEOUT=1 WS_IDLE_TIMEOUT=2 python ws_server.py   # timeouts Appendix A expects
```

It was verified against the real client (`@marianmeres/ws` 0.6.0) with the
script in Appendix A and `WS_ROOMS=1`: all 17 checks pass, on `websockets` 17.2
and Python 3.11 with deprecation warnings promoted to errors.

```python
"""
Reference server for the @marianmeres/ws wire protocol, version 2: the core
(messages between client and server) plus the rooms extension (namespaces,
rooms, presence, broadcast).

Need only the core? The core server in section 8 is a third of the size.

The protocol logic lives in `Hub` and does not depend on the framework; the
`websockets` library only supplies the socket. Python 3.10+.

    pip install "websockets>=13"
    python ws_server.py                 # ws://127.0.0.1:8765/ws
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("ws")

PROTOCOL_VERSION = 2
DEFAULT_NAMESPACE = "default"

# Close codes. 4xxx is the application range (RFC 6455). The client treats
# 4001 and 4003 as terminal and reconnects after everything else.
CLOSE_GOING_AWAY = 1001
CLOSE_AUTH_FAILED = 4001
CLOSE_AUTH_TIMEOUT = 4002
CLOSE_FORBIDDEN = 4003
CLOSE_IDLE_TIMEOUT = 4008
CLOSE_RATE_LIMITED = 4009
CLOSE_FRAME_TOO_LARGE = 4013
CLOSE_PROTOCOL_ERROR = 4400


def now_ms() -> int:
    return int(time.time() * 1000)


class Reject(Exception):
    """Raise from `on_message` to refuse a message with your own error code.

    The client's `send(payload, { ack: true })` rejects with a `WSRemoteError`
    carrying exactly this `code`, `message` and `details` — the last optional,
    any JSON value, for the client's code rather than its user. Any other
    exception is logged and reported as `internal`, without its text.
    """

    def __init__(self, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


@dataclass
class AuthResult:
    """What `verify` returns to accept a connection. Every field is optional."""

    client_id: Optional[str] = None  # overrides the id the client proposed
    namespace: Optional[str] = None  # overrides the namespace the client requested
    meta: dict[str, Any] = field(default_factory=dict)  # available to hooks


@dataclass
class Conn:
    ws: ServerConnection
    id: str
    namespace: str = DEFAULT_NAMESPACE
    rooms: dict[str, bool] = field(default_factory=dict)  # room -> wants presence
    meta: dict[str, Any] = field(default_factory=dict)
    authed: bool = False
    window_start: float = 0.0
    window_count: int = 0
    # Messages (`msg` frames) waiting for `on_message`, in arrival order.
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue)

    async def push(self, payload: Any) -> bool:
        """Direct message to this client (core): no room, arrives through the
        client's `message` event. False when the socket is already gone."""
        try:
            await self.ws.send(json.dumps({"type": "msg", "payload": payload}))
            return True
        except ConnectionClosed:
            return False


Verify = Callable[[Any, ServerConnection], Awaitable[Optional[AuthResult]]]
AllowBroadcast = Callable[[Conn, str], Awaitable[bool]]
# Returns the reply for an acknowledged message; None means "no reply".
OnMessage = Callable[[Conn, Any], Awaitable[Any]]


class Hub:
    """Connection registry, direct messages, room index, presence and delivery."""

    def __init__(
        self,
        *,
        verify: Optional[Verify] = None,  # None accepts everyone: development only
        on_message: Optional[OnMessage] = None,  # None refuses every `msg`: unsupported
        allow_broadcast: Optional[AllowBroadcast] = None,  # None denies: the safe default
        auth_timeout: float = 5.0,
        idle_timeout: float = 60.0,
        max_frame_size: int = 256 * 1024,
        max_frames_per_second: int = 100,
    ) -> None:
        self.verify = verify
        self.on_message = on_message
        self.allow_broadcast = allow_broadcast
        self.auth_timeout = auth_timeout
        self.idle_timeout = idle_timeout
        self.max_frame_size = max_frame_size
        self.max_frames_per_second = max_frames_per_second
        self.conns: dict[str, Conn] = {}  # authenticated, by client id
        self.index: dict[str, dict[str, set[str]]] = {}  # room -> namespace -> ids
        self._tasks: set[asyncio.Task] = set()

    # ---------------------------------------------------------- application API

    async def send(self, client_id: str, payload: Any) -> bool:
        """Direct message to one client by id — see `Conn.push`. False when
        that client is not connected."""
        conn = self.conns.get(client_id)
        return await conn.push(payload) if conn is not None else False

    async def publish(
        self,
        room: str,
        payload: Any,
        namespace: str = DEFAULT_NAMESPACE,
        sender: Optional[str] = None,
    ) -> int:
        """Server-side injection. Delivered with `from: null` unless `sender` is set."""
        message = {
            "room": room,
            "namespace": namespace,
            "from": sender,
            "payload": payload,
            "timestamp": now_ms(),
        }
        return await self._deliver(namespace, message)

    async def broadcast(self, room: str, payload: Any, sender: Optional[str] = None) -> int:
        """Server-side injection into `room` in every namespace."""
        message = {
            "room": room,
            "namespace": "*",
            "from": sender,
            "payload": payload,
            "timestamp": now_ms(),
        }
        return await self._deliver(None, message)

    def members(self, room: str, namespace: str = DEFAULT_NAMESPACE) -> list[str]:
        return list(self.index.get(room, {}).get(namespace, ()))

    # -------------------------------------------------------- connection loop

    async def handler(self, ws: ServerConnection) -> None:
        """One task per socket. Frames are handled strictly in arrival order."""
        conn = Conn(ws=ws, id=secrets.token_hex(6))
        try:
            while True:
                # Before auth the deadline is the handshake deadline; after it,
                # the idle deadline (the client pings every 25 s by default).
                timeout = self.idle_timeout if conn.authed else self.auth_timeout
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout)
                except asyncio.TimeoutError:
                    if conn.authed:
                        await ws.close(CLOSE_IDLE_TIMEOUT, "idle timeout")
                    else:
                        await ws.close(CLOSE_AUTH_TIMEOUT, "auth timeout")
                    return

                if len(raw) > self.max_frame_size:
                    await ws.close(CLOSE_FRAME_TOO_LARGE, "frame too large")
                    return
                if self._rate_limited(conn):
                    await ws.close(CLOSE_RATE_LIMITED, "rate limit exceeded")
                    return

                try:
                    frame = json.loads(raw)
                except ValueError:
                    await self._error(conn, "bad_request", "malformed frame")
                    await ws.close(CLOSE_PROTOCOL_ERROR, "malformed frame")
                    return

                if not isinstance(frame, dict) or not isinstance(frame.get("type"), str):
                    await self._error(conn, "bad_request", "missing frame type")
                    continue

                if frame["type"] == "auth":
                    if not await self._on_auth(conn, frame):
                        return
                    continue

                if not conn.authed:
                    await self._error(conn, "unauthorized", "not authenticated")
                    continue

                await self._dispatch(conn, frame)
        except ConnectionClosed:
            pass
        finally:
            await self._drop(conn)

    async def _dispatch(self, conn: Conn, frame: dict) -> None:
        kind = frame["type"]
        if kind == "ping":
            await self._send(conn, {"type": "pong"})
        elif kind == "msg":
            # Queued, not handled inline: a slow `on_message` must not hold up
            # the pong (the client drops a socket 10 s after an unanswered
            # ping) nor the room frames that follow. See `_work`.
            conn.inbox.put_nowait(frame)
        elif kind == "sub":
            await self._on_sub(conn, frame)
        elif kind == "unsub":
            await self._on_unsub(conn, frame)
        elif kind == "pub":
            await self._on_pub(conn, frame)
        elif kind == "broadcast":
            await self._on_broadcast(conn, frame)
        else:
            # Possibly a valid frame from a newer protocol. Answering it lets
            # the client fail fast instead of waiting out its send timeout.
            await self._refuse(conn, frame.get("id"), "unsupported", "unsupported frame type")

    # ------------------------------------------------------------- handshake

    async def _on_auth(self, conn: Conn, frame: dict) -> bool:
        """Returns False when the socket was closed."""
        if conn.authed:
            return True  # a repeated handshake is ignored

        result: Optional[AuthResult] = AuthResult()
        if self.verify is not None:
            try:
                result = await self.verify(frame.get("payload"), conn.ws)
            except Exception:
                result = None
        if result is None:
            await conn.ws.close(CLOSE_AUTH_FAILED, "authentication failed")
            return False

        proposed = frame.get("clientId")
        requested = frame.get("namespace")
        client_id = result.client_id or (
            proposed if isinstance(proposed, str) and proposed else conn.id
        )
        namespace = result.namespace or (
            requested if isinstance(requested, str) and requested else DEFAULT_NAMESPACE
        )

        # Same id already connected: the newcomer wins and the stale socket goes.
        # This is what lets a reconnect after a half-open drop recover instead
        # of leaving ghosts behind.
        existing = self.conns.get(client_id)
        if existing is not None and existing is not conn:
            await self._close(existing, CLOSE_GOING_AWAY, "replaced by new connection")

        conn.id = client_id
        conn.namespace = namespace
        conn.meta = result.meta
        conn.authed = True
        self.conns[client_id] = conn
        worker = asyncio.get_running_loop().create_task(self._work(conn))
        self._tasks.add(worker)
        worker.add_done_callback(self._tasks.discard)

        await self._send(
            conn,
            {
                "type": "hello",
                "clientId": client_id,
                "namespace": namespace,
                "protocol": PROTOCOL_VERSION,
            },
        )
        return True

    # --------------------------------------------------------------- messages

    async def _work(self, conn: Conn) -> None:
        """Runs `on_message` for one connection's messages: one at a time, in
        arrival order. Ends when `_drop` enqueues None, after finishing what
        was already received."""
        while (frame := await conn.inbox.get()) is not None:
            frame_id = frame.get("id")
            if not isinstance(frame_id, str) or not frame_id:
                frame_id = None  # fire-and-forget: nobody is waiting for an answer
            if self.on_message is None:
                await self._refuse(conn, frame_id, "unsupported", "this server accepts no messages")
                continue
            try:
                reply = await self.on_message(conn, frame.get("payload"))
                if frame_id is None:
                    continue
                ack = {"type": "ack", "id": frame_id}
                if reply is not None:
                    ack["payload"] = reply
                wire = json.dumps(ack)  # inside the try: an unencodable reply is `internal`
            except Reject as e:
                await self._refuse(conn, frame_id, e.code, e.message, e.details)
            except Exception:
                log.exception("on_message failed")
                await self._refuse(conn, frame_id, "internal", "internal error")
            else:
                try:
                    await conn.ws.send(wire)
                except ConnectionClosed:
                    pass

    # ----------------------------------------------------------------- rooms

    async def _on_sub(self, conn: Conn, frame: dict) -> None:
        rooms = frame.get("rooms")
        if not isinstance(rooms, list):
            await self._nack(conn, frame.get("id"), "bad_request", "rooms must be an array")
            return
        sync_rooms: list[str] = []
        for req in rooms:
            room = req.get("room") if isinstance(req, dict) else None
            if not isinstance(room, str) or not room:
                continue
            is_new = room not in conn.rooms
            wants_presence = bool(req.get("presence"))
            conn.rooms[room] = wants_presence
            self.index.setdefault(room, {}).setdefault(conn.namespace, set()).add(conn.id)
            # Tell the others first, so everyone agrees on the membership by
            # the time the joiner receives its snapshot.
            if is_new:
                await self._notify_presence(room, conn.namespace, "join", conn.id)
            if wants_presence:
                sync_rooms.append(room)

        # The snapshot goes out before the ack, so presence is settled by the
        # time the client's subscribe() resolves.
        for room in sync_rooms:
            await self._send(
                conn,
                {
                    "type": "presence",
                    "event": "sync",
                    "room": room,
                    "namespace": conn.namespace,
                    "clientId": None,
                    "members": self.members(room, conn.namespace),
                    "timestamp": now_ms(),
                },
            )
        await self._send(conn, {"type": "ack", "id": frame.get("id")})

    async def _on_unsub(self, conn: Conn, frame: dict) -> None:
        rooms = frame.get("rooms")
        if not isinstance(rooms, list):
            await self._nack(conn, frame.get("id"), "bad_request", "rooms must be an array")
            return
        for room in rooms:
            if not isinstance(room, str) or conn.rooms.pop(room, None) is None:
                continue
            self._index_remove(room, conn.namespace, conn.id)
            await self._notify_presence(room, conn.namespace, "leave", conn.id)
        await self._send(conn, {"type": "ack", "id": frame.get("id")})

    # ------------------------------------------------------------- publishing

    async def _on_pub(self, conn: Conn, frame: dict) -> None:
        room = frame.get("room")
        if not isinstance(room, str) or not room:
            await self._nack(conn, frame.get("id"), "bad_request", "missing room")
            return
        # A client may only publish into its own namespace.
        requested = frame.get("namespace")
        if requested and requested != conn.namespace:
            await self._nack(
                conn,
                frame.get("id"),
                "forbidden",
                f'cannot publish into namespace "{requested}"',
            )
            return
        message = {
            "room": room,
            "namespace": conn.namespace,
            "from": conn.id,
            "payload": frame.get("payload"),
            "timestamp": now_ms(),
        }
        recipients = await self._deliver(conn.namespace, message)
        await self._send(conn, {"type": "ack", "id": frame.get("id"), "recipients": recipients})

    async def _on_broadcast(self, conn: Conn, frame: dict) -> None:
        room = frame.get("room")
        if not isinstance(room, str) or not room:
            await self._nack(conn, frame.get("id"), "bad_request", "missing room")
            return
        allowed = False
        if self.allow_broadcast is not None:
            try:
                allowed = bool(await self.allow_broadcast(conn, room))
            except Exception:
                allowed = False
        if not allowed:
            await self._nack(conn, frame.get("id"), "forbidden", "broadcast not permitted")
            return
        message = {
            "room": room,
            "namespace": "*",
            "from": conn.id,
            "payload": frame.get("payload"),
            "timestamp": now_ms(),
        }
        recipients = await self._deliver(None, message)
        await self._send(conn, {"type": "ack", "id": frame.get("id"), "recipients": recipients})

    async def _deliver(self, namespace: Optional[str], message: dict) -> int:
        """Delivers to every subscriber of the room. `None` crosses all namespaces."""
        by_ns = self.index.get(message["room"])
        if not by_ns:
            return 0
        if namespace is None:
            targets = list(by_ns.items())
        else:
            targets = [(namespace, by_ns.get(namespace, set()))]
        count = 0
        for ns, ids in targets:
            for cid in list(ids):
                conn = self.conns.get(cid)
                if conn is None:
                    continue
                # Receivers always see the namespace they live in, even for a
                # broadcast that originated elsewhere.
                await self._send(conn, {"type": "msg", **message, "namespace": ns})
                count += 1
        return count

    # --------------------------------------------------------------- presence

    async def _notify_presence(
        self, room: str, namespace: str, event: str, subject_id: str
    ) -> None:
        ids = self.index.get(room, {}).get(namespace)
        if not ids:
            return
        members = list(ids)
        ts = now_ms()
        for cid in list(ids):
            if cid == subject_id:
                continue  # the subject gets a full sync, not a delta about itself
            conn = self.conns.get(cid)
            if conn is None or not conn.rooms.get(room):
                continue  # only subscribers who asked for presence are told
            await self._send(
                conn,
                {
                    "type": "presence",
                    "event": event,
                    "room": room,
                    "namespace": namespace,
                    "clientId": subject_id,
                    "members": members,
                    "timestamp": ts,
                },
            )

    # ---------------------------------------------------------------- plumbing

    async def _drop(self, conn: Conn) -> None:
        """Idempotent teardown: unregister first, then tell presence subscribers."""
        # Only remove the registry entry if it still points at this socket:
        # a replaced connection must not evict its replacement.
        if self.conns.get(conn.id) is conn:
            del self.conns[conn.id]
        # Stops the message worker once it has handled what was received.
        # Harmless when repeated, or when no worker was ever started.
        conn.inbox.put_nowait(None)
        rooms = list(conn.rooms)
        conn.rooms.clear()
        for room in rooms:
            self._index_remove(room, conn.namespace, conn.id)
        for room in rooms:
            await self._notify_presence(room, conn.namespace, "leave", conn.id)

    async def _close(self, conn: Conn, code: int, reason: str) -> None:
        # Never await the closing handshake here: a half-open peer would stall
        # us for the whole close timeout, and this runs inside *another*
        # connection's handshake.
        task = asyncio.get_running_loop().create_task(conn.ws.close(code, reason))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        await self._drop(conn)

    def _index_remove(self, room: str, namespace: str, client_id: str) -> None:
        by_ns = self.index.get(room)
        if not by_ns:
            return
        ids = by_ns.get(namespace)
        if not ids:
            return
        ids.discard(client_id)
        if not ids:
            del by_ns[namespace]
        if not by_ns:
            del self.index[room]

    def _rate_limited(self, conn: Conn) -> bool:
        if self.max_frames_per_second <= 0:
            return False
        now = time.monotonic()
        if now - conn.window_start >= 1.0:
            conn.window_start, conn.window_count = now, 0
        conn.window_count += 1
        return conn.window_count > self.max_frames_per_second

    async def _send(self, conn: Conn, frame: dict) -> None:
        try:
            await conn.ws.send(json.dumps(frame))
        except ConnectionClosed:
            pass

    async def _error(self, conn: Conn, code: str, message: str) -> None:
        await self._send(conn, {"type": "error", "error": {"code": code, "message": message}})

    async def _nack(self, conn: Conn, id_: Any, code: str, message: str) -> None:
        await self._send(
            conn, {"type": "nack", "id": id_, "error": {"code": code, "message": message}}
        )

    async def _refuse(
        self, conn: Conn, id_: Any, code: str, message: str, details: Any = None
    ) -> None:
        """`nack` when the frame carried a usable id (someone is waiting),
        an uncorrelated `error` when it did not.

        `details` is the application's — any JSON value, sent only when given.
        One that json cannot encode is answered as `internal` instead, so the
        client is never left waiting for a refusal that could not be sent."""
        error: dict = {"code": code, "message": message}
        if details is not None:
            error["details"] = details
        try:
            json.dumps(error)
        except (TypeError, ValueError):
            log.exception("error details could not be encoded")
            error = {"code": "internal", "message": "error could not be encoded"}
        if isinstance(id_, str) and id_:
            await self._send(conn, {"type": "nack", "id": id_, "error": error})
        else:
            await self._send(conn, {"type": "error", "error": error})


# ------------------------------------------------------------------ example app


async def verify(payload: Any, ws: ServerConnection) -> Optional[AuthResult]:
    """`payload` is whatever the client's `auth()` option returned.

    Returning None closes the socket with 4001, which the client treats as
    terminal: it stops reconnecting. Here the client's proposed id and
    namespace are honoured; in production derive both from the verified
    identity (e.g. `client_id=f"{user.id}#{tab}"`, `namespace=user.org_id`) so
    a client cannot pick another user's id or namespace.
    """
    if not isinstance(payload, dict) or payload.get("token") != "dev-secret":
        return None
    return AuthResult(meta={"user": payload.get("user")})


async def on_message(conn: Conn, payload: Any) -> Any:
    """Handles one `ws.send(payload)`. The return value is the reply for a send
    with `{ ack: true }`. Same contract as the core server's example."""
    op = payload.get("op") if isinstance(payload, dict) else None
    if op == "echo":
        return payload
    if op == "push":
        await conn.push(payload.get("data"))
        return None
    # `details` is for the client's code: here, the op it got wrong.
    raise Reject("unknown_op", f"unknown op: {op!r}", {"op": op})


async def allow_broadcast(conn: Conn, room: str) -> bool:
    """Broadcast crosses namespaces, so it is denied unless you say otherwise."""
    return room == "announcements"


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    hub = Hub(
        verify=verify,
        on_message=on_message,
        allow_broadcast=allow_broadcast,
        auth_timeout=float(os.environ.get("WS_AUTH_TIMEOUT", 5)),
        idle_timeout=float(os.environ.get("WS_IDLE_TIMEOUT", 60)),
    )
    port = int(os.environ.get("WS_PORT", 8765))
    async with serve(hub.handler, "127.0.0.1", port) as server:
        print(f"listening on ws://127.0.0.1:{port}/ws", flush=True)
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
```

Notes on the design choices, in the order they matter:

- **One `recv` loop per connection, every rooms frame fully handled before the
  next is read.** This is what guarantees rule 12 (a `sub` is registered before
  the `pub` that follows it) with no extra machinery.
- **`msg` frames are the exception** — queued for a per-connection worker, as in
  the core server, so that a slow `on_message` delays neither pongs nor room
  traffic. Messages stay ordered among themselves; their order relative to room
  frames is not preserved, and nothing depends on it.
- **`asyncio.wait_for(recv, timeout)` doubles as the auth timer and the idle
  timer.** No sweeper task, no bookkeeping of "last seen".
- **`_close` never awaits the closing handshake.** It runs inside _another_
  connection's handshake when evicting a duplicate id, and a half-open peer
  would stall it for the whole close timeout — past the 10 s the newcomer gives
  us for `hello`.
- **`_drop` is idempotent** and unregisters synchronously before its first
  `await`, because the evicting handler and the evicted connection's own
  `finally` can both call it.
- **Sends are awaited inline**, so a slow reader can stall the publisher that is
  fanning out to it. Good enough for a reference; for production put an outgoing
  queue with a writer task per connection, keeping per-connection order.
- **Single instance, in memory.** Horizontal scaling needs a fan-out between
  instances (Redis pub/sub or similar) and a shared view of presence. Local
  delivery stays each instance's own job, which is why `recipients` is
  instance-local.

---

## 10. Optional HTTP surface

The client uses none of this. The reference server mounts, next to the upgrade
route, a small HTTP API for server-side injection and operations; replicate it
only if something else in your system needs it.

| Method | Path                          | Behaviour                                                                            |
| ------ | ----------------------------- | ------------------------------------------------------------------------------------ |
| GET    | `/`                           | The upgrade. Without an `Upgrade: websocket` header: **426**                         |
| GET    | `/stats`                      | Counts of connections, pending handshakes, rooms, per namespace — authenticated only |
| POST   | `/publish/{namespace}/{room}` | JSON body becomes `payload`, delivered with `from: null`                             |
| POST   | `/broadcast/{room}`           | Same, across all namespaces                                                          |

The POST routes respond `{ "ok": true, "recipients": n }`, and answer a body
that is not valid JSON with **400**.

`/stats` and both POST routes must sit behind an HTTP-level authentication of
your own, which is why the reference mounts none of them without one. An
unauthenticated "push anything into any room" endpoint is a vulnerability; and
`/stats` reports counts per namespace, so in a multi-tenant deployment an
unauthenticated read enumerates the tenants that are online.

The upgrade route is the exception — it is always mounted, since authentication
happens in the `auth` frame. It does take one optional check: an origin
allow-list (the reference calls it `allowedOrigins`) answering a disallowed
`Origin` with **403** before upgrading, so the handshake is never reached. Worth
having whenever authentication trusts cookies, because a browser attaches those
to a cross-site socket too. Keep it opt-in: only browsers send `Origin` at all,
so a default allow-list would reject every non-browser client, and a _missing_
header should stay allowed unless you knowingly demand one.

---

## Appendix A — conformance script

Drives the real client against your server. Requires [Deno](https://deno.com)
and network access to fetch the client from JSR; nothing to install.

It expects the server to accept the auth payload `{ "token": "dev-secret" }`
and reject other tokens with 4001, and to answer messages the way both Python
examples do: `{ "op": "echo" }` replies with the payload, `{ "op": "push",
"data": … }` pushes `data` back as a direct message, anything else is refused
with code `unknown_op` and `details: { "op": … }`. Two checks need short server
timeouts (auth 1 s, idle 2 s); the Python examples read them from
`WS_AUTH_TIMEOUT` and `WS_IDLE_TIMEOUT`.

Core checks always run. With `WS_ROOMS=1` the rooms checks run as well — they
also expect broadcast to be allowed into `announcements` only, the policy in the
section 9 example. Without it the script instead checks that rooms are refused
as `unsupported`.

```bash
WS_URL=ws://127.0.0.1:8765/ws deno run -A conformance.ts              # core server
WS_URL=ws://127.0.0.1:8765/ws WS_ROOMS=1 deno run -A conformance.ts   # with rooms
```

```typescript
/**
 * Conformance check: drives the real @marianmeres/ws client against whatever
 * server listens at WS_URL.
 *
 * Core checks always run. Rooms checks run with WS_ROOMS=1; without it, the
 * script instead checks that rooms are refused as "unsupported".
 *
 * The server must accept the auth payload { token: "dev-secret" } and reject
 * any other with 4001, and answer messages like the Python examples do:
 * { op: "echo" } replies with the payload, { op: "push", data } pushes `data`
 * back as a direct message, anything else is refused with code "unknown_op"
 * and details { op }. With rooms, broadcast must be allowed into
 * "announcements" only.
 *
 * Two checks need short server timeouts: WS_AUTH_TIMEOUT=1 WS_IDLE_TIMEOUT=2.
 */
import {
	createWSClient,
	type WSMessage,
	type WSPresenceEvent,
	WSRemoteError,
	type WSRoomMessage,
	WSTerminatedError,
} from "jsr:@marianmeres/ws@^0.6.0";
import { assert, assertEquals, assertRejects } from "jsr:@std/assert@^1";

const URL = Deno.env.get("WS_URL") ?? "ws://127.0.0.1:8765/ws";
const ROOMS = Deno.env.get("WS_ROOMS") === "1";
const auth = () => ({ token: "dev-secret", user: "smoke" });
const client = (o: Record<string, unknown> = {}) =>
	createWSClient({ url: URL, logger: null, pingInterval: 0, auth, ...o });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until(fn: () => boolean, label: string, ms = 4000) {
	const t0 = Date.now();
	while (!fn()) {
		if (Date.now() - t0 > ms) throw new Error(`timeout: ${label}`);
		await sleep(20);
	}
}
let passed = 0, failed = 0;
async function test(name: string, fn: () => Promise<void>) {
	try {
		await fn();
		console.log("ok   ", name);
		passed++;
	} catch (e) {
		console.log("FAIL ", name, "\n     ", e);
		failed++;
	}
}

/** A socket without the client library, for frames the client never sends. */
async function raw() {
	const ws = new WebSocket(URL);
	const frames: Record<string, unknown>[] = [];
	const closed = new Promise<CloseEvent>((r) => ws.onclose = r);
	ws.onmessage = (e) => frames.push(JSON.parse(e.data));
	await new Promise((r) => ws.onopen = r);
	return { ws, frames, closed, send: (f: unknown) => ws.send(JSON.stringify(f)) };
}

// ------------------------------------------------------------------ core

await test("core: pre-auth frame -> error unauthorized; hello; bad json -> 4400", async () => {
	const r = await raw();
	r.send({ type: "ping" });
	await until(() => r.frames.length === 1, "error frame");
	assertEquals(r.frames[0].type, "error");
	assertEquals((r.frames[0].error as Record<string, unknown>).code, "unauthorized");
	r.send({ type: "auth", protocol: 2, payload: auth() });
	await until(() => r.frames.length === 2, "hello");
	assertEquals(r.frames[1].type, "hello");
	assertEquals(r.frames[1].protocol, 2);
	r.send({ type: "ping" });
	await until(() => r.frames.length === 3, "pong");
	assertEquals(r.frames[2], { type: "pong" });
	r.ws.send("not json");
	const ev = await r.closed;
	assertEquals(ev.code, 4400);
	assertEquals(r.frames[3].type, "error");
});

await test("core: send with ack resolves with the server's reply", async () => {
	const c = client();
	try {
		const reply = await c.send({ op: "echo", n: 1 }, { ack: true });
		assertEquals(reply, { op: "echo", n: 1 });
	} finally {
		c.dispose();
	}
});

await test("core: fire-and-forget send; the push arrives as a direct message", async () => {
	const c = client();
	try {
		const got: WSMessage[] = [];
		c.on("message", (m) => got.push(m));
		assertEquals(await c.send({ op: "push", data: { hello: "there" } }), undefined);
		await until(() => got.length === 1, "pushed message");
		assertEquals(got[0].payload, { hello: "there" });
		assertEquals(got[0].room, undefined, "a direct message has no room");
	} finally {
		c.dispose();
	}
});

await test("core: a refused message rejects with the server's code; socket stays", async () => {
	const c = client();
	try {
		const err = await assertRejects(
			() => c.send({ op: "nope" }, { ack: true }),
			WSRemoteError,
		);
		assertEquals(err.code, "unknown_op");
		assert(c.connected);
		assertEquals(await c.send({ op: "echo" }, { ack: true }), { op: "echo" });
	} finally {
		c.dispose();
	}
});

await test("core: a refusal's details reach the application, untouched", async () => {
	const c = client();
	try {
		const err = await assertRejects(
			() => c.send({ op: "nope" }, { ack: true }),
			WSRemoteError,
		);
		assertEquals(err.code, "unknown_op");
		assertEquals(err.details, { op: "nope" });
	} finally {
		c.dispose();
	}
});

await test("core: unknown frame -> nack unsupported with an id, error without", async () => {
	const r = await raw();
	r.send({ type: "auth", protocol: 2, payload: auth() });
	await until(() => r.frames.some((f) => f.type === "hello"), "hello");
	r.send({ type: "teleport", id: "t1" });
	await until(() => r.frames.some((f) => f.type === "nack"), "nack");
	const nack = r.frames.find((f) => f.type === "nack")!;
	assertEquals(nack.id, "t1");
	assertEquals((nack.error as Record<string, unknown>).code, "unsupported");
	r.send({ type: "teleport" });
	await until(() => r.frames.some((f) => f.type === "error"), "error");
	const error = r.frames.find((f) => f.type === "error")!;
	assertEquals((error.error as Record<string, unknown>).code, "unsupported");
	r.ws.close();
	await r.closed;
});

await test("core: heartbeat — ping is answered, connection stays up", async () => {
	const hb = client({ pingInterval: 200, pongTimeout: 400 });
	let closed = false;
	hb.on("close", () => closed = true);
	try {
		await hb.connect();
		await sleep(1500);
		assert(!closed && hb.connected);
	} finally {
		hb.dispose();
	}
});

await test("core: auth failure closes with 4001 and the client gives up", async () => {
	const bad = client({ auth: () => ({ token: "wrong" }) });
	try {
		const err = await assertRejects(() => bad.connect(), WSTerminatedError);
		assertEquals(err.code, 4001);
		assertEquals(bad.connectionState, "terminated");
	} finally {
		bad.dispose();
	}
});

await test("core: no auth frame -> closed 4002 (needs WS_AUTH_TIMEOUT=1)", async () => {
	const r = await raw();
	const ev = await r.closed;
	assertEquals(ev.code, 4002);
});

await test("core: idle reap 4008, then a send buffered meanwhile is flushed (needs WS_IDLE_TIMEOUT=2)", async () => {
	const c = client();
	try {
		await c.connect();
		const ev = await new Promise<{ code: number; willReconnect: boolean }>((r) =>
			c.on("close", r)
		);
		assertEquals(ev.code, 4008);
		assertEquals(ev.willReconnect, true);
		// Buffered while reconnecting, flushed once the new socket says hello.
		const reply = await c.send({ op: "echo", late: true }, { ack: true });
		assertEquals(reply, { op: "echo", late: true });
	} finally {
		c.dispose();
	}
});

if (!ROOMS) {
	await test("core: rooms are refused as unsupported, fast", async () => {
		const c = client({ sendTimeout: 20_000 });
		try {
			await c.connect();
			const t0 = Date.now();
			const err = await assertRejects(
				() => c.subscribe("r", () => {}),
				WSRemoteError,
			);
			assertEquals(err.code, "unsupported");
			assert(Date.now() - t0 < 2_000, "answered, not timed out");
		} finally {
			c.dispose();
		}
	});
}

// ----------------------------------------------------------------- rooms

if (ROOMS) {
	await test("rooms: hello carries clientId and namespace", async () => {
		const a = client({ clientId: "alice" });
		const b = client({ namespace: "org-2" });
		try {
			await a.connect();
			await b.connect();
			assertEquals(a.clientId, "alice");
			assertEquals(a.namespace, "default");
			assert(
				typeof b.clientId === "string" && b.clientId.length > 0,
				"generated id",
			);
			assertEquals(b.namespace, "org-2");
		} finally {
			a.dispose();
			b.dispose();
		}
	});

	await test("rooms: presence sync/join/leave, publish echo + recipients", async () => {
		const alice = client({ clientId: "alice" });
		const bob = client({ clientId: "bob" });
		try {
			await alice.connect();
			const aliceEv: WSPresenceEvent[] = [];
			const aliceMsgs: WSRoomMessage[] = [];
			await alice.subscribe("room", (m) => aliceMsgs.push(m), {
				presence: (e) => aliceEv.push(e),
			});
			await until(() => aliceEv.length === 1, "alice sync");
			assertEquals(aliceEv[0].event, "sync");
			assertEquals(aliceEv[0].clientId, null);
			assertEquals(aliceEv[0].members, ["alice"]);

			await bob.connect();
			const bobEv: WSPresenceEvent[] = [];
			const bobMsgs: WSRoomMessage[] = [];
			await bob.subscribe("room", (m) => bobMsgs.push(m), {
				presence: (e) => bobEv.push(e),
			});
			await until(() => bobEv.length === 1, "bob sync");
			assertEquals(bobEv[0].members.sort(), ["alice", "bob"]);
			await until(() => aliceEv.length === 2, "alice sees join");
			assertEquals(aliceEv[1].event, "join");
			assertEquals(aliceEv[1].clientId, "bob");

			const { recipients } = await bob.publish("room", { text: "hi" });
			assertEquals(recipients, 2, "sender is echoed too");
			await until(() => aliceMsgs.length === 1 && bobMsgs.length === 1, "delivery");
			assertEquals(aliceMsgs[0].payload, { text: "hi" });
			assertEquals(aliceMsgs[0].room, "room");
			assertEquals(aliceMsgs[0].namespace, "default");
			assertEquals(aliceMsgs[0].from, "bob");
			assert(
				Number.isInteger(aliceMsgs[0].timestamp) && aliceMsgs[0].timestamp > 1e12,
			);

			// presence upgrade: re-sub with presence on an already joined room
			const pEv: WSPresenceEvent[] = [];
			await alice.subscribe("plain", () => {});
			await alice.subscribe("plain", () => {}, { presence: (e) => pEv.push(e) });
			await until(() => pEv.length === 1, "sync after presence upgrade");
			assertEquals(pEv[0].members, ["alice"]);

			bob.dispose();
			await until(() => aliceEv.length === 3, "alice sees leave");
			assertEquals(aliceEv[2].event, "leave");
			assertEquals(aliceEv[2].clientId, "bob");
			assertEquals(aliceEv[2].members, ["alice"]);
		} finally {
			alice.dispose();
			bob.dispose();
		}
	});

	await test("rooms: explicit unsub sends leave and stops delivery", async () => {
		const a = client({ clientId: "alice" });
		const b = client({ clientId: "bob" });
		try {
			await a.connect();
			await b.connect();
			const bEv: WSPresenceEvent[] = [];
			await b.subscribe("u", () => {}, { presence: (e) => bEv.push(e) });
			const aMsgs: WSRoomMessage[] = [];
			await a.subscribe("u", (m) => aMsgs.push(m));
			await until(() => bEv.length === 2, "bob sees alice join");
			await a.unsubscribe("u");
			await until(() => bEv.length === 3, "bob sees alice leave");
			assertEquals(bEv[2].event, "leave");
			assertEquals(bEv[2].members, ["bob"]);
			const { recipients } = await b.publish("u", 1);
			assertEquals(recipients, 1);
			await sleep(200);
			assertEquals(aMsgs.length, 0);
		} finally {
			a.dispose();
			b.dispose();
		}
	});

	await test("rooms: namespaces isolate; foreign namespace publish is forbidden", async () => {
		const a = client({ clientId: "alice" });
		const c = client({ clientId: "carol", namespace: "org-2" });
		try {
			await a.connect();
			await c.connect();
			const cMsgs: WSRoomMessage[] = [];
			await c.subscribe("room", (m) => cMsgs.push(m));
			await a.subscribe("room", () => {});
			const { recipients } = await a.publish("room", { x: 1 });
			assertEquals(recipients, 1);
			await sleep(200);
			assertEquals(cMsgs.length, 0);
			const err = await assertRejects(
				() => a.publish("room", 1, "org-2"),
				WSRemoteError,
			);
			assertEquals(err.code, "forbidden");
		} finally {
			a.dispose();
			c.dispose();
		}
	});

	await test("rooms: broadcast denied by default, crosses namespaces when allowed", async () => {
		const a = client({ clientId: "alice" });
		const c = client({ clientId: "carol", namespace: "org-2" });
		try {
			await a.connect();
			await c.connect();
			const err = await assertRejects(() => a.broadcast("room", 1), WSRemoteError);
			assertEquals(err.code, "forbidden");

			const cMsgs: WSRoomMessage[] = [];
			const aMsgs: WSRoomMessage[] = [];
			await c.subscribe("announcements", (m) => cMsgs.push(m));
			await a.subscribe("announcements", (m) => aMsgs.push(m));
			const { recipients } = await a.broadcast("announcements", { text: "all" });
			assertEquals(recipients, 2);
			await until(
				() => cMsgs.length === 1 && aMsgs.length === 1,
				"broadcast delivery",
			);
			assertEquals(cMsgs[0].namespace, "org-2", "receiver sees its own namespace");
			assertEquals(aMsgs[0].namespace, "default");
			assertEquals(cMsgs[0].from, "alice");
		} finally {
			a.dispose();
			c.dispose();
		}
	});

	await test("rooms: same clientId again — newcomer wins, old socket closed 1001", async () => {
		const d1 = client({ clientId: "dave" });
		const d2 = client({ clientId: "dave" });
		try {
			await d1.connect();
			const closeEv = new Promise<{ code: number; willReconnect: boolean }>((r) =>
				d1.on("close", r)
			);
			await d2.connect();
			const ev = await closeEv;
			d1.dispose();
			assertEquals(ev.code, 1001);
			assertEquals(ev.willReconnect, true);
			const { recipients } = await d2.publish("x", 1);
			assertEquals(recipients, 0);
		} finally {
			d1.dispose();
			d2.dispose();
		}
	});

	await test("rooms: after a reconnect, re-subscribe lands before the flush (needs WS_IDLE_TIMEOUT=2)", async () => {
		const e = client({ clientId: "eve" });
		try {
			await e.connect();
			const msgs: WSRoomMessage[] = [];
			await e.subscribe("r", (m) => msgs.push(m));
			const ev = await new Promise<{ code: number }>((r) => e.on("close", r));
			assertEquals(ev.code, 4008);
			// buffered while reconnecting; must land in the re-subscribed room
			const { recipients } = await e.publish("r", { late: true });
			assertEquals(recipients, 1);
			await until(() => msgs.length === 1, "buffered publish delivered");
			assertEquals(msgs[0].payload, { late: true });
		} finally {
			e.dispose();
		}
	});
}

console.log(`\n${passed} passed, ${failed} failed`);
Deno.exit(failed ? 1 : 0);
```

---

## Appendix B — checklist

### Core

Handshake

- [ ] `auth` is accepted as the first frame; `hello` with `protocol: 2` goes out
      well within 10 s
- [ ] Rejected credentials close with 4001; "not allowed" closes with 4003
- [ ] No `auth` within your deadline closes with 4002
- [ ] Non-`auth` frames before authentication get `error` `unauthorized`
- [ ] A repeated `auth` is ignored

Messages

- [ ] A `msg` without `id` is handled and answered with nothing
- [ ] A `msg` with `id` is answered with exactly one `ack` or `nack` echoing it;
      the `ack` carries the reply in `payload`, or no `payload` at all
- [ ] A refusal is a `nack` with a meaningful `code`; a crash is `internal`,
      without internals in the message, and the socket stays open
- [ ] `error.details`, when the application supplies any, is passed through
      unchanged — and omitted, not `null`, when it supplies none
- [ ] A direct message to the client is `{"type": "msg", "payload": …}`, with no
      `room`
- [ ] Unknown frame types get `nack` `unsupported` with an `id`, `error`
      `unsupported` without; the socket stays open

Liveness and closing

- [ ] `ping` → `pong`, promptly, even while a message handler is running
- [ ] Silent connections are reaped with 4008
- [ ] Shutdown closes with 1001/1000 so clients come back

### Rooms extension

Identity

- [ ] `hello` carries `clientId` and `namespace`
- [ ] Server-assigned id/namespace override the client's proposal; otherwise the
      proposal is honoured; otherwise generated / `"default"`
- [ ] A duplicate client id evicts the older connection with 1001 and does not
      lose the newer registration when the old socket's close fires

Rooms and messages

- [ ] Rooms frames of one connection are handled in arrival order
- [ ] `sub` registers rooms, re-subscribing is harmless, and it is always
      acknowledged with `ack {id}`
- [ ] `unsub` is acknowledged; unknown rooms are ignored
- [ ] `pub` delivers `msg` to every subscriber of `(room, namespace)`, the
      publisher included, and acks with `recipients`
- [ ] A delivered `msg` carries `room`, `namespace`, `from`, `payload`
      (untouched), `timestamp` (integer epoch ms)
- [ ] `pub` with a foreign `namespace` is nacked `forbidden`
- [ ] `pub` without a `room` and `sub`/`unsub` with a non-array `rooms` are
      answered `nack` `bad_request` without closing the socket
- [ ] `broadcast` is nacked `forbidden` unless allowed; when allowed, every
      receiver sees its own namespace

Presence

- [ ] `sub` with `presence: true` yields a `sync` (before the `ack`) with the
      full membership
- [ ] Other presence subscribers get `join` on a new member and `leave` on
      `unsub` and on disconnect, with `members` reflecting the state after the
      event
- [ ] Subscribers without presence are counted in `members` but receive no
      presence frames
- [ ] Closing a connection for any reason removes it from all rooms and emits
      `leave` events
