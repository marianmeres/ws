<!--
GENERATED ANALYSIS — @marianmeres/ws (reference server)
Produced 2026-09-04 by an inline single-agent review: every source, test and doc file read;
each bug reproduced with a script against the real server; every file:line re-opened before
writing. Claims verified against the codebase at commit db21ff3. Planning artifact; no code
was changed.
-->

# Server — input hardening, trust boundaries, fan-out

> The reference server in `src/server/` is small, readable and mostly right. Its one
> critical defect is that it **trusts the shape of authenticated frames**: a `sub` whose
> `rooms` is a number throws inside a `void`ed async handler and takes the whole Deno
> process down. That is a remote denial of service by any connected client, and by anyone
> at all when `verify` is not configured.
>
> The rest is about trust boundaries the docs promise but the code does not enforce.
> Namespace isolation depends on `verify` returning a namespace, yet the hook cannot see
> what the client asked for. An unguarded `/stats` lists every connected tenant by name
> while claiming to expose "counts only". Nothing checks `Origin`, although `verify` is
> handed the request precisely so cookies can be used. Each of these is a small, surgical
> change plus a documentation correction.
>
> **Headline recommendation:** do #1 first and alone — it is the only item that can be
> exploited today by sending one frame — then #2 and #3, which both live in `#onAuth`.

## Summary of recommendations

| # | Recommendation                                                              | Value | Effort | Risk |
| - | --------------------------------------------------------------------------- | ----- | ------ | ---- |
| 1 | Validate frame shapes; never let a handler throw escape the socket callback | high  | S      | low  |
| 2 | Hand `verify` the client's requested identity; document the isolation rule  | high  | S      | low  |
| 3 | One handshake per socket; no ghost registration after a mid-verify close    | med   | S      | low  |
| 4 | Opt-in `allowedOrigins` check on the upgrade                                | med   | S      | low  |
| 5 | Mount `/stats` only behind `httpAuth`; answer bad JSON with 400             | med   | S      | low  |
| 6 | Encode a fan-out frame once per namespace, not once per socket              | med   | S      | low  |

> **Cut from the draft:** per-IP connection caps (out of scope for a reference server; the
> auth timeout already bounds pending sockets), `maxFrameSize` counting UTF-16 units rather
> than bytes (a factor of at most 3, on a limit that is a guard rail), and a guard on
> `handleUpgrade()` after `close()` (not observable in any real deployment sequence).

## Findings & recommendations (detailed)

### 1. Validate frame shapes; never let a handler throw escape

- **Problem / observation** — Any authenticated client can crash the server process with
  one frame. `#onSub` and `#onUnsub` iterate `rooms` with `for…of` without checking that
  it is an array; a number is not iterable, the `TypeError` rejects the `#onMessage`
  promise, and that promise is discarded with `void` in the socket callback. Deno treats
  the unhandled rejection as fatal. Reproduced: `{"type":"sub","id":"x","rooms":5}` exits
  the process with code 1. Without `verify` this needs no credentials.
- **Evidence** —
  - [`src/server/service.ts:228`](../../src/server/service.ts#L228):
    `void this.#onMessage(conn, event.data);`
  - [`src/server/service.ts:503`](../../src/server/service.ts#L503):
    `for (const request of requests ?? []) {`
  - [`src/server/service.ts:537`](../../src/server/service.ts#L537):
    `for (const room of rooms ?? []) {`
  - The same unvalidated trust applies to `frame.room` in `#onPub`/`#onBroadcast`
    ([`:559`](../../src/server/service.ts#L559), [`:600`](../../src/server/service.ts#L600))
    and to `frame.clientId`/`frame.namespace` in `#onAuth`
    ([`:467-468`](../../src/server/service.ts#L467-L468)), where an empty string or a
    non-string becomes a registry key. The Python reference in `PROTOCOL.md` §9 already
    validates both; the Deno server has drifted from the spec it ships.
- **Proposed change** —
  1. In `#onMessage`, wrap everything after decoding in `try … catch`. On an unexpected
     throw: `logger.error`, send `{ type: "error", error: { code: ERROR_CODE.INTERNAL, … } }`,
     then `#close(conn, CLOSE.INTERNAL_ERROR, "internal error")`. Both constants already
     exist ([`constants.ts:73`](../../src/protocol/constants.ts#L73),
     [`:116`](../../src/protocol/constants.ts#L116)). Keep a `.catch` on the `void`ed call
     in `handleUpgrade` as the last line of defence, so a future bug is a logged line and
     a closed socket, never a dead process.
  2. Malformed but well-formed-JSON input is a client error, not a server failure, and
     keeps the socket: `sub`/`unsub` with a non-array `rooms` → `nack` `bad_request`
     "rooms must be an array"; entries that are not `{ room: string }` are skipped as now.
     `pub`/`broadcast` with a missing or empty `room` → `nack` `bad_request` "missing room"
     (this is what `PROTOCOL.md` §7 line 467 and the Python reference already do). A `pub`
     whose `namespace` is present but not a string → `bad_request`.
  3. `#onAuth`: honour `frame.clientId` and `frame.namespace` only when they are non-empty
     strings; otherwise fall through to the generated id / `DEFAULT_NAMESPACE`, exactly as
     the Python `_on_auth` does.
- **Done when** — a raw client sending `{"type":"sub","id":"x","rooms":5}` receives a
  `nack` with code `bad_request` and the server keeps serving other clients, and the new
  `tests/protocol.test.ts` passes under `deno task test`.
- **Affected files** — `src/server/service.ts`, `tests/_helpers.ts` (a raw-socket helper:
  open, send, collect frames), `tests/protocol.test.ts` (new), `PROTOCOL.md` §7 (one line:
  non-array `rooms` is `bad_request`).
- **Effort / Value / Risk** — S / high / low. No wire change; only previously-crashing or
  previously-silent inputs change behaviour.
- **Implementation notes** — Tests to add, all with raw sockets against `startServer()`:
  non-array `rooms` on `sub` and on `unsub` → `nack bad_request`, socket still open,
  a following `ping` still gets `pong`; `pub` without `room` → `nack bad_request`;
  `auth` with `clientId: 123` → `hello` carries a generated id; `auth` with
  `namespace: ""` → `hello` says `default`. For the backstop, inject a throwing frame via
  the `decode` option (a decoded object whose `type` getter throws) and assert an `error`
  frame with code `internal` followed by close `1011`, with the test process alive.

### 2. Hand `verify` the client's requested identity

- **Problem / observation** — Namespace is the isolation boundary, and the server honours
  the client's requested namespace whenever `verify` does not return one. `verify` receives
  only the payload and the upgrade request, so it cannot even see the request it is
  silently approving. In a multi-tenant deployment this means any authenticated user can
  enter any tenant unless the application remembers to assign `namespace` itself, and it
  cannot validate the request without duplicating the namespace into the auth payload.
  Client ids have the same shape of problem: a claimed id evicts its owner (§3.2 of the
  protocol) and is what `from` shows to peers. `PROTOCOL.md` warns about this; `README.md`
  and `API.md`, where the `verify` hook is actually documented, do not.
- **Evidence** —
  - [`src/server/service.ts:61-64`](../../src/server/service.ts#L61-L64): the hook is
    `(payload: unknown, request: Request)`.
  - [`src/server/service.ts:467-468`](../../src/server/service.ts#L467-L468):
    `const id = result.clientId ?? frame.clientId ?? conn.id;` and
    `const namespace = result.namespace ?? frame.namespace ?? DEFAULT_NAMESPACE;`
  - [`PROTOCOL.md:137`](../../PROTOCOL.md#L137) carries the warning; the `verify` row in
    [`API.md:326`](../../API.md#L326) says only "Absent means no authentication".
- **Proposed change** — Add a third argument, non-breaking:

  ```ts
  /** What the client proposed in its `auth` frame. Hints, not facts. */
  export interface WSRequestedIdentity {
  	clientId?: string;
  	namespace: string;
  }

  verify?: (
  	payload: unknown,
  	request: Request,
  	requested: WSRequestedIdentity,
  ) => Promise<AuthResult | null> | AuthResult | null;
  ```

  Pass the values already validated by #1 (`namespace` falls back to `DEFAULT_NAMESPACE`
  when absent). Export the type from `src/server/mod.ts`. Then document the rule in the
  three places a server author reads: the `verify` row and a short **Security** callout
  under `createWSApp` in `API.md`; one sentence plus a code comment in the README server
  example; a line under "Safe defaults are deny" in `AGENTS.md`. The rule: _in any
  multi-tenant deployment `verify` must return `namespace` and `clientId`; otherwise the
  client's proposals are honoured verbatim. Use `requested` to validate them instead of
  duplicating them into the auth payload._
- **Done when** — a test shows `verify` receiving `{ clientId: "alice", namespace: "org-1" }`
  for a client configured with those values, a `verify` that returns `null` on a
  disallowed `requested.namespace` closes the socket with `4001`, and `API.md` documents
  the third argument.
- **Affected files** — `src/server/service.ts`, `src/server/mod.ts`, `src/protocol/frames.ts`
  or `service.ts` for the new interface (keep it next to `AuthResult` in `frames.ts` so
  the protocol entry point stays the single source of truth), `tests/integration.test.ts`,
  `API.md`, `README.md`, `AGENTS.md`.
- **Effort / Value / Risk** — S / high / low. Additive; existing two-argument hooks keep
  working.
- **Implementation notes** — Depends on #1 having validated the strings. Do not change the
  fallback order (assigned → requested → generated); the point is visibility and
  documentation, not a new default that would break single-tenant users.

### 3. One handshake per socket; no ghosts

- **Problem / observation** — `#onAuth` checks `conn.authed` before awaiting `verify`, so
  two `auth` frames sent back to back run `verify` twice concurrently and send two
  `hello` frames. Worse, a socket that closes while `verify` is pending is registered as an
  authenticated connection afterwards: `#onClose` already ran, nothing runs again, and the
  entry survives until the idle sweeper reaps it. With `idleTimeout: 0` it is permanent.
  Reproduced: two frames → `verify` called twice; close during `verify` →
  `stats().connections === 1` with no live socket.
- **Evidence** —
  - [`src/server/service.ts:451`](../../src/server/service.ts#L451): `if (conn.authed) return;`
    precedes the `await` on [`:456`](../../src/server/service.ts#L456).
  - [`src/server/service.ts:484-485`](../../src/server/service.ts#L484-L485): registration
    happens after the await with no check that the socket is still open.
- **Proposed change** — Add `verifying: boolean` and `closed: boolean` to `Connection`.
  In `#onAuth`: `if (conn.authed || conn.verifying) return;` set `verifying = true` before
  the await and `false` after; after the await, `if (conn.closed || conn.socket.readyState !== WebSocket.OPEN) return;`
  before touching the registry. Set `conn.closed = true` at the top of `#onClose`.
- **Done when** — two tests pass: two `auth` frames call `verify` once and produce one
  `hello`; a socket closed during a pending `verify` leaves `stats().connections === 0`
  after `verify` resolves.
- **Affected files** — `src/server/service.ts`, `tests/protocol.test.ts`.
- **Effort / Value / Risk** — S / med / low.
- **Implementation notes** — The auth timer is cleared before `verify` runs
  ([`:448`](../../src/server/service.ts#L448)), so a slow `verify` is bounded only by the
  client's own 10 s handshake deadline. Leave that as is; note it in the JSDoc of
  `authTimeout`.

### 4. Opt-in `allowedOrigins` check on the upgrade

- **Problem / observation** — `handleUpgrade` upgrades unconditionally. The `verify` hook
  is handed the upgrade request "headers, cookies, url", which invites cookie-based
  authentication, and a cookie-authenticated WebSocket endpoint with no `Origin` check is
  the textbook cross-site WebSocket hijacking setup: a page on another site opens a socket
  to this server, and whichever cookies the browser attaches travel with it.
- **Evidence** —
  - [`src/server/service.ts:204-205`](../../src/server/service.ts#L204-L205): the request
    goes straight to `Deno.upgradeWebSocket`.
  - [`src/server/service.ts:38-39`](../../src/server/service.ts#L38-L39): the context doc
    advertises "headers, cookies, url".
- **Proposed change** — A new `WSServiceOptions.allowedOrigins`, opt-in:

  ```ts
  /**
   * Origins allowed to open a socket. Unset means no check — safe only when
   * `verify` does not rely on cookies. An array permits a *missing* Origin
   * (non-browser clients) and requires a listed one when present; a function
   * decides on its own.
   */
  allowedOrigins?: string[] | ((origin: string | null, request: Request) => boolean);
  ```

  Checked at the top of `handleUpgrade` before the upgrade; a rejected request gets
  `new Response("Origin not allowed", { status: 403 })`. `createWSApp` inherits it through
  `WSAppOptions extends WSServiceOptions`. Document it in the `createWSApp` table plus a
  "Cross-site WebSocket hijacking" note in `API.md`, and one sentence in the README server
  section.
- **Done when** — an upgrade request carrying an unlisted `Origin` is answered `403` and
  never reaches `verify`; the stock client still connects when its origin is listed or it
  sends none; both are tests.
- **Affected files** — `src/server/service.ts`, `tests/integration.test.ts`, `API.md`,
  `README.md`.
- **Effort / Value / Risk** — S / med / low. Unset keeps today's behaviour exactly.
- **Implementation notes** — For the negative test use `fetch` with the handshake headers
  set by hand (`upgrade`, `connection`, `sec-websocket-key`, `sec-websocket-version`,
  `origin`); a `403` comes back as an ordinary response. Check first what `Origin` Deno's
  own `WebSocket` sends, if any, by logging it inside `verify` — the positive test must
  match that reality rather than assume.

### 5. Mount `/stats` only behind `httpAuth`; answer bad JSON with 400

- **Problem / observation** — Without `httpAuth` the `/stats` route is public and returns
  `namespaces: Record<string, number>`, which lists every connected namespace by name. In
  the README's own server example namespaces are org ids, so an unauthenticated request
  enumerates the tenants that are online. The code comment, `API.md` and the service JSDoc
  all say "counts only, never client ids, safe to expose", which is true of ids and false
  of namespaces. Separately, an invalid JSON body on the injection routes returns `500`
  because a plain `Error` is thrown; demino maps a `status` property on a thrown error.
- **Evidence** —
  - [`src/server/mod.ts:118-121`](../../src/server/mod.ts#L118-L121): unguarded mount.
  - [`src/server/service.ts:306-316`](../../src/server/service.ts#L306-L316): the
    `namespaces` map.
  - [`src/server/mod.ts:160-166`](../../src/server/mod.ts#L160-L166): `readJson` throws
    `new Error("Invalid JSON body")`; reproduced as HTTP `500`.
  - demino honours `err.status` on a thrown error (`src/demino.ts:1138-1140` in the
    `@marianmeres/demino` checkout, version 1.17).
- **Proposed change** — One rule for the whole HTTP surface, matching the POST routes and
  the package's "safe defaults are deny" convention: **no `httpAuth`, no `/stats`**.
  `service.stats()` stays available in-process for development. `readJson` throws
  `Object.assign(new Error("Invalid JSON body"), { status: 400 })`. Fix the claim in all
  four places it is made (`mod.ts` comment, `service.ts` JSDoc, `API.md` line 405, the
  README/API route tables), and the `PROTOCOL.md` §10 table.
- **Done when** — `GET /stats` returns `404` without `httpAuth` and `200` with it, a
  malformed POST body returns `400`, and the existing "stats reports…" test passes with
  `httpAuth` supplied.
- **Affected files** — `src/server/mod.ts`, `src/server/service.ts` (JSDoc only),
  `tests/integration.test.ts`, `README.md`, `API.md`, `PROTOCOL.md` §10,
  `example/README.md` (already says the token is needed; verify wording).
- **Effort / Value / Risk** — S / med / low. A behaviour change for anyone relying on the
  public route in production; the release note must say so.

### 6. Encode a fan-out frame once per namespace

- **Problem / observation** — `#deliverLocal` builds and encodes the `msg` frame inside
  the per-socket loop, so a room with N subscribers serializes the same payload N times.
  The frame differs between recipients only by `namespace`, which is constant within one
  namespace.
- **Evidence** — [`src/server/service.ts:622-631`](../../src/server/service.ts#L622-L631):
  `this.#send(conn, { type: FRAME.MSG, ...message, namespace: ns })` inside `for (const id of ids)`,
  and `#send` calls `this.#encode(frame)` at [`:746`](../../src/server/service.ts#L746).
- **Proposed change** — Encode once per `(message, ns)`, then hand the encoded value to a
  new `#sendEncoded(conn, wire)`; `#send` becomes `#sendEncoded(conn, this.#encode(frame))`.
  If a custom encoder throws, log and count zero recipients for that namespace rather than
  aborting the other namespaces.
- **Done when** — a test with a counting `encode` option shows exactly one `msg` encoding
  for a publish delivered to three subscribers in one namespace, and two for a broadcast
  spanning two namespaces.
- **Affected files** — `src/server/service.ts`, `tests/integration.test.ts`.
- **Effort / Value / Risk** — S / med / low. Pure refactor of a hot path; the existing
  delivery tests cover ordering and counts.

## Open questions / decisions needed

All resolved with the planner's defaults in the [`PROGRESS.md`](./PROGRESS.md) decisions
log; override any of them there before running the sprint.

- #1: on an unexpected handler throw, close with `1011` after an `error` frame, or keep
  the socket? **Default: close.** A handler that threw may have left the connection's
  bookkeeping half-updated; a reconnect is cheap and the client treats 1011 as recoverable.
- #4: should an array `allowedOrigins` reject a _missing_ `Origin`? **Default: allow.**
  Only browsers send it, and the check exists to stop browsers.
- #5: keep `/stats` public but redacted, or not mounted? **Default: not mounted.** One rule
  for every HTTP route is easier to state and to trust than a route whose shape depends on
  configuration.
