<!--
GENERATED ANALYSIS — @marianmeres/ws (client)
Produced 2026-09-04 by an inline single-agent review: every source, test and doc file read;
each bug reproduced with a script against the real server; every file:line re-opened before
writing. Claims verified against the codebase at commit db21ff3. Planning artifact; no code
was changed.
-->

# Client — promises that must settle, and settle honestly

> The client's core design — generation-guarded sockets, a refcounted room registry, one
> deadline per send, re-subscribe before flush — is sound and the resilience tests prove
> the parts that matter. What is wrong is narrower and all of one kind: **promises that
> either never settle or settle with the wrong answer**. One of them is fatal: the
> README's own `unsub(); ws.dispose();` produces an unhandled rejection that exits a Deno
> or Node process.
>
> The second theme is that the client waits out a 30-second timer for answers it already
> knows. A publish in flight when the socket drops cannot be acknowledged by anyone, a
> publish after a terminal close cannot go anywhere, and a payload that will not serialize
> cannot go anywhere either — yet all three hang for `sendTimeout` and then report a
> generic timeout. Worse, a `subscribe()` in that state rejects and detaches its handler
> after the reconnect has already re-established the room.
>
> **Headline recommendation:** fix the unhandled rejection first (#1, ten lines), then
> settle in-flight frames on close (#2) — that one change removes the whole "wait for a
> timer to say what we already know" family and is the prerequisite for #3 and #6.

## Summary of recommendations

| # | Recommendation                                                         | Value | Effort | Risk |
| - | ---------------------------------------------------------------------- | ----- | ------ | ---- |
| 1 | Catch the unsubscriber's fire-and-forget `unsub`                       | high  | S      | low  |
| 2 | Settle in-flight frames the moment the socket closes                   | high  | M      | med  |
| 3 | Fail sends fast when the outcome is already known                      | med   | S      | low  |
| 4 | Receive binary frames as `ArrayBuffer`                                 | med   | S      | low  |
| 5 | A stale `auth()` rejection must not close a newer socket               | med   | S      | low  |
| 6 | Emit `close` on a local `disconnect()`, retire the dead `#intentional` | med   | S      | low  |

> **Cut from the draft:** synthesizing a local presence `sync` for a handler added to a
> room that already has presence (cheap, but it changes when presence callbacks fire and
> no consumer has asked for it — revisit if one does), and validating server frame shapes
> on the client (the client only talks to servers the application chose; a malformed
> `presence.members` is a server bug, not an attack surface).

## Findings & recommendations (detailed)

### 1. Catch the unsubscriber's fire-and-forget `unsub`

- **Problem / observation** — The unsubscriber returned by `subscribe()` sends an `unsub`
  frame when the last handler leaves while connected, and drops the promise that
  `#sendControl` returns. That promise rejects on dispose (`failAll`), on a terminal close
  (`failAll`) and on the send timeout. Nothing catches it. Reproduced with the README's
  exact sequence, `unsub(); ws.dispose();`: Deno exits with
  `Uncaught (in promise) WSDisposedError`. Node 15+ behaves the same by default; browsers
  log an uncaught error.
- **Evidence** —
  - [`src/client/ws-client.ts:580-589`](../../src/client/ws-client.ts#L580-L589): the
    closure calls `this.#sendControl({ type: FRAME.UNSUB, … })` as a statement.
  - [`src/client/ws-client.ts:745-750`](../../src/client/ws-client.ts#L745-L750):
    `#sendControl` tracks the frame in the outbox, so its promise can reject.
  - [`src/client/ws-client.ts:524`](../../src/client/ws-client.ts#L524): `dispose()` calls
    `failAll(new WSDisposedError())`, which is the rejection in the reproduction.
  - `README.md` lines 80-81 show the sequence as the canonical usage.
- **Proposed change** — Attach a catch that logs at debug level and does nothing else; an
  unsub that could not be confirmed is not actionable and must not become an `error`
  event. Audit the other fire-and-forget sends while there: the `#onHello` re-subscribe
  already has a catch ([`:937`](../../src/client/ws-client.ts#L937)); `unsubscribe()` is
  awaited by the caller; the heartbeat ping is untracked. Add a one-line convention to
  `AGENTS.md`: a control-frame promise the client does not await must carry a catch.
- **Done when** — a test that subscribes, calls the unsubscriber while connected and
  immediately disposes passes under `deno test` with no uncaught promise rejection
  reported.
- **Affected files** — `src/client/ws-client.ts`, `tests/resilience.test.ts`, `AGENTS.md`.
- **Effort / Value / Risk** — S / high / low.

### 2. Settle in-flight frames the moment the socket closes

- **Problem / observation** — `#onClose` never touches the outbox on a recoverable close.
  Queued frames rightly wait for the reconnect, but frames already transmitted and
  awaiting an ack are left pending although that ack can no longer arrive: the socket
  that would carry it is gone, and the next socket is a new session. Three consequences,
  all reproduced:
  1. A `subscribe()` whose `sub` was sent just before the drop waits out `sendTimeout`,
     rejects with `WSTimeoutError`, and its catch **detaches the handler** — after the
     reconnect has already re-subscribed the room. End state: the server delivers to a
     room the client no longer holds, and no `unsub` is ever sent. Observed:
     `server.service.members("chat")` lists the client while `isSubscribed("chat")` is
     false.
  2. The batched re-subscribe in `#onHello` has the same exposure and surfaces as a
     spurious `error` event, "re-subscribe failed", 30 s after a second drop.
  3. A `publish()` in flight at the drop hangs for the full `sendTimeout` before
     reporting a timeout, although the answer was known at the close.
- **Evidence** —
  - [`src/client/ws-client.ts:947-982`](../../src/client/ws-client.ts#L947-L982): the
    non-terminal path only logs, emits `close` and schedules the reconnect.
  - [`src/client/ws-client.ts:600-606`](../../src/client/ws-client.ts#L600-L606): the
    catch in `subscribe()` removes the handler on any rejection.
  - [`src/client/outbox.ts:19-26`](../../src/client/outbox.ts#L19-L26): every entry
    already records `queued`, so in-flight frames are distinguishable; `failAll` exists
    ([`:127`](../../src/client/outbox.ts#L127)) and `failQueued`
    ([`:141`](../../src/client/outbox.ts#L141)) is dead code — nothing calls it.
  - `README.md:192-195` documents the current behaviour ("It rejects on `sendTimeout`")
    as a consequence of at-most-once delivery, which it is not: not resending is the
    guarantee; waiting is just a missing branch.
- **Proposed change** —
  1. `src/protocol/errors.ts`: add `WSConnectionLostError extends WSError` — "The
     connection closed before the server acknowledged the frame; it was not resent."
  2. `src/client/outbox.ts`: replace `failQueued` with
     `settleInFlight(decide: (frame: ClientFrame) => Error | null): void` — for every
     pending entry with `queued === false`: discard it, then reject with the returned
     error or resolve with `{ recipients: 0 }` when `null`.
  3. `#onClose`, on every non-terminal path (reconnect _and_ the idle exit), and in
     `disconnect()` after `#closeSocket`:
     `this.#outbox.settleInFlight((f) => f.type === FRAME.SUB || f.type === FRAME.UNSUB ? null : new WSConnectionLostError())`.
     Subs resolve because the room is registered locally and the re-subscribe step will
     establish it — precisely the documented contract for a `subscribe()` issued while
     offline. Unsubs resolve because the server forgets the room on close anyway.
     Publishes and broadcasts reject: they were not delivered and will not be resent.
  4. Leave the terminal path on `failAll(error)` and the queued frames untouched.
  5. Docs: `API.md` errors table and the `publish()` **Throws** list gain the new error;
     `README.md` "Delivery is at-most-once" says it rejects immediately with
     `WSConnectionLostError`; `AGENTS.md` "Every send carries one deadline" gets the
     sentence "and a socket close settles in-flight frames at once".
- **Done when** — two tests pass: a `subscribe()` whose ack is lost to a server restart
  resolves, keeps its handler, and the room is live on both sides after the reconnect; a
  `publish()` in flight when the server closes the socket without acking rejects with
  `WSConnectionLostError` well inside `sendTimeout`. And `Outbox.failQueued` no longer
  exists.
- **Affected files** — `src/protocol/errors.ts`, `src/client/outbox.ts`,
  `src/client/ws-client.ts`, `tests/unit.test.ts`, `tests/resilience.test.ts`,
  `tests/_helpers.ts` (a stub server that accepts a `pub` and closes instead of acking),
  `API.md`, `README.md`, `AGENTS.md`.
- **Effort / Value / Risk** — M / high / med. The risk is behavioural: a consumer that
  relied on the timeout as its only failure signal now gets a faster, more specific
  rejection. That is the documented intent, so the release note names it.
- **Implementation notes** — Do #1 first: once in-flight unsubs reject immediately on
  close, the uncaught promise in the unsubscriber would fire far more often. The
  reproduction for the lost `sub` is: `const stopping = server.stop()` without awaiting,
  `subscribe()` while the state still reads `open`, await `stopping`, restart on the same
  port, then assert. Keep `subscribe()`'s catch as it is — after this change a rejection
  there means the server actually answered, or is unresponsive for a full `sendTimeout`.

### 3. Fail sends fast when the outcome is already known

- **Problem / observation** — Two more cases where the client knows the answer and waits
  anyway. After a terminal close the state is `terminated` and `autoConnect` does not
  restart from it, yet `#send` queues the frame; the caller learns 30 s later that
  nothing was acknowledged, not that the connection was refused. Reproduced: `publish()`
  after a 4001 rejects with `WSTimeoutError` after exactly `sendTimeout`. And when
  `encode` throws — a `BigInt` in the payload is enough with the JSON default —
  `#sendRaw` turns the exception into an `error` event and returns, so the tracked
  promise waits for an ack that was never requested and then reports a timeout.
- **Evidence** —
  - [`src/client/ws-client.ts:723-736`](../../src/client/ws-client.ts#L723-L736): `#send`
    checks `outboxMaxSize` but never the `terminated` state.
  - [`src/client/ws-client.ts:752-760`](../../src/client/ws-client.ts#L752-L760):
    `#sendRaw` catches and swallows; it has no return value.
  - Reproduced: `publish({ big: 10n })` rejects with
    "No acknowledgement within 500ms" while the `error` event says
    "Do not know how to serialize a BigInt".
- **Proposed change** — Keep the terminal error in its own field (`#terminalError`) so a
  later handler-thrown `#lastError` cannot mask it, and in `#send`: when the state is
  `terminated`, `return Promise.reject(this.#terminalError)`. Make `#sendRaw` return
  `Error | null`; `#send`, `#sendControl` and the drain loop in `#onHello` call
  `this.#outbox.fail(id, err)` when it returns an error. `subscribe()` in the terminated
  state keeps registering the room and resolving — a later `connect()` with fresh
  credentials is the documented way out of `terminated`, and rooms must survive it.
- **Done when** — two tests pass: after a terminal close, `publish()` rejects with
  `WSTerminatedError` in well under `sendTimeout`; `publish({ big: 10n })` rejects with an
  error that is not `WSTimeoutError` and whose message names `BigInt`.
- **Affected files** — `src/client/ws-client.ts`, `tests/resilience.test.ts`, `API.md`
  (`publish()` **Throws** list gains `WSTerminatedError`).
- **Effort / Value / Risk** — S / med / low.
- **Implementation notes** — Depends on #2's outbox shape (`fail` already exists; only
  the call sites change). The encode error is an ordinary `TypeError`; do not wrap it, so
  the caller sees the runtime's own message.

### 4. Receive binary frames as `ArrayBuffer`

- **Problem / observation** — The client never sets `binaryType`, which defaults to
  `"blob"` in browsers, Deno and Node. A binary frame therefore arrives as a `Blob`, the
  default decoder passes it to `TextDecoder.decode`, and that throws. Reproduced: a stub
  server sending a binary `hello` leaves the stock client unable to connect, with the
  `error` event reading "Argument 1 is not an ArrayBuffer". Two consequences: the
  `WSDecoder` contract (`string | ArrayBuffer`) is false on the client, so the custom
  binary codec that the docs call "the forward path" cannot be used; and `PROTOCOL.md`
  line 56 claims the client's default decoder accepts binary frames, which it does not.
  The Deno server side already defaults to `arraybuffer` and decoded a binary `auth`
  frame correctly in the same experiment.
- **Evidence** —
  - [`src/client/ws-client.ts:779-786`](../../src/client/ws-client.ts#L779-L786): the
    socket is constructed and stored; no `binaryType`.
  - [`src/client/ws-client.ts:194-195`](../../src/client/ws-client.ts#L194-L195): the
    default decoder assumes `string | ArrayBuffer`.
  - [`PROTOCOL.md:56`](../../PROTOCOL.md#L56).
- **Proposed change** — `socket.binaryType = "arraybuffer";` immediately after
  construction. Reword `PROTOCOL.md` line 56 so it states what is now true: the client
  sets `binaryType` to `arraybuffer`, so binary frames decode as UTF-8 JSON by default and
  a custom decoder receives an `ArrayBuffer`.
- **Done when** — two tests pass: a stub server that answers `auth` with a binary `hello`
  connects the stock client; a client and the reference server configured with a matching
  binary codec (`TextEncoder`/`TextDecoder` over JSON is enough) subscribe, publish and
  deliver end to end.
- **Affected files** — `src/client/ws-client.ts`, `tests/codec.test.ts` (new),
  `tests/_helpers.ts` (binary-hello stub), `PROTOCOL.md`.
- **Effort / Value / Risk** — S / med / low. One line; no consumer can be relying on
  receiving Blobs because the default decoder rejects them.

### 5. A stale `auth()` rejection must not close a newer socket

- **Problem / observation** — `#onOpen` awaits the application's `auth()`; if that call
  rejects after the socket it belonged to was superseded by `disconnect()` and a new
  `connect()`, the catch runs `#forceClose` on whatever socket is current. Reproduced: the
  new, healthy socket receives a `close` with `4400 auth payload failed` and reconnects,
  because the generation check sits _after_ the catch.
- **Evidence** — [`src/client/ws-client.ts:813-822`](../../src/client/ws-client.ts#L813-L822):
  the `catch` on line 816 calls `#fail` and `#forceClose` before line 822 compares
  generations.
- **Proposed change** — In the catch, `if (generation !== this.#generation) return;` as
  the first statement, with a debug log that a superseded auth attempt failed. The
  behaviour for a _current_ socket is unchanged: `4400` and a reconnect, which is right
  for a transient token-refresh failure.
- **Done when** — a test where the first `auth()` call rejects 300 ms after
  `disconnect()` + `connect()` shows no `close` event on the second socket and the client
  still `open` afterwards.
- **Affected files** — `src/client/ws-client.ts`, `tests/resilience.test.ts`.
- **Effort / Value / Risk** — S / med / low.

### 6. Emit `close` on a local `disconnect()`, retire `#intentional`

- **Problem / observation** — `disconnect()` closes the socket through `#closeSocket`,
  which bumps the generation so the real `onclose` is ignored, and never calls `#onClose`.
  So a local disconnect emits no `close` event at all, although `WSEvents.close` is
  documented as "Socket closed" and `CLOSE.CLIENT_GONE` exists for exactly this case. The
  `#intentional` flag is set for this purpose and read in `#onClose` to compute
  `willReconnect`, but every path that reaches `#onClose` has reset it to `false` first,
  so it is dead machinery: `willReconnect` is always `!terminal`. Reproduced: zero `close`
  events after `disconnect()`.
- **Evidence** —
  - [`src/client/ws-client.ts:503-514`](../../src/client/ws-client.ts#L503-L514):
    `disconnect()` sets `#intentional`, closes, and sets state `idle` without emitting.
  - [`src/client/ws-client.ts:1064-1069`](../../src/client/ws-client.ts#L1064-L1069):
    `#closeSocket` increments `#generation`, which silences the socket's own `onclose`.
  - [`src/client/ws-client.ts:954`](../../src/client/ws-client.ts#L954): the only read of
    `#intentional`; [`:490`](../../src/client/ws-client.ts#L490) and
    [`:774`](../../src/client/ws-client.ts#L774) reset it on every connect.
- **Proposed change** — In `disconnect()`, when a socket existed (state was
  `connecting`, `authenticating` or `open`), emit
  `close` with `{ code: CLOSE.CLIENT_GONE, reason: "client disconnect", willReconnect: false }`
  after closing it. Do not emit from `reconnecting` or `idle` — there is no socket to
  close. Delete `#intentional` and simplify `willReconnect` to `!terminal`. Document in
  `API.md`'s `WSEvents` table that a local disconnect emits `close` with `4900` and
  `willReconnect: false`.
- **Done when** — a test shows exactly one `close` event with code `4900` and
  `willReconnect: false` for `disconnect()` while open, and none for `disconnect()` while
  idle; `grep -n intentional src/client/ws-client.ts` is empty.
- **Affected files** — `src/client/ws-client.ts`, `tests/resilience.test.ts`, `API.md`.
- **Effort / Value / Risk** — S / med / low. A consumer that shows "connection lost" on
  every `close` will now also see a deliberate disconnect; `willReconnect: false` and the
  `4900` code are the documented way to tell them apart.
- **Implementation notes** — Do this after #2, which also edits `disconnect()` to settle
  in-flight frames; the two edits land in the same function.

## Open questions / decisions needed

All resolved with the planner's defaults in the [`PROGRESS.md`](./PROGRESS.md) decisions
log; override any of them there before running the sprint.

- #2: reject in-flight publishes at close with a **new** `WSConnectionLostError`, or reuse
  `WSTimeoutError`? **Default: new error.** The two situations call for different
  reactions — one is "retry when connected", the other "the server is not answering" —
  and the package already branches on `instanceof`.
- #2: should in-flight `sub` frames resolve or reject-without-detaching? **Default:
  resolve.** It is the same state as a `subscribe()` issued offline, which resolves.
- #6: emit `close` on `disconnect()`, or only document that it is not emitted?
  **Default: emit.** The event's own doc, the `4900` code and the `#intentional` flag all
  point at the same intent; the code just never reached it.
