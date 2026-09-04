# Implementation Progress — @marianmeres/ws hardening

<!-- tracker: v1 -->

Living tracker for acting on [`00-overview-and-roadmap.md`](./00-overview-and-roadmap.md).
A fresh conversation should read this file first, then the relevant `NN-*.md` section.

**Status legend:** ⬜ ready · 🚧 in progress · ⏸️ blocked/awaiting decision · 🔒 human-only · ✅ done · ⏭️ deferred

> Convention: one branch per sprint, one commit per task. Each task resolves its source doc's
> "Open questions" first (record in the Decisions log), then implement → verify → tick here.
> Every task adds or adjusts the test named in its **Done when** and updates the docs that
> describe the behaviour it changes — the doc lives in the same commit as the code.

## Sprint 1 — hardening (crashers, trust boundaries, promise semantics)

```sprint
Branch: `sprint/hardening`
Verify: deno task test
Verify: deno lint
Verify: deno fmt --check
Verify: deno check src/mod.ts src/server.ts src/protocol.ts
Verify: deno doc --lint src/mod.ts src/server.ts src/protocol.ts
Verify: deno publish --dry-run --allow-dirty
```

| Status | ID  | Deps                                            | Task                                                                       | Source                            | Commit |
| ------ | --- | ----------------------------------------------- | -------------------------------------------------------------------------- | --------------------------------- | ------ |
| ✅     | T01 | —                                               | Server: validate frame shapes; never let a handler throw escape the socket | [01](./01-server.md) #1           | —      |
| ✅     | T02 | —                                               | Client: catch the unsubscriber's fire-and-forget `unsub`                   | [02](./02-client.md) #1           | —      |
| ✅     | T08 | T01                                             | Server: hand `verify` the requested identity; document the isolation rule  | [01](./01-server.md) #2           | —      |
| ✅     | T03 | T02                                             | Client: settle in-flight frames the moment the socket closes               | [02](./02-client.md) #2           | —      |
| ✅     | T04 | T03                                             | Client: fail sends fast on terminated state and encode errors              | [02](./02-client.md) #3           | —      |
| ⬜     | T07 | T03                                             | Client: emit `close` on a local `disconnect()`; retire `#intentional`      | [02](./02-client.md) #6           | —      |
| ⬜     | T05 | —                                               | Client: receive binary frames as `ArrayBuffer`                             | [02](./02-client.md) #4           | —      |
| ⬜     | T06 | —                                               | Client: a stale `auth()` rejection must not close a newer socket           | [02](./02-client.md) #5           | —      |
| ⬜     | T09 | T01                                             | Server: one handshake per socket; no ghost registration                    | [01](./01-server.md) #3           | —      |
| ⬜     | T10 | —                                               | Server: opt-in `allowedOrigins` check on the upgrade                       | [01](./01-server.md) #4           | —      |
| ⬜     | T11 | —                                               | Server: mount `/stats` only behind `httpAuth`; answer bad JSON with 400    | [01](./01-server.md) #5           | —      |
| ⬜     | T12 | —                                               | Server: encode a fan-out frame once per namespace                          | [01](./01-server.md) #6           | —      |
| ⬜     | T13 | T01 T02 T03 T04 T05 T06 T07 T08 T09 T10 T11 T12 | Docs: sweep the remaining drift; re-align `PROTOCOL.md` with the code      | [03](./03-docs-and-release.md) #1 | —      |
| 🔒     | T14 | T13                                             | Release 0.4.0: bump `PROTOCOL.md` version references, run `deno task rpm`  | [03](./03-docs-and-release.md) #2 | —      |

Row order is execution order, not rank; the ranking is in the overview. T14 is human-only
by design: publishing is irreversible and outward-facing. When the driver runs dry on it,
that is the planned handover, not a defect.

There is no backlog: the user asked for one sprint holding everything. Items deliberately
left out are listed under "Deliberately omitted" in the overview, each with its reason.

## Decisions log

- **2026-09-04** — A send issued in the `terminated` state rejects at once with the
  `WSTerminatedError` of that close, kept in its own `#terminalError` field so a later
  handler-thrown `#lastError` cannot mask it; a frame the encoder refuses rejects with the
  runtime's own error, unwrapped, and still emits `error` — nothing restarts from
  `terminated` except an explicit `connect()`, and a frame that never left the process
  cannot be acknowledged, so buffering either one only defers a known answer by
  `sendTimeout`. `subscribe()` while terminated still registers the room and resolves. (T04)
- **2026-09-04** — In-flight frames are settled at the moment a socket closes: `sub` and
  `unsub` resolve, `pub` and `broadcast` reject with a new `WSConnectionLostError`; queued
  frames still wait for the reconnect; the terminal path keeps `failAll` — resolving a lost
  `sub` is the same state as a `subscribe()` issued offline, which already resolves; a
  distinct error lets callers branch on `instanceof` between "retry when connected" and
  "the server is not answering". Delivery stays at-most-once. (T03)
- **2026-09-04** — An unexpected throw inside the server's frame dispatch sends an `error`
  frame with code `internal` and closes with `1011`; a malformed-but-parseable frame is a
  `nack`/`error` with `bad_request` and the socket stays open — a throwing handler may have
  left bookkeeping half-done and 1011 is recoverable for the client; malformed input is the
  client's mistake and costs it nothing more than the one request. (T01)
- **2026-09-04** — `verify` gains a third argument `requested: { clientId?, namespace }`;
  the fallback order (assigned → requested → generated) is unchanged — the fix is
  visibility and a documented rule, not a new default that would break single-tenant
  users. (T08)
- **2026-09-04** — `allowedOrigins` is opt-in; unset means no check, exactly today's
  behaviour; the array form permits a _missing_ `Origin` and requires a listed one when
  present — only browsers send the header, and the check exists to stop browsers. (T10)
- **2026-09-04** — `/stats` follows the POST routes: not mounted without `httpAuth`;
  `service.stats()` remains for in-process use — one rule for the whole HTTP surface
  matches the package's "safe defaults are deny" and is easier to trust than a route whose
  shape depends on configuration. Named in the release note as a behaviour change. (T11)
- **2026-09-04** — `disconnect()` emits `close` with code `4900` and `willReconnect:
  false` when it actually closed a socket; nothing from `idle` or `reconnecting` — the
  event's doc, the `CLIENT_GONE` code and the `#intentional` flag all describe this intent
  and the code simply never reached it. (T07)
- **2026-09-04** — An `unsub` that could not be confirmed after dispose or a terminal close
  is logged at debug level, never surfaced as an `error` event — it is not actionable.
  (T02)
- **2026-09-04** — Release is minor, `0.4.0`, and `PROTOCOL_VERSION` stays `1` — new
  options and a new error class are additions, four behaviours change visibly, and nothing
  on the wire changed. (T14)
- **2026-09-04** — Omitted from the sprint as low value: a synthesized presence `sync`
  for a late presence handler; per-IP connection caps; `maxFrameSize` in bytes rather than
  UTF-16 units; guarding `handleUpgrade()` after `close()`; client-side validation of
  server frame shapes. Revisit any of them only with a concrete consumer need.

## How to resume (for a fresh conversation)

1. Read this file + `00-overview-and-roadmap.md`.
2. Pick the first ⬜ task whose `Deps` are all ✅ (or the `T##` you were given); open its
   source doc section for the detail and its **Done when** criterion. Check the finding
   still holds against the current code before writing anything — if it does not, stop and
   say so.
3. Resolve that task's "Open questions" with the owner; every one of them already has a
   default in the Decisions log above, so this is a confirmation, not a discussion.
4. Branch → implement → add the test the **Done when** names → run the `Verify:` commands
   → update this file → commit when the owner asks.
