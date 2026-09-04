<!--
GENERATED ANALYSIS — @marianmeres/ws (documentation and release)
Produced 2026-09-04 by an inline single-agent review: every source, test and doc file read;
each bug reproduced with a script against the real server; every file:line re-opened before
writing. Claims verified against the codebase at commit db21ff3. Planning artifact; no code
was changed.
-->

# Documentation sweep and release

> The docs are unusually good for a 0.x package: `API.md` covers every export,
> `PROTOCOL.md` is complete enough that a Python server written from it passed a
> conformance run against the real client, and `AGENTS.md` encodes the load-bearing
> invariants. The drift is small and specific: a handful of claims that the code does not
> honour, and a few behaviours the code has that the docs omit.
>
> Most of that drift is fixed _inside_ the code tasks in [`01-server.md`](./01-server.md)
> and [`02-client.md`](./02-client.md), because the doc that describes a behaviour belongs
> in the commit that changes it. What is left here is the residue — wording that no code
> task touches — plus one consistency pass over `PROTOCOL.md` after everything has landed,
> and the release, which is a human's to do.
>
> **Headline recommendation:** run #1 last, after every code task, and keep it to the
> listed items; a "while I'm in there" rewrite of `PROTOCOL.md` is exactly the drift this
> task exists to remove.

## Summary of recommendations

| # | Recommendation                                                     | Value | Effort | Risk |
| - | ------------------------------------------------------------------ | ----- | ------ | ---- |
| 1 | Sweep the remaining drift and re-align `PROTOCOL.md` with the code | med   | S      | low  |
| 2 | Release 0.4.0 (human-only)                                         | high  | S      | low  |

> **Cut from the draft:** the stale `.npm-dist/` (gitignored; the `publish` task rebuilds
> it), and rewriting the `ws or sse?` section — it is accurate and the linked
> `COMPARISON.md` exists in the sibling package.

## Findings & recommendations (detailed)

### 1. Sweep the remaining drift and re-align `PROTOCOL.md`

- **Problem / observation** — After the code tasks, these items are still wrong or
  missing, none of them owned by a code change:
  1. [`API.md:85-92`](../../API.md#L85-L92) — `connect()` also rejects with
     `WSTerminatedError` code `4900` when `disconnect()` is called while it is pending,
     and a `dispose()` during a pending connect yields that same error rather than
     `WSDisposedError`. Verified by reproduction.
  2. [`API.md:37`](../../API.md#L37) — `pingInterval: 0` needs the note that the
     reference server still reaps a silent connection after `idleTimeout`, so a
     heartbeat-free client reconnects roughly every minute; `pingInterval` and
     `idleTimeout` are disabled together or not at all.
  3. [`README.md:14`](../../README.md#L14) — "the tab regains focus": the trigger is
     `visibilitychange`, not focus. Say "becomes visible again".
  4. [`PROTOCOL.md:221`](../../PROTOCOL.md#L221) — `4400` is listed as server-sent only;
     the client also sends it when `auth()` throws
     ([`ws-client.ts:818`](../../src/client/ws-client.ts#L818)).
  5. `PROTOCOL.md` §7 (line 456 onward) — add that a non-array `rooms` is answered with
     `nack bad_request` and the socket stays open (server task #1), and give the Python
     `_on_sub`/`_on_unsub` in §9 the matching `isinstance(rooms, list)` guard so the
     reference implementation does not raise inside its handler task on that input.
  6. `PROTOCOL.md` §10 (line 1022) — the `/stats` row must say it exists only behind HTTP
     authentication (server task #5). Add `allowedOrigins` to the same section as an
     optional server behaviour (server task #4).
  7. `PROTOCOL.md` Appendix B (line 1359) — a checklist line for "`pub` without a `room`
     and `sub` with a non-array `rooms` are answered `bad_request` without closing".
  8. [`AGENTS.md:125`](../../AGENTS.md#L125) — the test count; re-read it from
     `deno task test` output at the time. Add `tests/protocol.test.ts` and
     `tests/codec.test.ts` to the "Before Making Changes" list where the file inventory
     appears. Under "Critical Conventions", two new lines: a control-frame promise the
     client does not await must carry a catch (client task #1); the server validates
     every frame field before use and wraps its dispatch, so a handler throw is a closed
     socket and never a dead process (server task #1). Under "Known Gaps", remove nothing,
     add "Origin checking is opt-in via `allowedOrigins`".
  9. `API.md` errors table ([`:613`](../../API.md#L613)) — confirm `WSConnectionLostError`
     was added by client task #2 and that every method's **Throws** list is consistent
     with the code after tasks #2 and #3.
- **Evidence** — Each item above cites its line; the behaviours were confirmed by the
  reproduction scripts run during the analysis.
- **Proposed change** — Edit exactly the items listed. Then read `PROTOCOL.md` §1 ("the
  short version") once against the final code and correct any of its ten statements that
  the sprint changed; as of the plan, none of them do.
- **Done when** — `grep -n "regains focus" README.md` prints nothing; the test count in
  `AGENTS.md` equals the count `deno task test` prints; `PROTOCOL.md` §7 and §10 describe
  the behaviour landed by server tasks #1, #4 and #5; and the sprint's `Verify:` commands
  pass.
- **Affected files** — `API.md`, `README.md`, `PROTOCOL.md`, `AGENTS.md`.
- **Effort / Value / Risk** — S / med / low.
- **Implementation notes** — Depends on every code task. Leave the version strings in
  `PROTOCOL.md` ("Written against 0.3.0", the `jsr:@marianmeres/ws@^0.3.0` import in
  Appendix A) alone; the release step below owns them. Keep `deno fmt`'s
  `proseWrap: preserve` in mind — do not reflow paragraphs you did not change.

### 2. Release 0.4.0 — human-only

- **Problem / observation** — The sprint changes public behaviour (`/stats` gating, the
  new `WSConnectionLostError`, immediate rejection of in-flight sends, `close` on
  `disconnect()`) and adds public API (`allowedOrigins`, the third `verify` argument,
  `WSRequestedIdentity`). That is a minor bump on a public package, and a release is
  irreversible and outward-facing, so it is not the machine's to take.
- **Proposed change** — In this order: update the two version references in
  `PROTOCOL.md` to `0.4.0`; run `deno task rpm` (bumps the minor version, publishes to JSR,
  builds and publishes npm). Write the release note from the decisions log in
  [`PROGRESS.md`](./PROGRESS.md), naming the four behaviour changes above explicitly.
  `PROTOCOL_VERSION` stays `1` — nothing on the wire changed; the new `bad_request`
  answers cover inputs the old server crashed or silently accepted on.
- **Done when** — `jsr:@marianmeres/ws@0.4.0` and `@marianmeres/ws@0.4.0` on npm resolve,
  and `PROTOCOL.md` names `0.4.0`.
- **Affected files** — `deno.json` (by the release task), `PROTOCOL.md`, `.npm-dist/`
  (rebuilt).
- **Effort / Value / Risk** — S / high / low.

## Open questions / decisions needed

- Minor (`0.4.0`) rather than patch: **decided, see the decisions log** — new options and
  a new error class are additions, and two behaviours change visibly.
- Whether to bump `PROTOCOL_VERSION`: **decided, no** — see #2.
