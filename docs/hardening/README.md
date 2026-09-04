# Hardening plan — `@marianmeres/ws`

This directory holds a **code-verified** review of the client, the reference server and
the docs, produced on **2026-09-04** against commit `db21ff3`, and the single sprint that
acts on it. It is a **planning artifact** — no source code was changed. Every finding was
reproduced with a script against the real server before it was written down; every
`file:line` was re-opened before it was cited; suggestions that were low-value or not this
package's problem were cut and are listed as such in each doc.

**Start here:** [`00-overview-and-roadmap.md`](./00-overview-and-roadmap.md) — the ranked
list, the sprint in execution order, the themes, and the dependency graph. Then
[`PROGRESS.md`](./PROGRESS.md) to run it.

## Documents

| #  | Doc                                                  | Scope                                      | Headline finding                                                                                       |
| -- | ---------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| 00 | [overview-and-roadmap](./00-overview-and-roadmap.md) | Synthesis, ranking, one sprint             | Sound design, two crashers, a tail of promises that settle late or wrong; 13 tasks plus a release      |
| 01 | [server](./01-server.md)                             | Reference server: input, trust boundaries  | One `sub` frame kills the process (#1); namespace isolation is client-controlled by default (#2)       |
| 02 | [client](./02-client.md)                             | Client: promise semantics, sockets         | The README's `unsub(); ws.dispose();` exits Deno/Node (#1); in-flight frames outlive their socket (#2) |
| 03 | [docs-and-release](./03-docs-and-release.md)         | Residual doc drift, `PROTOCOL.md`, release | Small, specific drift; one sweep after the code lands, then a human-only 0.4.0                         |

## How it was produced

Inline single-agent review: every source, test and doc file read in full → each suspected
bug turned into a script and run against the real server (eight scripts, all confirmed) →
findings written with `file:line` and a **Done when** each → decisions taken with the
planner's defaults and recorded in the tracker. "Cut from the draft" notes inside the docs
record what was dropped and why.

> The decisions each task needed are already in the `PROGRESS.md` decisions log as the
> planner's defaults. They are yours to override — do it there, before `sprint` runs, not
> during.
