# Reference implementation — a realtime room chat

A complete, standalone app built on `@marianmeres/ws`: a
[demino](https://jsr.io/@marianmeres/demino) server and a plain HTML client
written with [`@marianmeres/vanilla`](https://jsr.io/@marianmeres/vanilla) and
bundled by [`@marianmeres/deno-build`](https://jsr.io/@marianmeres/deno-build).
Styling is [`@marianmeres/design-tokens`](https://jsr.io/@marianmeres/design-tokens)
with the Bootstrap Reboot bridge — minimal, but every colour is a token.

```bash
deno task example        # builds the client bundle, then serves on :8000
```

Open <http://localhost:8000> in two windows with **two different nicknames**.

## What it exercises

| Feature                | Where to look                                                       |
| ---------------------- | ------------------------------------------------------------------- |
| `verify` handshake     | `server.ts` — nickname → client id + namespace; a bad one is `4001` |
| Namespaces             | the "workspace" field; two workspaces never see each other          |
| Rooms                  | the sidebar; switching unsubscribes then subscribes                 |
| Presence               | the member list, plus join/leave lines in the log                   |
| Acknowledged publishes | the `✓ n` next to your own messages — recipients from the ack       |
| Broadcast + its gate   | the "all workspaces" checkbox; denied outside `#announcements`      |
| Reconnect              | the badge in the header; kill the server and watch it recover       |
| Buffered sends         | send while the server is down — it flushes on reconnect             |
| Server-side injection  | `POST /ws/publish/…`; arrives with `from: null` (see below)         |
| The adapter seam       | `history.ts` — a `WSPubSubAdapter` teeing into a ring buffer        |

## Files

```
example/
├── server.ts          three demino apps: /ws (createWSApp), /api, / (static)
├── history.ts         WSPubSubAdapter decorator → in-memory chat history
├── shared.ts          the APPLICATION protocol — what goes inside `payload`
├── build-theme.ts     regenerates public/theme.css from a bundled theme
├── client/
│   ├── mod.ts         views (login + chat), built with @marianmeres/vanilla
│   └── store.ts       the WSClient and all reactive state; no DOM
└── public/
    ├── index.html     templates + one <script type="module">
    ├── app.css        layout, entirely token-driven
    ├── reboot.css     Bootstrap Reboot (vendored, MIT)
    ├── theme.css      generated — design tokens + the --bs-* bridge
    └── dist/bundle.js generated — deno task example:build
```

## Tasks

| Task                      | What                                                   |
| ------------------------- | ------------------------------------------------------ |
| `deno task example`       | build the bundle, then serve                           |
| `deno task example:build` | bundle `client/mod.ts` → `public/dist/bundle.js`       |
| `deno task example:watch` | same, rebuilding on change (run the server separately) |
| `deno task example:theme` | regenerate `public/theme.css`                          |

`PORT` and `WS_EXAMPLE_TOKEN` are read from the environment (defaults `8000`
and `dev-secret`).

## Things worth trying

**Watch it reconnect.** Stop the server (Ctrl-C) with the page open. The badge
goes amber and counts attempts; every socket was closed with `1001`, which is
_recoverable_ — a rolling deploy is exactly when clients must come back. Start
the server again and the client re-authenticates, re-subscribes every room, and
re-syncs presence on its own.

**Send while it is down.** Publishes issued offline are buffered (capped, never
unbounded) and flushed _after_ the re-subscribe, so they cannot land in a room
the server has not registered yet. The message appears, with its ack, once the
connection is back.

**Push from outside the browser.** The HTTP injection routes exist only because
`server.ts` supplies `httpAuth` — without it `createWSApp` does not mount them
at all.

```bash
curl -X POST http://localhost:8000/ws/publish/acme/general \
  -H "authorization: Bearer dev-secret" \
  -H "content-type: application/json" \
  -d '{"kind":"chat","id":"srv-1","nick":"deploy-bot","text":"Deploy finished"}'
```

It arrives with `from: null`, which is how a client tells a server push apart
from peer traffic — the example renders those as a highlighted system line.
Send a payload that is _not_ a chat line and the client falls back to printing
the raw JSON rather than dropping it.

**Cross the namespace boundary.** Log in twice with different workspaces and
talk in `#general` — neither side sees the other. Then tick "broadcast to all
workspaces" in `#announcements`, where the server's `allowBroadcast` says yes.
Try the same checkbox in `#general` and the refusal comes back as a `nack`
(`WSRemoteError`), because the gate **denies by default**.

**Get rejected.** Log in as `admin`. `verify` returns `null`, the socket closes
with `4001 AUTH_FAILED`, and the client _stops_ — a terminal close is the one
exit that does not retry, so the app has to route you back to the form.

```
GET  /ws/stats                     # connection counts (needs the bearer token)
GET  /api/history/{workspace}/{room}
```

## Notes on the design

**The protocol does not know what a chat is.** `shared.ts` defines
`{ kind: "chat" | "typing", … }` and both ends import it. The library never
looks inside `payload` — typing pings and chat lines are indistinguishable to
it, and telling them apart is entirely the app's job.

**History is the app's job too.** There is no server-side replay: a message
that was transmitted but unacknowledged when a socket died is _not_ resent, and
the server forgets it the moment it is fanned out. So the backlog comes from a
plain `fetch` on join, and the client de-duplicates it against live traffic
using an id it put in the payload itself.

**`history.ts` uses the adapter seam.** `WSService` hands every publish and
broadcast to `adapter.publish(envelope)` after local delivery — the one place
all traffic converges. A real deployment puts Redis or Deno KV there; here a
decorator records chat lines and delegates the rest. Broadcasts (`namespace:
null`) belong to no single room and are deliberately not stored.

**Client ids are not nicknames.** `verify` returns `nick#suffix`, with the
suffix proposed by the tab and validated server-side. Ids are unique by
contract — a second connection authenticating with a live id replaces it — so
letting two tabs share one would make them kick each other forever. Keeping the
suffix stable across reconnects is what stops presence from churning.
