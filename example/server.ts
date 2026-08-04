/**
 * Reference server for the chat example.
 *
 * Three demino apps composed into one handler:
 *
 * | Mount  | What                                                          |
 * | ------ | ------------------------------------------------------------- |
 * | `/ws`  | `createWSApp()` — the upgrade endpoint, `/stats`, injection    |
 * | `/api` | this example's own routes (chat history)                      |
 * | `/`    | static files from `example/public`                            |
 *
 * Run:
 * ```
 * deno task example          # builds the client bundle, then serves
 * ```
 *
 * @module
 */

import {
	demino,
	deminoCompose,
	type DeminoHandler,
	logListenInfo,
} from "@marianmeres/demino";
import { base36 } from "@marianmeres/uid";
import { dirname, fromFileUrl, join } from "@std/path";
import { createWSApp } from "../src/server.ts";
import { HistoryAdapter } from "./history.ts";
import {
	BROADCAST_ROOM,
	DEFAULT_WORKSPACE,
	NICK_RE,
	RESERVED_NICKS,
	TAB_RE,
	WORKSPACE_RE,
} from "./shared.ts";

const HERE = dirname(fromFileUrl(import.meta.url));
const PUBLIC_DIR = join(HERE, "public");

const PORT = Number(Deno.env.get("PORT")) || 8000;

/**
 * Shared secret for the HTTP injection routes. Hard-coded default because this
 * is a demo; the point is that the routes exist *only* because `httpAuth` does.
 */
const ADMIN_TOKEN = Deno.env.get("WS_EXAMPLE_TOKEN") || "dev-secret";

/**
 * Every message passes through here on its way out. See `history.ts` — this is
 * the adapter seam doing double duty as the app's message log.
 */
const history = new HistoryAdapter();

/**
 * Guards `POST /ws/publish/…` and `POST /ws/broadcast/…`.
 *
 * A middleware that *returns* short-circuits the chain, so returning a 401 here
 * is all it takes. Without this handler `createWSApp` does not mount those
 * routes at all — an unauthenticated "push anything into any room" endpoint is
 * not something you get by forgetting to configure it.
 */
const requireAdminToken: DeminoHandler = (req: Request) => {
	if (req.headers.get("authorization") !== `Bearer ${ADMIN_TOKEN}`) {
		return new Response("Unauthorized", { status: 401 });
	}
};

const { app: wsApp, service } = createWSApp("/ws", [], {
	adapter: history,
	httpAuth: requireAdminToken,

	/**
	 * The handshake. Runs before *every* (re)connect with whatever the client's
	 * `auth()` returned, so a token refresh needs no special casing.
	 *
	 * Returning `null` closes the socket with `4001 AUTH_FAILED`, which the
	 * client treats as terminal — it stops retrying and emits `terminated`.
	 * That is exactly what we want for a malformed nickname: retrying a typo
	 * forever helps nobody.
	 */
	verify: (payload: unknown) => {
		const { nick, workspace, tab } = (payload ?? {}) as Record<string, unknown>;

		if (typeof nick !== "string" || !NICK_RE.test(nick)) return null;
		if (RESERVED_NICKS.includes(nick.toLowerCase())) return null;

		const namespace = typeof workspace === "string" && WORKSPACE_RE.test(workspace)
			? workspace.toLowerCase()
			: DEFAULT_WORKSPACE;

		// The client proposes a per-tab suffix and we honour it when it looks
		// sane, so the id survives a reconnect and presence does not churn.
		// Anything else gets one from us — never trust the payload.
		const suffix = typeof tab === "string" && TAB_RE.test(tab) ? tab : base36(4);

		return {
			// The suffix matters: client ids are unique *by contract* — when a
			// second connection authenticates with an id that is already
			// connected, the newcomer wins and the older socket is closed. That
			// is what makes reconnect-after-half-open recover instead of piling
			// up ghosts, but it would also make two tabs sharing a nickname
			// kick each other in an endless loop.
			clientId: `${nick}#${suffix}`,
			// The namespace is the isolation boundary: two workspaces can both
			// have a #general and never see each other.
			namespace,
			// Available to `allowBroadcast` (and any other hook) as `ctx.meta`.
			meta: { nick },
		};
	},

	/**
	 * Broadcast crosses the namespace boundary, so it **denies by default** and
	 * has to be opened deliberately. Here: only in one room. Try the client's
	 * "all workspaces" checkbox in #general to see the refusal arrive as a
	 * `nack` → `WSRemoteError`.
	 */
	allowBroadcast: (_ctx, room) => room === BROADCAST_ROOM,
});

/**
 * The example's own API. History is *not* a protocol feature — the server has
 * no replay and never resends an undelivered message — so a plain HTTP fetch on
 * join is how a chat gets its backlog.
 */
const apiApp = demino("/api");

apiApp.get(
	"/history/[namespace]/[room]",
	(_req: Request, _info: unknown, ctx: { params: Record<string, string> }) => ({
		messages: history.read(ctx.params.namespace, ctx.params.room),
	}),
);

/** Static file server for the built client. */
const siteApp = demino();
siteApp.static("/", PUBLIC_DIR);

if (import.meta.main) {
	Deno.serve(
		{
			port: PORT,
			onListen: (addr: Deno.NetAddr) => {
				logListenInfo(addr);
				const base = `http://localhost:${addr.port}`;
				console.log(`
  Open ${base} in two windows, with two different nicknames.

  Server-side injection (arrives with from: null, rendered as a system line):

    curl -X POST ${base}/ws/publish/${DEFAULT_WORKSPACE}/general \\
      -H "authorization: Bearer ${ADMIN_TOKEN}" \\
      -H "content-type: application/json" \\
      -d '{"kind":"chat","id":"srv-1","nick":"deploy-bot","text":"Deploy finished"}'

  Connection stats:  ${base}/ws/stats  (guarded by the same token)
`);
			},
		},
		deminoCompose([siteApp, apiApp, wsApp]),
	);

	// Close every socket with 1001 on Ctrl-C. Clients treat that as recoverable
	// and reconnect — restart the server and watch them come back.
	Deno.addSignalListener("SIGINT", () => {
		console.log("\nshutting down…");
		service.close().finally(() => Deno.exit(0));
	});
}

export { apiApp, history, service, siteApp, wsApp };
