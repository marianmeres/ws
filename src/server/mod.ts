/**
 * Reference server — a demino app implementing the wire protocol.
 *
 * Deno-first: it uses `Deno.upgradeWebSocket`. Import it from
 * `@marianmeres/ws/server` so `demino` never reaches a browser bundle.
 *
 * @example
 * ```ts
 * import { createWSApp } from "@marianmeres/ws/server";
 *
 * const { app, service } = createWSApp("/ws", [], {
 *     verify: async (payload) => {
 *         const user = await auth((payload as any)?.token);
 *         return user ? { clientId: user.id, namespace: user.orgId } : null;
 *     },
 * });
 * Deno.serve(app);
 * ```
 *
 * @module
 */

import {
	type Demino,
	demino,
	type DeminoHandler,
	type DeminoOptions,
} from "@marianmeres/demino";
import { WSService, type WSServiceOptions } from "./service.ts";

export * from "./adapters/abstract.ts";
export * from "./adapters/local.ts";
export type { WSRequestedIdentity } from "../protocol/frames.ts";
export {
	type WSConnectionContext,
	WSService,
	type WSServiceOptions,
	type WSStats,
} from "./service.ts";

/** Configuration for {@link createWSApp}. */
export interface WSAppOptions extends WSServiceOptions {
	/**
	 * Guards every HTTP route except the upgrade: `GET /stats`,
	 * `POST /publish` and `POST /broadcast`.
	 *
	 * **Without it none of them are mounted at all.** They are a separate
	 * trust boundary from the WebSocket `verify` hook — mounting an
	 * unauthenticated "push anything into any room" endpoint by default would
	 * be a genuine vulnerability, and `/stats` names every connected namespace,
	 * so exposing them has to be a deliberate act.
	 */
	httpAuth?: DeminoHandler;
	/**
	 * Passed straight through to `demino()`. HTTP-level concerns (access
	 * logging, error handling, proxy trust) are configured here, separately
	 * from the WebSocket `logger`.
	 */
	deminoOptions?: DeminoOptions;
}

/** What {@link createWSApp} returns. */
export interface WSApp {
	/** The demino app. Mount it, or serve it directly. */
	app: Demino;
	/** Inject messages from server-side code, read stats, shut down. */
	service: WSService;
}

/**
 * Creates a mountable WebSocket server.
 *
 * Routes, relative to `mountPath`:
 *
 * | Method | Path                          | Notes                                 |
 * | ------ | ----------------------------- | ------------------------------------- |
 * | GET    | `/`                           | WebSocket upgrade                     |
 * | GET    | `/stats`                      | Requires `httpAuth`, else not mounted |
 * | POST   | `/publish/[namespace]/[room]` | Requires `httpAuth`, else not mounted |
 * | POST   | `/broadcast/[room]`           | Requires `httpAuth`, else not mounted |
 *
 * The returned `service` is the one the app is wired to, so server-side
 * injection and the sockets share a single registry.
 *
 * @param mountPath - demino mount path. Default `/ws`
 * @param middlewares - applied to every route on this app
 * @param options - see {@link WSAppOptions}
 * @returns the demino app and its service
 *
 * @example
 * ```ts
 * const { app, service } = createWSApp("/ws", [], {
 *     verify: (payload) => ({ clientId: String(payload) }),
 * });
 *
 * Deno.serve(app);
 * await service.publish("general", { text: "hi" });
 * ```
 */
export function createWSApp(
	mountPath: string = "/ws",
	middlewares: DeminoHandler[] = [],
	options: WSAppOptions = {},
): WSApp {
	const service = new WSService(options);
	const app = demino(mountPath, middlewares, options.deminoOptions);
	const { httpAuth } = options;

	app.get("/", (req: Request) => {
		// A plain GET here is almost always a misconfigured client, so say so
		// rather than letting upgradeWebSocket throw an opaque error.
		if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
			return new Response("Expected a WebSocket upgrade request", {
				status: 426,
				headers: { upgrade: "websocket" },
			});
		}
		return service.handleUpgrade(req);
	});

	if (httpAuth) {
		// Counts only, never client ids — but it does name every connected
		// namespace, which in a multi-tenant deployment enumerates the tenants
		// that are online. Use `service.stats()` for unguarded in-process reads.
		app.get("/stats", httpAuth, () => service.stats());

		app.post(
			"/publish/[namespace]/[room]",
			httpAuth,
			async (
				req: Request,
				_info: unknown,
				ctx: { params: Record<string, string> },
			) => {
				const payload = await readJson(req);
				const recipients = await service.publish(
					ctx.params.room,
					payload,
					ctx.params.namespace,
				);
				return { ok: true, recipients };
			},
		);

		app.post(
			"/broadcast/[room]",
			httpAuth,
			async (
				req: Request,
				_info: unknown,
				ctx: { params: Record<string, string> },
			) => {
				const payload = await readJson(req);
				const recipients = await service.broadcast(ctx.params.room, payload);
				return { ok: true, recipients };
			},
		);
	}

	return { app, service };
}

async function readJson(req: Request): Promise<unknown> {
	try {
		return await req.json();
	} catch {
		// demino turns a `status` on a thrown error into that response code;
		// without it a client's malformed body would be reported as a 500.
		throw Object.assign(new Error("Invalid JSON body"), { status: 400 });
	}
}
