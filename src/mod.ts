/**
 * `@marianmeres/ws` — a WebSocket client with namespaces, rooms, presence and
 * auto-reconnect.
 *
 * The client is dependency-light and runs in browsers, Deno, Node 22+, Bun and
 * Workers — `WebSocket` is a global in all of them, so there is no polyfill and
 * no transport dependency.
 *
 * The reference server lives behind a separate entry point,
 * `@marianmeres/ws/server`, so `demino` never lands in a browser bundle.
 *
 * @example
 * ```ts
 * import { createWSClient } from "@marianmeres/ws";
 *
 * const ws = createWSClient({ url: "/ws", namespace: "org-123" });
 *
 * const unsub = await ws.subscribe("chat", (msg) => {
 *     console.log(msg.from, msg.payload);
 * });
 *
 * await ws.publish("chat", { text: "hello" });
 * ```
 *
 * @module
 */

export {
	createWSClient,
	type SubscribeOptions,
	WSClient,
	type WSClientOptions,
	type WSConnectionState,
	type WSEvents,
	type WSState,
} from "./client/ws-client.ts";

export type { MessageHandler, PresenceHandler } from "./client/rooms.ts";

export { backoffDelay } from "./client/backoff.ts";

export * from "./protocol/mod.ts";
