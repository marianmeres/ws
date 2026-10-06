/**
 * `@marianmeres/ws` — a WebSocket client with auto-reconnect, half-open
 * detection and buffered sends, plus optional namespaces, rooms and presence.
 *
 * Two ways to use it, freely mixed on one connection:
 *
 * - **Messages** — `send()` to the server, `on("message")` to receive. The
 *   server only has to implement the small core of the protocol.
 * - **Rooms** — `subscribe()` / `publish()` / `broadcast()` and presence, for
 *   a server that relays between clients (the rooms extension).
 *
 * The client is dependency-light and runs in browsers, Deno, Node 22+, Bun and
 * Workers — `WebSocket` is a global in all of them, so there is no polyfill and
 * no transport dependency.
 *
 * The reference server lives behind a separate entry point,
 * `@marianmeres/ws/server`, so `demino` never lands in a browser bundle.
 *
 * @example Messages
 * ```ts
 * import { createWSClient } from "@marianmeres/ws";
 *
 * const ws = createWSClient({ url: "/ws", auth: () => session.token });
 *
 * ws.on("message", (msg) => console.log(msg.payload));
 *
 * ws.send({ op: "typing" }); // fire-and-forget
 * const doc = await ws.send({ op: "load", id: 42 }, { ack: true }); // the reply
 * ```
 *
 * @example Rooms
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
	type WSCloseInfo,
	type WSConnectionState,
	type WSEvents,
	type WSSendOptions,
	type WSState,
} from "./client/ws-client.ts";

export type { MessageHandler, PresenceHandler } from "./client/rooms.ts";

export { backoffDelay } from "./client/backoff.ts";

export * from "./protocol/mod.ts";
