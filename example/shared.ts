/**
 * The **application** protocol — what this chat puts inside `payload`.
 *
 * This file is the whole point of the "protocol type is not application type"
 * rule: `@marianmeres/ws` owns a closed set of frame types (`pub`, `msg`,
 * `presence`, …) and treats `payload` as opaque. Everything below is ours, it
 * lives on top, and the library never looks at it.
 *
 * Imported by **both** sides — the server uses it to decide what is worth
 * keeping in history, the client uses it to render. Sharing one module is what
 * keeps the two ends from drifting.
 *
 * @module
 */

/** Rooms the example offers. Purely an app-level list; rooms are created on demand. */
export const ROOMS = ["general", "random", "announcements"] as const;

/** Room the server's `allowBroadcast` gate permits. See `example/server.ts`. */
export const BROADCAST_ROOM = "announcements";

/** Room joined first. */
export const DEFAULT_ROOM = ROOMS[0];

/** Namespace used when the login form is left empty. */
export const DEFAULT_WORKSPACE = "acme";

/** How many chat messages the server keeps per (namespace, room). */
export const HISTORY_LIMIT = 50;

/** A chat line. `id` is ours — the wire protocol has no message ids. */
export interface ChatPayload {
	kind: "chat";
	/** App-level id, used to de-duplicate history against live messages. */
	id: string;
	/** Display name. Distinct from the client id, which carries a suffix. */
	nick: string;
	text: string;
}

/** Ephemeral "still typing" ping. Never stored, never acknowledged visually. */
export interface TypingPayload {
	kind: "typing";
	nick: string;
}

/** Everything this app sends through `publish()` / `broadcast()`. */
export type AppPayload = ChatPayload | TypingPayload;

function isRecord(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null;
}

/** Narrows an opaque payload to a chat line. */
export function isChatPayload(payload: unknown): payload is ChatPayload {
	return (
		isRecord(payload) &&
		payload.kind === "chat" &&
		typeof payload.text === "string" &&
		typeof payload.nick === "string"
	);
}

/** Narrows an opaque payload to a typing ping. */
export function isTypingPayload(payload: unknown): payload is TypingPayload {
	return (
		isRecord(payload) && payload.kind === "typing" &&
		typeof payload.nick === "string"
	);
}

/**
 * Nickname rules.
 *
 * The server enforces these in `verify` (a rejected nick closes the socket with
 * `4001 AUTH_FAILED`, which is terminal — the client must not retry). The
 * client applies the same regex before connecting purely so the common typo
 * gets an inline message instead of a round trip.
 */
export const NICK_RE = /^[a-z0-9][a-z0-9_-]{1,15}$/i;

/** Same idea for the workspace, which becomes the connection's namespace. */
export const WORKSPACE_RE = /^[a-z0-9][a-z0-9_-]{1,23}$/i;

/**
 * Shape of the per-tab suffix the client asks for, appended to the nickname to
 * form the client id. Validated server-side like everything else in the auth
 * payload — it is client-supplied data, so it does not get to be trusted.
 */
export const TAB_RE = /^[a-z0-9]{4,8}$/;

/** Nicks the server refuses, so `from: null` server pushes stay unambiguous. */
export const RESERVED_NICKS: readonly string[] = ["server", "system", "admin"];
