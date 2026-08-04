/**
 * Wire protocol constants shared by client and server.
 *
 * This module is intentionally dependency free — it is the single source of
 * truth both sides import, which is what keeps them from drifting apart.
 *
 * @module
 */

/**
 * Protocol version, announced by the server in its `hello` frame.
 *
 * It costs nothing today and it is the only thing that makes a breaking
 * protocol change survivable later.
 */
export const PROTOCOL_VERSION = 1;

/** Namespace used when the client does not specify one. */
export const DEFAULT_NAMESPACE = "default";

/**
 * Frame type discriminators.
 *
 * Note these are *protocol* types and have nothing to do with whatever the
 * application puts inside `payload` — the payload is opaque and is never
 * inspected nor mutated by this library.
 */
export const FRAME = {
	// client -> server
	AUTH: "auth",
	SUB: "sub",
	UNSUB: "unsub",
	PUB: "pub",
	BROADCAST: "broadcast",
	PING: "ping",
	// server -> client
	HELLO: "hello",
	ACK: "ack",
	NACK: "nack",
	MSG: "msg",
	PRESENCE: "presence",
	PONG: "pong",
	ERROR: "error",
} as const;

/**
 * WebSocket close codes.
 *
 * The `4xxx` range is reserved for application use by RFC 6455.
 */
export const CLOSE = {
	/** Normal closure. Server-sent means "restarting" — the client reconnects. */
	NORMAL: 1000,
	GOING_AWAY: 1001,
	/** No close frame received. The everyday network drop. */
	ABNORMAL: 1006,
	INTERNAL_ERROR: 1011,

	/** Authentication rejected. Terminal — retrying cannot help. */
	AUTH_FAILED: 4001,
	/** Client did not send an `auth` frame in time. Recoverable. */
	AUTH_TIMEOUT: 4002,
	/** Authenticated but not permitted. Terminal. */
	FORBIDDEN: 4003,
	/** Connection went silent and was reaped. Recoverable. */
	IDLE_TIMEOUT: 4008,
	/** Too many frames per second. Recoverable, with a longer backoff floor. */
	RATE_LIMITED: 4009,
	/** Frame exceeded `maxFrameSize`. Recoverable. */
	FRAME_TOO_LARGE: 4013,
	/** Malformed frame or codec mismatch. Recoverable (but likely a config bug). */
	PROTOCOL_ERROR: 4400,
	/** Local, client-initiated teardown. Never reconnects by definition. */
	CLIENT_GONE: 4900,
} as const;

/**
 * Close codes after which reconnecting is pointless.
 *
 * Everything *not* in this list reconnects — including a server-sent `1000`,
 * because a graceful shutdown or rolling deploy is exactly when clients must
 * come back.
 */
export const DEFAULT_TERMINAL_CLOSE_CODES: readonly number[] = [
	CLOSE.AUTH_FAILED,
	CLOSE.FORBIDDEN,
];

/** Application-level error codes carried in `nack`/`error` frames. */
export const ERROR_CODE = {
	UNAUTHORIZED: "unauthorized",
	FORBIDDEN: "forbidden",
	BAD_REQUEST: "bad_request",
	RATE_LIMITED: "rate_limited",
	INTERNAL: "internal",
} as const;

/** Presence event kinds. */
export const PRESENCE = {
	/** Full membership snapshot, sent on every (re)subscribe. */
	SYNC: "sync",
	JOIN: "join",
	LEAVE: "leave",
} as const;
