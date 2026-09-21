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
 *
 * Version 2 split the protocol into a required **core** (handshake, messages
 * in both directions, heartbeat) and an optional **rooms extension**
 * (subscriptions, publishing, presence, broadcast), so a server that only
 * needs a data channel implements the core and nothing else.
 */
export const PROTOCOL_VERSION = 2;

/** Namespace used when the client does not specify one. */
export const DEFAULT_NAMESPACE = "default";

/**
 * Frame type discriminators.
 *
 * Note these are *protocol* types and have nothing to do with whatever the
 * application puts inside `payload` — the payload is opaque and is never
 * inspected nor mutated by this library.
 *
 * Every server implements the **core** frames. The **rooms extension**
 * frames are sent only when the application uses rooms (`subscribe()`,
 * `publish()`, `broadcast()`, presence), so a server that does not support
 * them never sees one.
 */
export const FRAME = {
	// core, client -> server
	/** Handshake. Always the first frame; carries the auth payload. */
	AUTH: "auth",
	/** Liveness probe. Answered with `pong`. */
	PING: "ping",

	// core, server -> client
	/** Handshake accepted; carries the protocol version (and, with rooms, identity). */
	HELLO: "hello",
	/** Positive acknowledgement of a frame that carried an `id`; may carry a reply. */
	ACK: "ack",
	/** Negative acknowledgement; carries a `WSErrorInfo`. */
	NACK: "nack",
	/** Reply to `ping`. */
	PONG: "pong",
	/** Uncorrelated error — not tied to any request id. */
	ERROR: "error",

	// core, both directions
	/**
	 * A message. Client -> server it is addressed to the server itself;
	 * server -> client it is either a direct message (core) or a room delivery
	 * (rooms extension, recognisable by its `room` field).
	 */
	MSG: "msg",

	// rooms extension, client -> server
	/** Join one or more rooms, optionally with presence. */
	SUB: "sub",
	/** Leave one or more rooms. */
	UNSUB: "unsub",
	/** Publish into a room within the connection's own namespace. */
	PUB: "pub",
	/** Publish into a room across every namespace. Gated server-side. */
	BROADCAST: "broadcast",

	// rooms extension, server -> client
	/** A membership change in a presence-enabled room. */
	PRESENCE: "presence",
} as const;

/**
 * WebSocket close codes.
 *
 * The `4xxx` range is reserved for application use by RFC 6455.
 */
export const CLOSE = {
	/** Normal closure. Server-sent means "restarting" — the client reconnects. */
	NORMAL: 1000,
	/** Endpoint going away (shutdown, navigation). Recoverable. */
	GOING_AWAY: 1001,
	/** No close frame received. The everyday network drop. */
	ABNORMAL: 1006,
	/** Unexpected server-side condition. Recoverable. */
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
	/** Operation attempted before the handshake completed. */
	UNAUTHORIZED: "unauthorized",
	/** Authenticated, but not permitted — e.g. a denied broadcast. */
	FORBIDDEN: "forbidden",
	/** Malformed or nonsensical frame. */
	BAD_REQUEST: "bad_request",
	/** Frame rate cap exceeded. */
	RATE_LIMITED: "rate_limited",
	/**
	 * A frame type this server does not implement — typically a rooms frame
	 * sent to a core-only server, or a `msg` to a server with no handler.
	 */
	UNSUPPORTED: "unsupported",
	/** Unexpected server-side failure. */
	INTERNAL: "internal",
} as const;

/** Presence event kinds (rooms extension). */
export const PRESENCE = {
	/** Full membership snapshot, sent on every (re)subscribe. */
	SYNC: "sync",
	/** A client joined the room. Delta — `clientId` is the joiner. */
	JOIN: "join",
	/** A client left the room. Delta — `clientId` is the leaver. */
	LEAVE: "leave",
} as const;
