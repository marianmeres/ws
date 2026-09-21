/**
 * Wire frame definitions.
 *
 * The central rule of this protocol: **the protocol frame type and the
 * application message type are separate things.** The protocol owns a small
 * closed set of frame types; `payload` is opaque and belongs entirely to the
 * application. Your payload may contain its own `type` field and nothing will
 * collide.
 *
 * The frames come in two layers. The **core** — `auth`/`hello`, `msg` in both
 * directions, `ack`/`nack`, `ping`/`pong`, `error` — is all a server must
 * implement. The **rooms extension** — `sub`, `unsub`, `pub`, `broadcast`,
 * `presence`, and the routing fields on a delivered `msg` — is opt-in.
 *
 * @module
 */

import type { FRAME, PRESENCE } from "./constants.ts";

/** Structured error detail carried by `nack` and `error` frames. */
export interface WSErrorInfo {
	/** Machine-readable code — see `ERROR_CODE`. */
	code: string;
	/** Human-readable explanation. Never parse this. */
	message: string;
}

/**
 * A message as delivered to application code — the `message` event fires
 * with one for every inbound `msg` frame.
 *
 * A `msg` frame minus its `type` field *is* a `WSMessage` — there is no
 * translation layer and no divergence between wire names and API names.
 *
 * Only `payload` is guaranteed. A direct message from the server (core) has
 * nothing else; a message delivered through a room (rooms extension) carries
 * every routing field — see {@link WSRoomMessage}. `room` tells them apart.
 */
export interface WSMessage<T = unknown> {
	/** Opaque application payload. */
	payload: T;
	/** Room the message was published to. Absent on a direct message. */
	room?: string;
	/** Namespace the message belongs to. Absent on a direct message. */
	namespace?: string;
	/**
	 * Originating client id, `null` when injected server-side. Absent on a
	 * direct message, which always comes from the server.
	 */
	from?: string | null;
	/** Server-assigned epoch milliseconds. Absent on a direct message. */
	timestamp?: number;
}

/**
 * A message delivered through a room (rooms extension). This is what room
 * handlers receive: every routing field is present.
 */
export interface WSRoomMessage<T = unknown> extends WSMessage<T> {
	/** Room the message was published to. */
	room: string;
	/** Namespace the message belongs to — always the receiver's own. */
	namespace: string;
	/**
	 * Originating client id, or `null` when the message was injected
	 * server-side (via the service API or the HTTP routes).
	 */
	from: string | null;
	/** Server-assigned epoch milliseconds. */
	timestamp: number;
}

/** Kind of presence change. */
export type PresenceEventType = (typeof PRESENCE)[keyof typeof PRESENCE];

/**
 * A membership change in a room the client subscribed to with presence
 * enabled (rooms extension).
 *
 * `sync` carries the full snapshot and is emitted on every (re)subscribe —
 * crucially including after a reconnect, when membership may have changed
 * completely while the client was away.
 */
export interface WSPresenceEvent {
	/** Whether this is a full snapshot or a join/leave delta. */
	event: PresenceEventType;
	/** Room the membership change happened in. */
	room: string;
	/** Namespace the room belongs to. */
	namespace: string;
	/** The client that joined or left. `null` for `sync`. */
	clientId: string | null;
	/** Full membership after applying this event. */
	members: string[];
	/** Server-assigned epoch milliseconds. */
	timestamp: number;
}

/** A single room subscription request (rooms extension). */
export interface SubRequest {
	/** Room name to join. */
	room: string;
	/** Track membership and deliver `presence` frames for this room. */
	presence?: boolean;
}

/** Frames sent by the client. */
export type ClientFrame =
	| {
		type: typeof FRAME.AUTH;
		/**
		 * Not sent since protocol 2 and never acknowledged. Kept optional so a
		 * server keeps accepting a protocol-1 client that still sends it.
		 */
		id?: string;
		protocol: number;
		payload: unknown;
		/** Preferred client id — the server may override it. */
		clientId?: string;
		/** Requested namespace — sent only when the application chose one. */
		namespace?: string;
	}
	| {
		type: typeof FRAME.MSG;
		/** Present only when the sender awaits an `ack`/`nack`. */
		id?: string;
		payload: unknown;
	}
	| { type: typeof FRAME.PING }
	| { type: typeof FRAME.SUB; id: string; rooms: SubRequest[] }
	| { type: typeof FRAME.UNSUB; id: string; rooms: string[] }
	| {
		type: typeof FRAME.PUB;
		id: string;
		room: string;
		namespace?: string;
		payload: unknown;
	}
	| { type: typeof FRAME.BROADCAST; id: string; room: string; payload: unknown };

/** Frames sent by the server. */
export type ServerFrame =
	| {
		type: typeof FRAME.HELLO;
		protocol: number;
		/** Assigned client id. Optional in the core; always sent with rooms. */
		clientId?: string;
		/** Assigned namespace. Optional in the core; always sent with rooms. */
		namespace?: string;
	}
	| {
		type: typeof FRAME.ACK;
		id: string;
		/** Delivery count, on `pub`/`broadcast` acks only. */
		recipients?: number;
		/** The server's reply, on `msg` acks only. Any JSON value. */
		payload?: unknown;
	}
	| { type: typeof FRAME.NACK; id: string; error: WSErrorInfo }
	| ({ type: typeof FRAME.MSG } & WSMessage)
	| { type: typeof FRAME.PONG }
	| { type: typeof FRAME.ERROR; error: WSErrorInfo }
	| ({ type: typeof FRAME.PRESENCE } & WSPresenceEvent);

/** Any frame, in either direction. */
export type WSFrame = ClientFrame | ServerFrame;

/** Result of a successful `publish()` / `broadcast()` (rooms extension). */
export interface WSPublishResult {
	/**
	 * Sockets the message was handed to **on the receiving server instance**.
	 *
	 * Best-effort telemetry, never a delivery guarantee — and it stays
	 * instance-local once a distributed adapter is in play.
	 */
	recipients: number;
}

/**
 * What the client proposed in its `auth` frame. Hints, not facts — they are
 * whatever the socket sent, so validate them before honouring them.
 */
export interface WSRequestedIdentity {
	/** The claimed client id, when the frame carried a usable one. */
	clientId?: string;
	/** The requested namespace, or the default when the frame carried none. */
	namespace: string;
}

/** Outcome of the server's `verify()` hook. */
export interface AuthResult {
	/** Assign a specific client id. Defaults to a generated one. */
	clientId?: string;
	/** Force the connection's namespace, overriding the client's request. */
	namespace?: string;
	/** Arbitrary data to associate with the connection (available to hooks). */
	meta?: Record<string, unknown>;
}

/** Encodes an outgoing frame for the wire. */
export type WSEncoder = (frame: WSFrame) => string | ArrayBufferView | ArrayBuffer;

/** Decodes an inbound wire message into a frame. */
export type WSDecoder = (raw: string | ArrayBuffer) => WSFrame;
