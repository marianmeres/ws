/**
 * Wire frame definitions.
 *
 * The central rule of this protocol: **the protocol frame type and the
 * application message type are separate things.** The protocol owns a small
 * closed set of frame types; `payload` is opaque and belongs entirely to the
 * application. Your payload may contain its own `type` field and nothing will
 * collide.
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
 * A message as delivered to application code.
 *
 * A `msg` frame minus its `type` field *is* a `WSMessage` — there is no
 * translation layer and no divergence between wire names and API names.
 */
export interface WSMessage<T = unknown> {
	/** Room the message was published to. */
	room: string;
	/** Namespace the message belongs to. */
	namespace: string;
	/**
	 * Originating client id, or `null` when the message was injected
	 * server-side (via the service API or the HTTP routes).
	 */
	from: string | null;
	/** Opaque application payload. */
	payload: T;
	/** Server-assigned epoch milliseconds. */
	timestamp: number;
}

/** Kind of presence change. */
export type PresenceEventType = (typeof PRESENCE)[keyof typeof PRESENCE];

/**
 * A membership change in a room the client subscribed to with presence
 * enabled.
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

/** A single room subscription request. */
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
		id: string;
		protocol: number;
		payload: unknown;
		/** Preferred client id — the server may override it. */
		clientId?: string;
		namespace?: string;
	}
	| { type: typeof FRAME.SUB; id: string; rooms: SubRequest[] }
	| { type: typeof FRAME.UNSUB; id: string; rooms: string[] }
	| {
		type: typeof FRAME.PUB;
		id: string;
		room: string;
		namespace?: string;
		payload: unknown;
	}
	| { type: typeof FRAME.BROADCAST; id: string; room: string; payload: unknown }
	| { type: typeof FRAME.PING };

/** Frames sent by the server. */
export type ServerFrame =
	| {
		type: typeof FRAME.HELLO;
		clientId: string;
		namespace: string;
		protocol: number;
	}
	| { type: typeof FRAME.ACK; id: string; recipients?: number }
	| { type: typeof FRAME.NACK; id: string; error: WSErrorInfo }
	| ({ type: typeof FRAME.MSG } & WSMessage)
	| ({ type: typeof FRAME.PRESENCE } & WSPresenceEvent)
	| { type: typeof FRAME.PONG }
	| { type: typeof FRAME.ERROR; error: WSErrorInfo };

/** Any frame, in either direction. */
export type WSFrame = ClientFrame | ServerFrame;

/** Result of a successful `publish()` / `broadcast()`. */
export interface WSPublishResult {
	/**
	 * Sockets the message was handed to **on the receiving server instance**.
	 *
	 * Best-effort telemetry, never a delivery guarantee — and it stays
	 * instance-local once a distributed adapter is in play.
	 */
	recipients: number;
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
