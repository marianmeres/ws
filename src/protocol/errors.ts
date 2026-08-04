/**
 * Typed errors.
 *
 * Every rejection this library produces is one of these, so callers can branch
 * on `instanceof` rather than string-matching messages.
 *
 * @module
 */

import type { WSErrorInfo } from "./frames.ts";

/** Base class for everything thrown by this library. */
export class WSError extends Error {
	constructor(message: string) {
		super(message);
		this.name = new.target.name;
	}
}

/**
 * The connection reached a terminal state and will not retry.
 *
 * The only path by which an infinitely-retrying client gives up, which is why
 * it is logged at error level and rejects any pending `connect()`.
 */
export class WSTerminatedError extends WSError {
	constructor(readonly code: number, readonly reason: string) {
		super(`Connection terminated (${code})${reason ? `: ${reason}` : ""}`);
	}
}

/**
 * `connectTimeout` elapsed before the first successful connect.
 *
 * Note the client keeps retrying in the background — this rejection bounds
 * *your await*, not the connection attempt.
 */
export class WSConnectTimeoutError extends WSError {
	constructor(ms: number) {
		super(`Not connected within ${ms}ms (still retrying in background)`);
	}
}

/** `sendTimeout` elapsed while queued, in flight, or awaiting an ack. */
export class WSTimeoutError extends WSError {
	constructor(ms: number) {
		super(`No acknowledgement within ${ms}ms`);
	}
}

/** The frame was evicted from a full outbox before it could be sent. */
export class WSOutboxDropError extends WSError {
	constructor() {
		super("Dropped from outbox (capacity reached while disconnected)");
	}
}

/** The server rejected the operation with a `nack`. */
export class WSRemoteError extends WSError {
	readonly code: string;
	constructor(info: WSErrorInfo) {
		super(info.message);
		this.code = info.code;
	}
}

/** The client was disposed while the operation was pending. */
export class WSDisposedError extends WSError {
	constructor() {
		super("Client disposed");
	}
}

/** Sending was attempted while disconnected with buffering disabled. */
export class WSNotConnectedError extends WSError {
	constructor() {
		super("Not connected (outbox buffering is disabled)");
	}
}
