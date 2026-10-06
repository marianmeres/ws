/**
 * Outbox + pending-ack registry.
 *
 * This is where three decisions collide: awaited acks, buffering while
 * disconnected, and retrying forever. Combined naively they produce promises
 * that pend indefinitely, so every tracked frame carries **one timeout
 * spanning queue + flight + ack** — not an ack-only timeout.
 *
 * Not every frame awaits an ack. A `msg` sent without `{ ack: true }` is
 * complete the moment it is written to the socket; it passes through here
 * only so that, while offline, it is buffered and bounded like everything else.
 *
 * Written by hand rather than on top of `@marianmeres/batch`: that flusher
 * triggers on interval/count, whereas this one triggers on connection state.
 * Bending it into shape costs more than the little code it saves.
 *
 * @module
 */

import type { ClientFrame } from "../protocol/frames.ts";
import { WSOutboxDropError, WSTimeoutError } from "../protocol/errors.ts";

/** What a settled send resolves with — the parts of the `ack` the caller needs. */
export interface OutboxResult {
	/** Delivery count from a `pub`/`broadcast` ack; `0` otherwise. */
	recipients: number;
	/** The server's reply from a `msg` ack; `undefined` otherwise. */
	payload?: unknown;
}

/** A tracked frame together with the key it is tracked under. */
export interface OutboxEntry {
	/** The key — the frame's wire `id` when it has one, a local one otherwise. */
	id: string;
	/** The frame itself. */
	frame: ClientFrame;
}

interface PendingSend {
	frame: ClientFrame;
	resolve: (result: OutboxResult) => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
	/** Still waiting in the queue (true) or already transmitted (false). */
	queued: boolean;
	/** Settled by an ack (true), or complete once transmitted (false). */
	awaitAck: boolean;
}

/** Configuration for {@link Outbox}. */
export interface OutboxOptions {
	/** Max frames buffered while disconnected. `0` disables buffering. */
	maxSize: number;
	/**
	 * Default deadline per frame, covering queue + flight + ack. A frame may
	 * override it when tracked.
	 */
	sendTimeout: number;
	/** Called with frames evicted because the queue was full. */
	onDrop?: (frames: ClientFrame[]) => void;
}

/**
 * Tracks frames awaiting acknowledgement, and buffers those that could not be
 * sent yet.
 */
export class Outbox {
	#pending = new Map<string, PendingSend>();
	/** Ids of not-yet-transmitted frames, in FIFO order. */
	#queue: string[] = [];
	#dropped = 0;
	#options: OutboxOptions;

	constructor(options: OutboxOptions) {
		this.#options = options;
	}

	/** Frames buffered but not yet transmitted. */
	get queuedCount(): number {
		return this.#queue.length;
	}

	/** Frames awaiting an ack, transmitted or not. */
	get pendingCount(): number {
		return this.#pending.size;
	}

	/** Total frames evicted because the queue was full, for the lifetime. */
	get droppedCount(): number {
		return this.#dropped;
	}

	/**
	 * Registers a frame and returns the promise the caller awaits.
	 *
	 * @param id - correlation id, matched against the server's ack/nack; for a
	 * frame that awaits no ack, any locally unique key
	 * @param frame - the frame itself, retained so it can be flushed later
	 * @param queued - `true` to buffer it, `false` if it is going out now
	 * @param awaitAck - `false` when the frame is complete once transmitted —
	 * see {@link transmitted}
	 * @param timeout - deadline for this frame in ms, replacing the default
	 * `sendTimeout`; same span (queue + flight + ack)
	 */
	track(
		id: string,
		frame: ClientFrame,
		queued: boolean,
		awaitAck = true,
		timeout: number = this.#options.sendTimeout,
	): Promise<OutboxResult> {
		return new Promise<OutboxResult>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#discard(id);
				reject(new WSTimeoutError(timeout));
			}, timeout);

			this.#pending.set(id, { frame, resolve, reject, timer, queued, awaitAck });

			if (queued) {
				this.#queue.push(id);
				this.#applyCap();
			}
		});
	}

	/**
	 * Takes every buffered frame out of the queue and returns them in FIFO
	 * order, for the caller to write and then report via {@link transmitted}
	 * or {@link fail}. They stay pending — awaiting acks now, not a connection.
	 */
	drain(): OutboxEntry[] {
		const entries: OutboxEntry[] = [];
		for (const id of this.#queue) {
			const entry = this.#pending.get(id);
			if (!entry) continue;
			entry.queued = false;
			entries.push({ id, frame: entry.frame });
		}
		this.#queue = [];
		return entries;
	}

	/**
	 * Reports a frame as written to the socket. One that awaits no ack is
	 * complete and resolves here; one that does keeps waiting for its ack.
	 */
	transmitted(id: string): void {
		const entry = this.#pending.get(id);
		if (!entry || entry.awaitAck) return;
		this.#discard(id);
		entry.resolve({ recipients: 0 });
	}

	/**
	 * Resolves a pending frame — the server acked it.
	 *
	 * @param recipients - the ack's delivery count, `0` when it carried none
	 * @param payload - the ack's reply, when it carried one
	 */
	settle(id: string, recipients: number, payload?: unknown): boolean {
		const entry = this.#discard(id);
		if (!entry) return false;
		entry.resolve(payload === undefined ? { recipients } : { recipients, payload });
		return true;
	}

	/** Rejects a single pending frame. */
	fail(id: string, error: Error): boolean {
		const entry = this.#discard(id);
		if (!entry) return false;
		entry.reject(error);
		return true;
	}

	/**
	 * Rejects everything. Used on terminal close and on dispose — without it,
	 * callers would wait out `sendTimeout` for an answer that can never come.
	 */
	failAll(error: Error): void {
		const entries = [...this.#pending.values()];
		this.#pending.clear();
		this.#queue = [];
		for (const entry of entries) {
			clearTimeout(entry.timer);
			entry.reject(error);
		}
	}

	/**
	 * Settles every already-transmitted frame, leaving the queued ones to wait
	 * for the next connection.
	 *
	 * Used when the socket closes: the ack can no longer arrive, because the
	 * socket that would have carried it is gone and the next one is a new
	 * session. Waiting out `sendTimeout` would only delay an answer that is
	 * already known.
	 *
	 * @param decide - per frame: the error to reject with, or `null` to resolve
	 * it with no recipients
	 */
	settleInFlight(decide: (frame: ClientFrame) => Error | null): void {
		for (const [id, entry] of [...this.#pending]) {
			if (entry.queued) continue;
			const error = decide(entry.frame);
			this.#discard(id);
			if (error) entry.reject(error);
			else entry.resolve({ recipients: 0 });
		}
	}

	#discard(id: string): PendingSend | undefined {
		const entry = this.#pending.get(id);
		if (!entry) return undefined;
		clearTimeout(entry.timer);
		this.#pending.delete(id);
		if (entry.queued) {
			const at = this.#queue.indexOf(id);
			if (at !== -1) this.#queue.splice(at, 1);
		}
		return entry;
	}

	/**
	 * Enforces `maxSize` by dropping the **oldest** frames.
	 *
	 * Dropping the oldest keeps the freshest state, which is what almost every
	 * real-time use wants. Each victim's promise rejects immediately, so a
	 * caller always gets an answer rather than silently losing a message.
	 */
	#applyCap(): void {
		const max = this.#options.maxSize;
		const evicted: ClientFrame[] = [];

		while (this.#queue.length > max) {
			const id = this.#queue.shift();
			if (id === undefined) break;
			const entry = this.#pending.get(id);
			if (!entry) continue;
			clearTimeout(entry.timer);
			this.#pending.delete(id);
			this.#dropped++;
			evicted.push(entry.frame);
			entry.reject(new WSOutboxDropError());
		}

		if (evicted.length) this.#options.onDrop?.(evicted);
	}
}
