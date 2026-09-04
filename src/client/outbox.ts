/**
 * Outbox + pending-ack registry.
 *
 * This is where three decisions collide: awaited acks, buffering while
 * disconnected, and retrying forever. Combined naively they produce promises
 * that pend indefinitely, so every tracked frame carries **one timeout
 * spanning queue + flight + ack** — not an ack-only timeout.
 *
 * Written by hand rather than on top of `@marianmeres/batch`: that flusher
 * triggers on interval/count, whereas this one triggers on connection state.
 * Bending it into shape costs more than the little code it saves.
 *
 * @module
 */

import type { ClientFrame, WSPublishResult } from "../protocol/frames.ts";
import { WSOutboxDropError, WSTimeoutError } from "../protocol/errors.ts";

interface PendingSend {
	frame: ClientFrame;
	resolve: (result: WSPublishResult) => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
	/** Still waiting in the queue (true) or already transmitted (false). */
	queued: boolean;
}

/** Configuration for {@link Outbox}. */
export interface OutboxOptions {
	/** Max frames buffered while disconnected. `0` disables buffering. */
	maxSize: number;
	/** Overall deadline per frame, covering queue + flight + ack. */
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
	 * @param id - correlation id, matched against the server's ack/nack
	 * @param frame - the frame itself, retained so it can be flushed later
	 * @param queued - `true` to buffer it, `false` if it is going out now
	 */
	track(id: string, frame: ClientFrame, queued: boolean): Promise<WSPublishResult> {
		return new Promise<WSPublishResult>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#discard(id);
				reject(new WSTimeoutError(this.#options.sendTimeout));
			}, this.#options.sendTimeout);

			this.#pending.set(id, { frame, resolve, reject, timer, queued });

			if (queued) {
				this.#queue.push(id);
				this.#applyCap();
			}
		});
	}

	/**
	 * Marks every buffered frame as transmitted and returns them in FIFO order.
	 * They stay pending — they are awaiting acks now, not a connection.
	 */
	drain(): ClientFrame[] {
		const frames: ClientFrame[] = [];
		for (const id of this.#queue) {
			const entry = this.#pending.get(id);
			if (!entry) continue;
			entry.queued = false;
			frames.push(entry.frame);
		}
		this.#queue = [];
		return frames;
	}

	/** Resolves a pending frame — the server acked it. */
	settle(id: string, recipients: number): boolean {
		const entry = this.#discard(id);
		if (!entry) return false;
		entry.resolve({ recipients });
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
