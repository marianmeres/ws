/**
 * Application-level ping/pong liveness probe.
 *
 * Half-open TCP is *the* defining WebSocket failure: the peer vanishes, no FIN
 * ever arrives, `onclose` never fires, and the client sits there looking
 * connected while receiving nothing. Browsers expose no protocol-level ping,
 * so detecting it requires an application-level probe.
 *
 * The interval doubles as proxy keep-alive — nginx and friends drop idle
 * sockets at 60s by default, so the 25s default sits comfortably under that.
 *
 * @module
 */

import { createTicker, type Ticker } from "@marianmeres/ticker";

/** Configuration for {@link Heartbeat}. */
export interface HeartbeatOptions {
	/** Ping cadence in ms. `0` disables the heartbeat entirely. */
	interval: number;
	/** How long to wait for a pong before declaring the socket dead. */
	timeout: number;
	/** Send a ping frame. */
	onPing: () => void;
	/** No pong arrived in time — the connection is dead but pretending. */
	onTimeout: () => void;
}

/** Drives ping frames and enforces the pong deadline. */
export class Heartbeat {
	#options: HeartbeatOptions;
	#ticker: Ticker | null = null;
	#unsubscribe: (() => void) | null = null;
	#deadline: ReturnType<typeof setTimeout> | undefined;

	constructor(options: HeartbeatOptions) {
		this.#options = options;
	}

	/** Whether the heartbeat is currently running. */
	get running(): boolean {
		return !!this.#ticker;
	}

	start(): void {
		if (this.#options.interval <= 0 || this.#ticker) return;

		this.#ticker = createTicker(this.#options.interval);
		this.#unsubscribe = this.#ticker.subscribe((timestamp) => {
			// The ticker emits 0 on stop; only real ticks should ping.
			if (!timestamp) return;
			this.#options.onPing();
			this.#armDeadline();
		});
		this.#ticker.start();
	}

	stop(): void {
		this.#clearDeadline();
		this.#unsubscribe?.();
		this.#unsubscribe = null;
		this.#ticker?.stop();
		this.#ticker = null;
	}

	/**
	 * Any inbound traffic proves the connection is alive, not just a literal
	 * `pong` — so the client feeds every received frame through here.
	 */
	alive(): void {
		this.#clearDeadline();
	}

	/**
	 * Starts the pong deadline if one is not already running.
	 *
	 * Deliberately does **not** restart a pending deadline. The deadline
	 * measures time since the *oldest* unanswered ping, so a later ping must
	 * not postpone the verdict on an earlier one — otherwise any configuration
	 * with `pingInterval <= pongTimeout` would reset the timer forever and
	 * silently disable half-open detection altogether.
	 */
	#armDeadline(): void {
		if (this.#deadline !== undefined) return;
		this.#deadline = setTimeout(() => {
			this.#deadline = undefined;
			this.#options.onTimeout();
		}, this.#options.timeout);
	}

	#clearDeadline(): void {
		if (this.#deadline !== undefined) {
			clearTimeout(this.#deadline);
			this.#deadline = undefined;
		}
	}
}
