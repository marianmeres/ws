/**
 * The WebSocket client.
 *
 * @module
 */

import { createClog, type Logger } from "@marianmeres/clog";
import { createPubSub, type Subscriber, type Unsubscriber } from "@marianmeres/pubsub";
import { base36 } from "@marianmeres/uid";

import {
	CLOSE,
	DEFAULT_NAMESPACE,
	DEFAULT_TERMINAL_CLOSE_CODES,
	FRAME,
	PROTOCOL_VERSION,
} from "../protocol/constants.ts";
import type {
	ClientFrame,
	ServerFrame,
	SubRequest,
	WSDecoder,
	WSEncoder,
	WSMessage,
	WSPresenceEvent,
	WSPublishResult,
} from "../protocol/frames.ts";
import {
	WSConnectionLostError,
	WSConnectTimeoutError,
	WSDisposedError,
	WSError,
	WSNotConnectedError,
	WSRemoteError,
	WSTerminatedError,
} from "../protocol/errors.ts";
import { backoffDelay } from "./backoff.ts";
import { Heartbeat } from "./heartbeat.ts";
import { Outbox } from "./outbox.ts";
import { type MessageHandler, type PresenceHandler, RoomRegistry } from "./rooms.ts";

/**
 * Connection lifecycle states.
 *
 * A plain union rather than a state-machine dependency — the transition table
 * below is the whole of the machinery, and it is small enough to read.
 */
export type WSConnectionState =
	| "idle"
	| "connecting"
	| "authenticating"
	| "open"
	| "reconnecting"
	| "terminated"
	| "disposed";

/**
 * Legal transitions.
 *
 * This exists because the reconnect races are where ad-hoc boolean flags rot:
 * a close arriving mid-`connecting`, a `disconnect()` during backoff, a
 * terminal close while the outbox is flushing. An illegal transition is a bug
 * in this file, so it logs loudly rather than failing quietly.
 */
const TRANSITIONS: Record<WSConnectionState, readonly WSConnectionState[]> = {
	idle: ["connecting", "disposed"],
	connecting: ["authenticating", "reconnecting", "terminated", "idle", "disposed"],
	authenticating: ["open", "reconnecting", "terminated", "idle", "disposed"],
	open: ["reconnecting", "terminated", "idle", "disposed"],
	reconnecting: ["connecting", "terminated", "idle", "disposed"],
	// A terminal state is still re-entrant via an explicit connect() — the user
	// may have refreshed the credentials that got them rejected.
	terminated: ["connecting", "idle", "disposed"],
	disposed: [],
};

/** Events emitted by {@link WSClient}. */
export interface WSEvents {
	/** Socket opened; authentication has not happened yet. */
	open: void;
	/** Authenticated and ready. */
	connected: { clientId: string; namespace: string };
	/** Firehose — every message, regardless of room. */
	message: WSMessage;
	/** Membership change in a room subscribed with presence enabled. */
	presence: WSPresenceEvent;
	/**
	 * Socket closed. `willReconnect` reflects the retry classification, and is
	 * `false` for the `4900` a local `disconnect()` emits.
	 */
	close: { code: number; reason: string; willReconnect: boolean };
	/** A retry is scheduled; `delay` is the jittered backoff in ms. */
	reconnecting: { attempt: number; delay: number };
	/** Gave up — the only non-retrying exit. */
	terminated: { code: number; reason: string };
	/** Something failed but the client carries on: decode, a throwing handler, … */
	error: Error;
}

/** Reactive connection state, delivered through the Svelte store contract. */
export interface WSState {
	/** Current lifecycle state. */
	state: WSConnectionState;
	/** `true` only in the `open` state — authenticated and usable. */
	connected: boolean;
	/** `true` while connecting, authenticating or waiting out a backoff. */
	connecting: boolean;
	/** Consecutive failed connection attempts; resets to 0 on success. */
	attempt: number;
	/** Most recent error, retained until the next successful connect. */
	lastError: Error | null;
}

/** Per-room subscription options. */
export interface SubscribeOptions {
	/**
	 * Enables presence tracking for this room.
	 *
	 * Presence is opt-in per room rather than global: a 10k-subscriber
	 * notification room does not want 10k join events every time the fleet
	 * reconnects.
	 */
	presence?: PresenceHandler;
}

/** Configuration for {@link WSClient}. */
export interface WSClientOptions<TAuth = unknown> {
	/**
	 * Endpoint. `ws://`/`wss://`, or `http(s)://` (upgraded automatically), or
	 * a path resolved against `location` in the browser. Default `/ws`.
	 */
	url?: string | URL;
	/** Isolation boundary. Default `"default"`. */
	namespace?: string;
	/** Preferred client id; the server may override it. */
	clientId?: string;
	/** Rooms joined automatically on every (re)connect. */
	rooms?: string[];
	/**
	 * Produces the auth payload. Called before *every* (re)connect, so
	 * returning a fresh token here is all that token refresh requires.
	 */
	auth?: () => TAuth | Promise<TAuth>;
	/**
	 * Let the first `subscribe()`/`publish()` start the connection.
	 * Default `true` — with an outbox and infinite retry, requiring an explicit
	 * `connect()` first is ceremony whose only product is an error for people
	 * who forgot.
	 */
	autoConnect?: boolean;
	/** `null` disables logging. */
	logger?: Logger | null;

	/** Initial reconnect delay in ms. Default 500. */
	reconnectDelay?: number;
	/** Reconnect delay ceiling in ms. Default 30_000. */
	reconnectDelayMax?: number;
	/** Close codes after which retrying stops. Default `[4001, 4003]`. */
	terminalCloseCodes?: number[];

	/** Ping cadence in ms. `0` disables. Default 25_000. */
	pingInterval?: number;
	/**
	 * Liveness deadline in ms — bounds both the pong reply and the initial
	 * auth handshake. Default 10_000.
	 */
	pongTimeout?: number;

	/** Bound for the *first* `connect()` await. `0` waits indefinitely. */
	connectTimeout?: number;
	/** Per-send deadline covering queue + flight + ack. Default 30_000. */
	sendTimeout?: number;
	/** Frames buffered while disconnected. `0` disables buffering. Default 100. */
	outboxMaxSize?: number;
	/** Called with frames evicted from a full outbox. */
	onOutboxDrop?: (frames: ClientFrame[]) => void;

	/** Custom wire encoder. Must match the server's. */
	encode?: WSEncoder;
	/** Custom wire decoder. Must match the server's. */
	decode?: WSDecoder;
}

const DEFAULTS = {
	url: "/ws",
	namespace: DEFAULT_NAMESPACE,
	autoConnect: true,
	reconnectDelay: 500,
	reconnectDelayMax: 30_000,
	pingInterval: 25_000,
	pongTimeout: 10_000,
	connectTimeout: 0,
	sendTimeout: 30_000,
	outboxMaxSize: 100,
} as const;

const defaultEncode: WSEncoder = (frame) => JSON.stringify(frame);
const defaultDecode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

const noop = () => {};

/** Idempotent, `using`-compatible unsubscriber. */
function makeUnsubscriber(fn: () => void): Unsubscriber {
	let done = false;
	const u = (() => {
		if (done) return;
		done = true;
		fn();
	}) as Unsubscriber;
	(u as unknown as Record<symbol, unknown>)[Symbol.dispose] = u;
	return u;
}

/**
 * A reconnecting WebSocket client with namespaces, rooms and presence.
 *
 * @example
 * ```ts
 * const ws = createWSClient({ url: "/ws", namespace: "org-123" });
 * const unsub = await ws.subscribe("chat", (msg) => console.log(msg.payload));
 * await ws.publish("chat", { text: "hi" });
 * ```
 */
export class WSClient<TAuth = unknown> {
	#url: URL;
	#requestedNamespace: string;
	#requestedClientId: string | undefined;
	#authFn: (() => TAuth | Promise<TAuth>) | undefined;
	#autoConnect: boolean;
	#terminalCodes: readonly number[];
	#reconnectDelay: number;
	#reconnectDelayMax: number;
	#connectTimeout: number;
	#pongTimeout: number;
	#outboxMaxSize: number;
	#encode: WSEncoder;
	#decode: WSDecoder;

	/** Logger. Assignable — set to `null` to silence. */
	logger: Logger | null;

	#socket: WebSocket | null = null;
	/**
	 * Guards against events from superseded sockets. Every callback checks its
	 * captured generation, so a slow `onclose` from an old socket cannot
	 * cancel the reconnect that replaced it.
	 */
	#generation = 0;
	#state: WSConnectionState = "idle";
	#clientId: string | null = null;
	#namespace: string | null = null;
	#attempt = 0;
	#lastError: Error | null = null;
	/**
	 * Kept apart from `#lastError` on purpose: a handler that throws after the
	 * terminal close would overwrite `#lastError` and a send rejected from the
	 * `terminated` state would then blame the wrong thing.
	 */
	#terminalError: WSTerminatedError | null = null;

	#rooms = new RoomRegistry();
	#outbox: Outbox;
	#heartbeat: Heartbeat;

	#bus = createPubSub<Record<string, unknown>>();
	#stateBus = createPubSub<Record<string, unknown>>();

	#reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	#handshakeTimer: ReturnType<typeof setTimeout> | undefined;
	#connectTimer: ReturnType<typeof setTimeout> | undefined;
	#connectDeferred:
		| { promise: Promise<void>; resolve: () => void; reject: (e: Error) => void }
		| null = null;
	#wakeListeners: Array<() => void> = [];

	/**
	 * Nothing connects here — the socket opens on the first `connect()`,
	 * `subscribe()` or `publish()`.
	 *
	 * @param options - see {@link WSClientOptions}; every field has a default
	 */
	constructor(options: WSClientOptions<TAuth> = {}) {
		this.logger = options.logger === undefined ? createClog("ws") : options.logger;

		this.#url = WSClient.resolveUrl(options.url ?? DEFAULTS.url);
		this.#requestedNamespace = options.namespace ?? DEFAULTS.namespace;
		this.#requestedClientId = options.clientId;
		this.#authFn = options.auth;
		this.#autoConnect = options.autoConnect ?? DEFAULTS.autoConnect;
		this.#terminalCodes = options.terminalCloseCodes ??
			DEFAULT_TERMINAL_CLOSE_CODES;
		this.#reconnectDelay = options.reconnectDelay ?? DEFAULTS.reconnectDelay;
		this.#reconnectDelayMax = options.reconnectDelayMax ??
			DEFAULTS.reconnectDelayMax;
		this.#connectTimeout = options.connectTimeout ?? DEFAULTS.connectTimeout;
		this.#pongTimeout = options.pongTimeout ?? DEFAULTS.pongTimeout;
		this.#outboxMaxSize = options.outboxMaxSize ?? DEFAULTS.outboxMaxSize;
		this.#encode = options.encode ?? defaultEncode;
		this.#decode = options.decode ?? defaultDecode;

		this.#outbox = new Outbox({
			maxSize: this.#outboxMaxSize,
			sendTimeout: options.sendTimeout ?? DEFAULTS.sendTimeout,
			onDrop: options.onOutboxDrop,
		});

		this.#heartbeat = new Heartbeat({
			interval: options.pingInterval ?? DEFAULTS.pingInterval,
			timeout: options.pongTimeout ?? DEFAULTS.pongTimeout,
			onPing: () => this.#sendRaw({ type: FRAME.PING }),
			onTimeout: () => {
				this.logger?.warn?.("pong timeout — connection is half-open");
				this.#forceClose(CLOSE.IDLE_TIMEOUT, "pong timeout");
			},
		});

		// Rooms configured up front get a no-op handler; the `message` firehose
		// still fires, so `rooms: [...]` + `on("message")` is a valid style.
		for (const room of options.rooms ?? []) this.#rooms.add(room, noop);

		this.#installWakeListeners();
	}

	/**
	 * Normalizes an endpoint: relative paths resolve against `location`, and
	 * `http(s)` is upgraded to `ws(s)`.
	 *
	 * @param input - absolute url, or a path when running in a browser
	 * @returns the normalized `ws(s)://` url
	 * @throws {WSError} when the input cannot be resolved — outside a browser
	 * there is no `location` to resolve a relative path against
	 */
	static resolveUrl(input: string | URL): URL {
		const base = typeof globalThis.location !== "undefined"
			? globalThis.location.href
			: undefined;
		let url: URL;
		try {
			url = new URL(String(input), base);
		} catch {
			throw new WSError(
				`Invalid url "${input}". Outside a browser an absolute ws:// or ` +
					`wss:// url is required.`,
			);
		}
		if (url.protocol === "http:") url.protocol = "ws:";
		else if (url.protocol === "https:") url.protocol = "wss:";
		return url;
	}

	// ---------------------------------------------------------------- getters

	/** `true` only when authenticated and usable — not merely socket-open. */
	get connected(): boolean {
		return this.#state === "open";
	}

	/** Current lifecycle state. See {@link WSConnectionState}. */
	get connectionState(): WSConnectionState {
		return this.#state;
	}

	/** Server-assigned id, available once connected. */
	get clientId(): string | null {
		return this.#clientId;
	}

	/** Active namespace — the server's assignment wins over the request. */
	get namespace(): string {
		return this.#namespace ?? this.#requestedNamespace;
	}

	/** Resolved endpoint. A copy — mutating it does not affect the client. */
	get url(): URL {
		return new URL(this.#url.href);
	}

	/**
	 * The underlying socket, or `null` while disconnected.
	 *
	 * Escape hatch for inspection. Sending on it directly bypasses the outbox
	 * and the ack correlation, so don't.
	 */
	get socket(): WebSocket | null {
		return this.#socket;
	}

	/** Rooms currently subscribed. */
	get rooms(): string[] {
		return this.#rooms.rooms;
	}

	/**
	 * Reactive state, Svelte-store compatible: the callback fires immediately
	 * with the current value and again on every change.
	 */
	get state(): { subscribe: (cb: Subscriber<WSState>) => Unsubscriber } {
		return {
			subscribe: (cb: Subscriber<WSState>) => {
				cb(this.#snapshot());
				return this.#stateBus.subscribe("state", cb as Subscriber);
			},
		};
	}

	/**
	 * Debug snapshot: url, state, identity, rooms and outbox counters.
	 *
	 * For logging and troubleshooting — the shape is not part of the stable API.
	 */
	dump(): Record<string, unknown> {
		return {
			url: this.#url.href,
			state: this.#state,
			clientId: this.#clientId,
			namespace: this.namespace,
			rooms: this.#rooms.rooms,
			attempt: this.#attempt,
			pending: this.#outbox.pendingCount,
			queued: this.#outbox.queuedCount,
			dropped: this.#outbox.droppedCount,
		};
	}

	// ----------------------------------------------------------------- events

	/**
	 * Subscribes to a lifecycle event. See {@link WSEvents}.
	 *
	 * @param event - event name
	 * @param cb - handler; a throw here is caught and reported as `error`
	 * @returns detaches the handler; also `Symbol.dispose`-compatible
	 */
	on<K extends keyof WSEvents>(
		event: K,
		cb: (data: WSEvents[K]) => void,
	): Unsubscriber {
		return this.#bus.subscribe(event as string, cb as Subscriber);
	}

	/**
	 * Like {@link on}, but detaches after the first emission.
	 *
	 * @param event - event name
	 * @param cb - handler
	 * @returns detaches the handler early, if it has not fired yet
	 */
	once<K extends keyof WSEvents>(
		event: K,
		cb: (data: WSEvents[K]) => void,
	): Unsubscriber {
		return this.#bus.subscribeOnce(event as string, cb as Subscriber);
	}

	// -------------------------------------------------------------- lifecycle

	/**
	 * Starts the connection and resolves once it is established.
	 *
	 * Idempotent: concurrent calls share one promise, and it resolves
	 * immediately when already connected.
	 *
	 * Rejects **only** where retrying cannot help:
	 * - {@link WSTerminatedError} — terminal close code (bad credentials, etc.)
	 * - {@link WSConnectTimeoutError} — `connectTimeout` elapsed; note the
	 *   client keeps retrying in the background, so this bounds *your await*,
	 *   not the connection attempt
	 *
	 * Ordinary network failure never rejects; that is what the infinite retry
	 * is for.
	 *
	 * Calling this is optional when `autoConnect` is on — it is a readiness
	 * gate, not a prerequisite.
	 *
	 * @returns resolves once authenticated
	 */
	connect(): Promise<void> {
		if (this.#state === "disposed") {
			return Promise.reject(new WSDisposedError());
		}
		if (this.#state === "open") return Promise.resolve();
		if (this.#connectDeferred) return this.#connectDeferred.promise;

		let resolve!: () => void;
		let reject!: (e: Error) => void;
		const promise = new Promise<void>((res, rej) => {
			resolve = res;
			reject = rej;
		});
		this.#connectDeferred = { promise, resolve, reject };

		if (this.#connectTimeout > 0) {
			this.#connectTimer = setTimeout(() => {
				this.#settleConnect(new WSConnectTimeoutError(this.#connectTimeout));
			}, this.#connectTimeout);
		}

		if (this.#state === "idle" || this.#state === "terminated") this.#open();

		return promise;
	}

	/**
	 * Stops retrying and closes the socket.
	 *
	 * Resumable: handlers, room subscriptions and buffered sends all survive,
	 * so a later `connect()` picks up exactly where this left off. Use
	 * {@link dispose} for terminal teardown.
	 *
	 * Emits `close` with {@link CLOSE.CLIENT_GONE} and `willReconnect: false`
	 * when there was a socket to close; nothing when already idle, reconnecting
	 * or terminated.
	 */
	disconnect(): void {
		if (this.#state === "disposed") return;
		this.logger?.debug?.("disconnect()");
		this.#clearTimers();
		this.#heartbeat.stop();
		this.#settleConnect(
			new WSTerminatedError(CLOSE.CLIENT_GONE, "disconnect() called"),
		);
		const reason = "client disconnect";
		const closed = this.#socket !== null;
		this.#closeSocket(CLOSE.CLIENT_GONE, reason);
		if (closed) {
			this.#emit("close", {
				code: CLOSE.CLIENT_GONE,
				reason,
				willReconnect: false,
			});
		}
		this.#settleInFlight();
		this.#setState("idle");
	}

	/**
	 * Terminal teardown: disconnects, then drops every handler, room, timer and
	 * pending promise. The instance is unusable afterwards.
	 */
	dispose(): void {
		if (this.#state === "disposed") return;
		this.logger?.debug?.("dispose()");
		this.disconnect();
		this.#outbox.failAll(new WSDisposedError());
		this.#rooms.clear();
		this.#bus.unsubscribeAll();
		this.#stateBus.unsubscribeAll();
		this.#removeWakeListeners();
		this.#setState("disposed");
	}

	// ---------------------------------------------------------- subscriptions

	/**
	 * Subscribes to a room and attaches a handler.
	 *
	 * The handler is attached **synchronously**, before any frame goes out, so
	 * nothing arriving between the request and its acknowledgement is lost.
	 *
	 * Rooms are refcounted: N handlers produce one wire subscription, and the
	 * returned unsubscriber detaches this handler — sending `unsub` only when
	 * it was the last one.
	 *
	 * Resolution: when connected, this awaits the server's acknowledgement, so
	 * a rejected subscription surfaces as a rejection here. When not connected
	 * it resolves as soon as the room is registered — the subscription is then
	 * guaranteed to be established by the re-subscribe step on the next
	 * connect, and a failure there surfaces as an `error` event.
	 *
	 * @param room - room name, scoped to this client's namespace
	 * @param handler - receives every message published to the room
	 * @param options - pass `presence` to enable membership tracking
	 * @returns detaches this handler; also `Symbol.dispose`-compatible, and
	 * idempotent, so calling it twice is harmless
	 * @throws {WSRemoteError} when connected and the server refuses
	 * @throws {WSDisposedError} when the client was disposed
	 *
	 * @example
	 * ```ts
	 * const unsub = await ws.subscribe("chat", (msg) => render(msg.payload), {
	 *     presence: (e) => setMembers(e.members),
	 * });
	 * ```
	 */
	async subscribe<T = unknown>(
		room: string,
		handler: MessageHandler<T>,
		options?: SubscribeOptions,
	): Promise<Unsubscriber> {
		this.#assertUsable();

		const messageHandler = handler as MessageHandler;
		const presenceHandler = options?.presence;
		const { created, presenceUpgraded } = this.#rooms.add(
			room,
			messageHandler,
			presenceHandler,
		);

		const unsubscriber = makeUnsubscriber(() => {
			const emptied = this.#rooms.remove(room, messageHandler, presenceHandler);
			if (emptied && this.connected) {
				this.#sendControl({
					type: FRAME.UNSUB,
					id: this.#nextId(),
					rooms: [room],
				}).catch((e) => {
					// Not actionable: the handler is already detached locally and
					// the server drops the room on close anyway.
					this.logger?.debug?.(`unsub "${room}" unconfirmed: ${e.message}`);
				});
			}
		});

		if (this.#autoConnect) this.#ensureStarted();

		if ((created || presenceUpgraded) && this.connected) {
			try {
				await this.#sendControl({
					type: FRAME.SUB,
					id: this.#nextId(),
					rooms: [{ room, presence: this.#rooms.wantsPresence(room) }],
				});
			} catch (e) {
				// Keep local state honest: if the server refused, we are not
				// subscribed, and pretending otherwise would silently swallow
				// every message the caller expects.
				this.#rooms.remove(room, messageHandler, presenceHandler);
				throw e;
			}
		}

		return unsubscriber;
	}

	/**
	 * Removes every handler for a room and unsubscribes it.
	 *
	 * The blunt counterpart to the refcounted unsubscriber returned by
	 * {@link subscribe} — this drops other call sites' handlers too.
	 *
	 * @param room - room name; unknown rooms are a no-op
	 */
	async unsubscribe(room: string): Promise<void> {
		this.#assertUsable();
		if (!this.#rooms.removeRoom(room)) return;
		if (this.connected) {
			await this.#sendControl({
				type: FRAME.UNSUB,
				id: this.#nextId(),
				rooms: [room],
			});
		}
	}

	/**
	 * Whether the room is held locally.
	 *
	 * Reflects local intent, not server state: a room registered while offline
	 * reads `true` before the wire subscription exists.
	 *
	 * @param room - room name
	 */
	isSubscribed(room: string): boolean {
		return this.#rooms.has(room);
	}

	/**
	 * Last known membership of a presence-enabled room.
	 *
	 * @param room - room name
	 * @returns a copy of the members; empty when the room has no presence
	 */
	members(room: string): string[] {
		return this.#rooms.members(room);
	}

	// --------------------------------------------------------------- sending

	/**
	 * Publishes to a room within this client's namespace.
	 *
	 * Resolves with the recipient count once the server acknowledges. While
	 * disconnected the frame is buffered and the promise stays pending until it
	 * flushes — bounded by `sendTimeout`, never indefinitely.
	 *
	 * @param room - target room
	 * @param payload - opaque application data; never inspected or mutated
	 * @param namespace - must equal this client's namespace; the server rejects
	 * anything else, so this is only useful for asserting the expected one
	 * @returns the recipient count reported by the receiving server instance —
	 * best-effort telemetry, not a delivery guarantee
	 * @throws {WSTimeoutError} `sendTimeout` elapsed with no acknowledgement
	 * @throws {WSOutboxDropError} evicted from a full outbox
	 * @throws {WSNotConnectedError} sent while offline with `outboxMaxSize: 0`
	 * @throws {WSTerminatedError} sent after a terminal close, which only an
	 * explicit `connect()` recovers from — rejected at once, not buffered
	 * @throws {WSRemoteError} the server rejected it with a `nack`
	 */
	publish<T = unknown>(
		room: string,
		payload: T,
		namespace?: string,
	): Promise<WSPublishResult> {
		this.#assertUsable();
		const id = this.#nextId();
		return this.#send({
			type: FRAME.PUB,
			id,
			room,
			payload,
			...(namespace ? { namespace } : {}),
		}, id);
	}

	/**
	 * Publishes to a room across **all** namespaces.
	 *
	 * This crosses the isolation boundary, which is why it is its own method
	 * rather than a flag on {@link publish} — the server gates it separately
	 * via `allowBroadcast`, and it denies by default.
	 *
	 * @param room - target room, in every namespace at once
	 * @param payload - opaque application data
	 * @returns the recipient count across all namespaces on the receiving
	 * server instance
	 * @throws {WSRemoteError} with code `forbidden` when `allowBroadcast` denies
	 */
	broadcast<T = unknown>(room: string, payload: T): Promise<WSPublishResult> {
		this.#assertUsable();
		const id = this.#nextId();
		return this.#send({ type: FRAME.BROADCAST, id, room, payload }, id);
	}

	// -------------------------------------------------------------- internals

	#nextId(): string {
		return base36(12);
	}

	#assertUsable(): void {
		if (this.#state === "disposed") throw new WSDisposedError();
	}

	#ensureStarted(): void {
		if (this.#state === "idle") this.#open();
	}

	#send(frame: ClientFrame, id: string): Promise<WSPublishResult> {
		if (this.#autoConnect) this.#ensureStarted();

		// Nothing restarts from `terminated` except an explicit connect(), so
		// buffering here would only defer the same answer by `sendTimeout`.
		if (this.#terminalError && this.#state === "terminated") {
			return Promise.reject(this.#terminalError);
		}

		const canSendNow = this.connected &&
			this.#socket?.readyState === WebSocket.OPEN;

		if (!canSendNow && this.#outboxMaxSize === 0) {
			return Promise.reject(new WSNotConnectedError());
		}

		const promise = this.#outbox.track(id, frame, !canSendNow);
		if (canSendNow) {
			const error = this.#sendRaw(frame);
			if (error) this.#outbox.fail(id, error);
		}
		return promise;
	}

	/**
	 * Sends a control frame that must never be buffered.
	 *
	 * `sub`/`unsub` bypass the outbox deliberately: they are replayed wholesale
	 * by the re-subscribe step on reconnect, so queueing them too would apply
	 * them twice.
	 */
	#sendControl(frame: ClientFrame): Promise<WSPublishResult> {
		const id = "id" in frame ? frame.id : this.#nextId();
		const promise = this.#outbox.track(id, frame, false);
		const error = this.#sendRaw(frame);
		if (error) this.#outbox.fail(id, error);
		return promise;
	}

	/**
	 * @returns the error the send failed with — a frame that never left cannot
	 * be acknowledged, so the caller settles its promise instead of letting it
	 * wait out `sendTimeout`.
	 */
	#sendRaw(frame: ClientFrame): Error | null {
		const socket = this.#socket;
		if (!socket || socket.readyState !== WebSocket.OPEN) return null;
		try {
			socket.send(this.#encode(frame));
		} catch (e) {
			return this.#fail(e, "send failed");
		}
		return null;
	}

	#open(): void {
		// Defensive: every caller already guards this, but a second socket
		// opened from a race here would leak the first one silently.
		if (
			this.#state === "connecting" ||
			this.#state === "authenticating" ||
			this.#state === "open"
		) {
			return;
		}
		if (!this.#setState("connecting")) return;

		this.#terminalError = null;
		const generation = ++this.#generation;
		let socket: WebSocket;

		try {
			socket = new WebSocket(this.#url);
		} catch (e) {
			this.#fail(e, "socket construction failed");
			this.#scheduleReconnect();
			return;
		}

		this.#socket = socket;
		// Without this a binary frame arrives as a Blob, which no synchronous
		// decoder can read — the `WSDecoder` contract promises an `ArrayBuffer`.
		socket.binaryType = "arraybuffer";
		this.logger?.debug?.(`connecting to ${this.#url.href}`);

		socket.onopen = () => {
			if (generation !== this.#generation) return;
			void this.#onOpen(generation);
		};
		socket.onmessage = (event: MessageEvent) => {
			if (generation !== this.#generation) return;
			this.#onMessage(event.data);
		};
		socket.onerror = () => {
			if (generation !== this.#generation) return;
			// The browser deliberately withholds detail here; onclose carries
			// the actionable information, so this is informational only.
			this.logger?.debug?.("socket error");
		};
		socket.onclose = (event: CloseEvent) => {
			if (generation !== this.#generation) return;
			this.#onClose(event.code, event.reason);
		};
	}

	async #onOpen(generation: number): Promise<void> {
		this.#emit("open", undefined);
		if (!this.#setState("authenticating")) return;

		let payload: unknown = null;
		try {
			payload = (await this.#authFn?.()) ?? null;
		} catch (e) {
			this.#fail(e, "auth payload failed");
			this.#forceClose(CLOSE.PROTOCOL_ERROR, "auth payload failed");
			return;
		}
		// The await above yields; a close may have superseded this socket.
		if (generation !== this.#generation) return;

		this.#sendRaw({
			type: FRAME.AUTH,
			id: this.#nextId(),
			protocol: PROTOCOL_VERSION,
			payload,
			...(this.#requestedClientId ? { clientId: this.#requestedClientId } : {}),
			namespace: this.#requestedNamespace,
		});

		// Without this, a server that accepts the socket then never replies
		// leaves us in `authenticating` forever — the heartbeat has not started
		// yet, so nothing else would notice.
		this.#handshakeTimer = setTimeout(() => {
			this.logger?.warn?.("handshake timeout");
			this.#forceClose(CLOSE.AUTH_TIMEOUT, "handshake timeout");
		}, this.#pongTimeout);
	}

	#onMessage(raw: string | ArrayBuffer): void {
		// Any inbound traffic proves the socket is alive, not just a pong.
		this.#heartbeat.alive();

		let frame: ServerFrame;
		try {
			frame = this.#decode(raw) as ServerFrame;
		} catch (e) {
			this.#fail(e, "decode failed");
			return;
		}

		if (!frame || typeof frame.type !== "string") {
			this.logger?.warn?.("ignoring frame without a type", frame);
			return;
		}

		switch (frame.type) {
			case FRAME.HELLO:
				this.#onHello(frame.clientId, frame.namespace, frame.protocol);
				break;

			case FRAME.ACK:
				this.#outbox.settle(frame.id, frame.recipients ?? 0);
				break;

			case FRAME.NACK:
				this.#outbox.fail(frame.id, new WSRemoteError(frame.error));
				break;

			case FRAME.MSG: {
				const { type: _t, ...message } = frame;
				this.#emit("message", message as WSMessage);
				this.#rooms.deliver(
					message.room,
					message as WSMessage,
					(e) => this.#fail(e, "message handler threw"),
				);
				break;
			}

			case FRAME.PRESENCE: {
				const { type: _t, ...event } = frame;
				this.#emit("presence", event as WSPresenceEvent);
				this.#rooms.deliverPresence(
					event.room,
					event as WSPresenceEvent,
					(e) => this.#fail(e, "presence handler threw"),
				);
				break;
			}

			case FRAME.PONG:
				break;

			case FRAME.ERROR:
				this.#fail(new WSRemoteError(frame.error), "server error");
				break;

			default:
				this.logger?.warn?.("ignoring unknown frame type", frame);
		}
	}

	#onHello(clientId: string, namespace: string, protocol: number): void {
		clearTimeout(this.#handshakeTimer);
		this.#handshakeTimer = undefined;

		if (protocol !== PROTOCOL_VERSION) {
			this.logger?.warn?.(
				`protocol version mismatch (client ${PROTOCOL_VERSION}, server ${protocol})`,
			);
		}

		this.#clientId = clientId;
		this.#namespace = namespace;
		this.#attempt = 0;
		this.#lastError = null;

		if (!this.#setState("open")) return;

		this.logger?.debug?.(`connected as ${clientId} in "${namespace}"`);
		this.#emit("connected", { clientId, namespace });
		this.#heartbeat.start();
		this.#settleConnect(null);

		// Order matters and is not cosmetic: re-subscribe first, then flush.
		// The socket preserves ordering, so the server registers the rooms
		// before it sees any buffered publish destined for them.
		const requests: SubRequest[] = this.#rooms.subRequests();
		if (requests.length) {
			this.#sendControl({
				type: FRAME.SUB,
				id: this.#nextId(),
				rooms: requests,
			}).catch((e) => this.#fail(e, "re-subscribe failed"));
		}

		const buffered = this.#outbox.drain();
		if (buffered.length) {
			this.logger?.debug?.(`flushing ${buffered.length} buffered frame(s)`);
			for (const frame of buffered) {
				const error = this.#sendRaw(frame);
				if (error && "id" in frame) this.#outbox.fail(frame.id, error);
			}
		}
	}

	#onClose(code: number, reason: string): void {
		clearTimeout(this.#handshakeTimer);
		this.#handshakeTimer = undefined;
		this.#heartbeat.stop();
		this.#socket = null;

		const terminal = this.#terminalCodes.includes(code);
		const willReconnect = !terminal && this.#state !== "disposed";

		this.logger?.debug?.(
			`closed (${code}${reason ? ` ${reason}` : ""}), reconnect=${willReconnect}`,
		);
		this.#emit("close", { code, reason, willReconnect });

		if (terminal) {
			this.#setState("terminated");
			const error = new WSTerminatedError(code, reason);
			this.#lastError = error;
			this.#terminalError = error;
			// Loud on purpose: this is the only path where a client that
			// otherwise retries forever gives up, and a silent one looks
			// exactly like a network that never came back.
			this.logger?.error?.(error.message);
			this.#outbox.failAll(error);
			this.#settleConnect(error);
			this.#emit("terminated", { code, reason });
			return;
		}

		this.#settleInFlight();

		if (!willReconnect) {
			this.#setState("idle");
			return;
		}

		this.#scheduleReconnect();
	}

	/**
	 * Answers the frames that were on the wire when the socket went away.
	 *
	 * `sub`/`unsub` resolve: the room registry is authoritative locally and the
	 * re-subscribe step will establish it on the next connection, which is
	 * exactly the contract of a `subscribe()` issued while offline — and the
	 * server forgets its rooms on close anyway. Publishes reject: they were not
	 * delivered, and at-most-once means they will not be resent.
	 */
	#settleInFlight(): void {
		this.#outbox.settleInFlight((frame) =>
			frame.type === FRAME.SUB || frame.type === FRAME.UNSUB
				? null
				: new WSConnectionLostError()
		);
	}

	#scheduleReconnect(): void {
		if (!this.#setState("reconnecting")) return;

		this.#attempt++;
		const delay = backoffDelay(
			this.#attempt,
			this.#reconnectDelay,
			this.#reconnectDelayMax,
		);

		this.logger?.debug?.(`reconnecting in ${delay}ms (attempt ${this.#attempt})`);
		this.#emit("reconnecting", { attempt: this.#attempt, delay });
		this.#publishState();

		clearTimeout(this.#reconnectTimer);
		this.#reconnectTimer = setTimeout(() => {
			this.#reconnectTimer = undefined;
			this.#open();
		}, delay);
	}

	/**
	 * Reconnect right now instead of waiting out the backoff.
	 *
	 * Matters more than backoff tuning: a laptop waking from sleep should be
	 * back in milliseconds, not sit out a 30s timer it started before sleeping.
	 */
	#wake(source: string): void {
		if (this.#state !== "reconnecting") return;
		this.logger?.debug?.(`${source} — retrying immediately`);
		clearTimeout(this.#reconnectTimer);
		this.#reconnectTimer = undefined;
		this.#attempt = 0;
		this.#open();
	}

	#installWakeListeners(): void {
		const target = globalThis as unknown as {
			addEventListener?: (t: string, l: () => void) => void;
			removeEventListener?: (t: string, l: () => void) => void;
			document?: { visibilityState?: string };
		};
		if (typeof target.addEventListener !== "function") return;

		const onOnline = () => this.#wake("online");
		const onVisible = () => {
			if (target.document?.visibilityState === "visible") {
				this.#wake("tab visible");
			}
		};

		target.addEventListener("online", onOnline);
		this.#wakeListeners.push(() => target.removeEventListener?.("online", onOnline));

		if (target.document) {
			target.addEventListener("visibilitychange", onVisible);
			this.#wakeListeners.push(() =>
				target.removeEventListener?.("visibilitychange", onVisible)
			);
		}
	}

	#removeWakeListeners(): void {
		for (const remove of this.#wakeListeners) remove();
		this.#wakeListeners = [];
	}

	/**
	 * Closes a socket we have decided is dead, and drives the close transition
	 * ourselves.
	 *
	 * `#closeSocket` supersedes the generation, so the real `onclose` — if it
	 * arrives at all, which for a half-open socket it may not — is ignored.
	 * That is the point: waiting for it is exactly the hang we are escaping.
	 */
	#forceClose(code: number, reason: string): void {
		this.#closeSocket(code, reason);
		this.#onClose(code, reason);
	}

	#closeSocket(code: number, reason: string): void {
		const socket = this.#socket;
		this.#socket = null;
		// Supersede: any late callback from this socket is now ignored.
		this.#generation++;
		if (!socket) return;
		try {
			if (
				socket.readyState === WebSocket.OPEN ||
				socket.readyState === WebSocket.CONNECTING
			) {
				socket.close(code, reason);
			}
		} catch {
			// Closing an already-dead socket is not worth reporting.
		}
	}

	#clearTimers(): void {
		clearTimeout(this.#reconnectTimer);
		clearTimeout(this.#handshakeTimer);
		this.#reconnectTimer = undefined;
		this.#handshakeTimer = undefined;
	}

	#settleConnect(error: Error | null): void {
		clearTimeout(this.#connectTimer);
		this.#connectTimer = undefined;
		const deferred = this.#connectDeferred;
		if (!deferred) return;
		this.#connectDeferred = null;
		if (error) deferred.reject(error);
		else deferred.resolve();
	}

	#setState(next: WSConnectionState): boolean {
		if (this.#state === next) return true;
		if (!TRANSITIONS[this.#state].includes(next)) {
			this.logger?.warn?.(
				`ignoring illegal state transition ${this.#state} -> ${next}`,
			);
			return false;
		}
		this.#state = next;
		this.#publishState();
		return true;
	}

	#snapshot(): WSState {
		return {
			state: this.#state,
			connected: this.#state === "open",
			connecting: this.#state === "connecting" ||
				this.#state === "authenticating" ||
				this.#state === "reconnecting",
			attempt: this.#attempt,
			lastError: this.#lastError,
		};
	}

	#publishState(): void {
		this.#stateBus.publish("state", this.#snapshot());
	}

	#emit<K extends keyof WSEvents>(event: K, data: WSEvents[K]): void {
		this.#bus.publish(event as string, data);
	}

	#fail(error: unknown, context: string): Error {
		const err = error instanceof Error ? error : new WSError(String(error));
		this.#lastError = err;
		this.logger?.error?.(`${context}: ${err.message}`);
		this.#emit("error", err);
		return err;
	}
}

/**
 * Creates a {@link WSClient}.
 *
 * Both this and the class are exported, following the `PubSub` /
 * `createPubSub` precedent in `@marianmeres/pubsub`.
 *
 * @param options - see {@link WSClientOptions}
 * @returns a client that has not connected yet
 *
 * @example
 * ```ts
 * const ws = createWSClient({
 *     url: "wss://example.com/ws",
 *     namespace: "org-123",
 *     auth: () => session.token, // re-read on every reconnect
 * });
 *
 * await ws.subscribe("chat", (msg) => console.log(msg.from, msg.payload));
 * await ws.publish("chat", { text: "hello" });
 * ```
 */
export function createWSClient<TAuth = unknown>(
	options: WSClientOptions<TAuth> = {},
): WSClient<TAuth> {
	return new WSClient<TAuth>(options);
}
