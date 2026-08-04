/**
 * Connection registry, room index, presence tracking and delivery.
 *
 * @module
 */

import { createClog, type Logger } from "@marianmeres/clog";
import { base36 } from "@marianmeres/uid";

import {
	CLOSE,
	DEFAULT_NAMESPACE,
	ERROR_CODE,
	FRAME,
	PRESENCE,
	PROTOCOL_VERSION,
} from "../protocol/constants.ts";
import type {
	AuthResult,
	ClientFrame,
	PresenceEventType,
	ServerFrame,
	WSDecoder,
	WSEncoder,
	WSMessage,
} from "../protocol/frames.ts";
import type { WSBroadcastEnvelope, WSPubSubAdapter } from "./adapters/abstract.ts";
import { WSPubSubLocal } from "./adapters/local.ts";

/** What a hook knows about a connection. */
export interface WSConnectionContext {
	clientId: string;
	namespace: string;
	meta: Record<string, unknown>;
	request: Request;
}

/** Server statistics. */
export interface WSStats {
	/** Authenticated connections. */
	connections: number;
	/** Connections that have not authenticated yet. */
	pending: number;
	/** Distinct room names in use. */
	rooms: number;
	/** Authenticated connection count per namespace. */
	namespaces: Record<string, number>;
}

/** Configuration for {@link WSService}. */
export interface WSServiceOptions {
	/**
	 * Authenticates a connection. Return `null` (or throw) to reject with
	 * {@link CLOSE.AUTH_FAILED}. Omitted entirely means "no authentication",
	 * which is fine for development and not for anything else.
	 */
	verify?: (
		payload: unknown,
		request: Request,
	) => Promise<AuthResult | null> | AuthResult | null;
	/**
	 * Gate for cross-namespace broadcast. **Denies by default** — letting any
	 * client punch through every namespace boundary is not a safe default, and
	 * keeping `broadcast()` a distinct operation exists precisely so this check
	 * has somewhere to live.
	 */
	allowBroadcast?: (
		ctx: WSConnectionContext,
		room: string,
	) => boolean | Promise<boolean>;
	/** Deadline for the client's `auth` frame. Default 5_000. */
	authTimeout?: number;
	/** Close a connection silent for this long. Default 60_000 (~2 missed pings). */
	idleTimeout?: number;
	/** Reject oversized frames. Default 256 KiB. */
	maxFrameSize?: number;
	/** Per-connection frame rate cap. Default 100/s. */
	maxFramesPerSecond?: number;
	adapter?: WSPubSubAdapter;
	logger?: Logger | null;
	encode?: WSEncoder;
	decode?: WSDecoder;
}

interface Connection {
	id: string;
	socket: WebSocket;
	namespace: string;
	/** room -> wants presence events */
	rooms: Map<string, boolean>;
	meta: Record<string, unknown>;
	request: Request;
	authed: boolean;
	lastSeen: number;
	authTimer?: ReturnType<typeof setTimeout>;
	windowStart: number;
	windowCount: number;
}

const DEFAULTS = {
	authTimeout: 5_000,
	idleTimeout: 60_000,
	maxFrameSize: 256 * 1024,
	maxFramesPerSecond: 100,
} as const;

const defaultEncode: WSEncoder = (frame) => JSON.stringify(frame);
const defaultDecode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

/**
 * Owns every connection, the room index, and message delivery.
 *
 * Usable standalone (drive it from any `Deno.serve` handler) or through
 * `createWSApp`, which mounts it as a demino app.
 */
export class WSService {
	#options: Required<
		Omit<
			WSServiceOptions,
			"verify" | "allowBroadcast" | "adapter" | "logger" | "encode" | "decode"
		>
	>;
	#verify: WSServiceOptions["verify"];
	#allowBroadcast: WSServiceOptions["allowBroadcast"];
	#adapter: WSPubSubAdapter;
	#encode: WSEncoder;
	#decode: WSDecoder;

	/** Logger. Assignable — set to `null` to silence. */
	logger: Logger | null;

	/** Authenticated connections, keyed by client id. */
	#connections = new Map<string, Connection>();
	/** Connections that have not completed the handshake yet. */
	#pending = new Set<Connection>();
	/** room -> namespace -> client ids. */
	#index = new Map<string, Map<string, Set<string>>>();

	#sweepTimer: ReturnType<typeof setInterval> | undefined;
	#unsubscribeRemote: () => void;
	#closed = false;

	constructor(options: WSServiceOptions = {}) {
		this.logger = options.logger === undefined
			? createClog("ws:server")
			: options.logger;

		this.#options = {
			authTimeout: options.authTimeout ?? DEFAULTS.authTimeout,
			idleTimeout: options.idleTimeout ?? DEFAULTS.idleTimeout,
			maxFrameSize: options.maxFrameSize ?? DEFAULTS.maxFrameSize,
			maxFramesPerSecond: options.maxFramesPerSecond ??
				DEFAULTS.maxFramesPerSecond,
		};
		this.#verify = options.verify;
		this.#allowBroadcast = options.allowBroadcast;
		this.#adapter = options.adapter ?? new WSPubSubLocal();
		this.#encode = options.encode ?? defaultEncode;
		this.#decode = options.decode ?? defaultDecode;

		this.#unsubscribeRemote = this.#adapter.onRemote((envelope) => {
			this.#deliverLocal(envelope.namespace, envelope.message);
		});

		if (this.#options.idleTimeout > 0) {
			// Sweeping at half the timeout bounds the worst-case reap latency
			// to ~1.5x the configured value.
			this.#sweepTimer = setInterval(
				() => this.#sweep(),
				Math.max(250, Math.floor(this.#options.idleTimeout / 2)),
			);
		}
	}

	// ------------------------------------------------------------- public API

	/**
	 * Upgrades an HTTP request to a WebSocket and takes ownership of it.
	 *
	 * Returns the 101 response, which must be returned from the route handler
	 * unmodified.
	 */
	handleUpgrade(request: Request): Response {
		const { socket, response } = Deno.upgradeWebSocket(request);

		const conn: Connection = {
			id: base36(12),
			socket,
			namespace: DEFAULT_NAMESPACE,
			rooms: new Map(),
			meta: {},
			request,
			authed: false,
			lastSeen: Date.now(),
			windowStart: Date.now(),
			windowCount: 0,
		};
		this.#pending.add(conn);

		conn.authTimer = setTimeout(() => {
			if (conn.authed) return;
			this.logger?.debug?.("handshake timed out");
			this.#close(conn, CLOSE.AUTH_TIMEOUT, "auth timeout");
		}, this.#options.authTimeout);

		socket.onmessage = (event: MessageEvent) => {
			void this.#onMessage(conn, event.data);
		};
		socket.onclose = () => this.#onClose(conn);
		socket.onerror = () => this.logger?.debug?.(`socket error (${conn.id})`);

		return response;
	}

	/**
	 * Publishes into a namespace + room from server-side code.
	 *
	 * Delivered messages carry `from: null`, which is how clients distinguish
	 * server-injected messages from peer traffic.
	 */
	publish(
		room: string,
		payload: unknown,
		namespace: string = DEFAULT_NAMESPACE,
		from: string | null = null,
	): Promise<number> {
		const message: WSMessage = {
			room,
			namespace,
			from,
			payload,
			timestamp: Date.now(),
		};
		const recipients = this.#deliverLocal(namespace, message);
		return this.#propagate({ namespace, message }, recipients);
	}

	/** Publishes into a room across every namespace. */
	broadcast(
		room: string,
		payload: unknown,
		from: string | null = null,
	): Promise<number> {
		const message: WSMessage = {
			room,
			// A broadcast has no single namespace; receivers see their own.
			namespace: "*",
			from,
			payload,
			timestamp: Date.now(),
		};
		const recipients = this.#deliverLocal(null, message);
		return this.#propagate({ namespace: null, message }, recipients);
	}

	stats(): WSStats {
		const namespaces: Record<string, number> = {};
		for (const conn of this.#connections.values()) {
			namespaces[conn.namespace] = (namespaces[conn.namespace] ?? 0) + 1;
		}
		return {
			connections: this.#connections.size,
			pending: this.#pending.size,
			rooms: this.#index.size,
			namespaces,
		};
	}

	/** Members of a room within a namespace. */
	members(room: string, namespace: string = DEFAULT_NAMESPACE): string[] {
		return [...(this.#index.get(room)?.get(namespace) ?? [])];
	}

	/** Closes every connection and releases all timers. */
	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;

		clearInterval(this.#sweepTimer);
		this.#sweepTimer = undefined;
		this.#unsubscribeRemote();

		for (const conn of [...this.#pending, ...this.#connections.values()]) {
			this.#close(conn, CLOSE.GOING_AWAY, "server shutting down");
		}
		this.#pending.clear();
		this.#connections.clear();
		this.#index.clear();

		await this.#adapter.close();
	}

	// -------------------------------------------------------------- internals

	async #propagate(
		envelope: WSBroadcastEnvelope,
		recipients: number,
	): Promise<number> {
		try {
			await this.#adapter.publish(envelope);
		} catch (e) {
			this.logger?.error?.(`adapter propagation failed: ${e}`);
		}
		return recipients;
	}

	async #onMessage(conn: Connection, raw: string | ArrayBuffer): Promise<void> {
		conn.lastSeen = Date.now();

		const size = typeof raw === "string" ? raw.length : raw.byteLength;
		if (size > this.#options.maxFrameSize) {
			return this.#close(conn, CLOSE.FRAME_TOO_LARGE, "frame too large");
		}

		if (this.#rateLimited(conn)) {
			return this.#close(conn, CLOSE.RATE_LIMITED, "rate limit exceeded");
		}

		let frame: ClientFrame;
		try {
			frame = this.#decode(raw) as ClientFrame;
		} catch {
			this.#send(conn, {
				type: FRAME.ERROR,
				error: { code: ERROR_CODE.BAD_REQUEST, message: "malformed frame" },
			});
			return this.#close(conn, CLOSE.PROTOCOL_ERROR, "malformed frame");
		}

		if (!frame || typeof frame.type !== "string") {
			return this.#send(conn, {
				type: FRAME.ERROR,
				error: { code: ERROR_CODE.BAD_REQUEST, message: "missing frame type" },
			});
		}

		if (frame.type === FRAME.AUTH) return await this.#onAuth(conn, frame);

		if (!conn.authed) {
			return this.#send(conn, {
				type: FRAME.ERROR,
				error: {
					code: ERROR_CODE.UNAUTHORIZED,
					message: "not authenticated",
				},
			});
		}

		switch (frame.type) {
			case FRAME.PING:
				return this.#send(conn, { type: FRAME.PONG });

			// `sub`/`unsub` are handled synchronously on purpose. The socket
			// preserves ordering, and handling these without an await means a
			// buffered publish that follows a re-subscribe can never overtake
			// it and land in a room the server has not registered yet.
			case FRAME.SUB:
				return this.#onSub(conn, frame.id, frame.rooms);

			case FRAME.UNSUB:
				return this.#onUnsub(conn, frame.id, frame.rooms);

			case FRAME.PUB:
				return await this.#onPub(conn, frame);

			case FRAME.BROADCAST:
				return await this.#onBroadcast(conn, frame);

			default:
				return this.#send(conn, {
					type: FRAME.ERROR,
					error: {
						code: ERROR_CODE.BAD_REQUEST,
						message: `unknown frame type`,
					},
				});
		}
	}

	async #onAuth(
		conn: Connection,
		frame: Extract<ClientFrame, { type: typeof FRAME.AUTH }>,
	): Promise<void> {
		clearTimeout(conn.authTimer);
		conn.authTimer = undefined;

		if (conn.authed) return; // ignore a repeated handshake

		let result: AuthResult | null = {};
		if (this.#verify) {
			try {
				result = await this.#verify(frame.payload, conn.request);
			} catch (e) {
				this.logger?.debug?.(`verify threw: ${e}`);
				result = null;
			}
		}

		if (result === null) {
			return this.#close(conn, CLOSE.AUTH_FAILED, "authentication failed");
		}

		const id = result.clientId ?? frame.clientId ?? conn.id;
		const namespace = result.namespace ?? frame.namespace ?? DEFAULT_NAMESPACE;

		// Same id reconnecting: the newcomer wins, the stale socket goes. This
		// is what makes a reconnect after a half-open drop actually recover
		// instead of accumulating ghosts.
		const existing = this.#connections.get(id);
		if (existing && existing !== conn) {
			this.logger?.debug?.(`replacing existing connection ${id}`);
			this.#close(existing, CLOSE.GOING_AWAY, "replaced by new connection");
		}

		conn.id = id;
		conn.namespace = namespace;
		conn.meta = result.meta ?? {};
		conn.authed = true;

		this.#pending.delete(conn);
		this.#connections.set(id, conn);

		this.logger?.debug?.(`authenticated ${id} in "${namespace}"`);
		this.#send(conn, {
			type: FRAME.HELLO,
			clientId: id,
			namespace,
			protocol: PROTOCOL_VERSION,
		});
	}

	#onSub(
		conn: Connection,
		id: string,
		requests: Array<{ room: string; presence?: boolean }>,
	): void {
		const syncRooms: string[] = [];

		for (const request of requests ?? []) {
			const room = request?.room;
			if (typeof room !== "string" || !room) continue;

			const isNew = !conn.rooms.has(room);
			conn.rooms.set(room, !!request.presence);
			this.#indexAdd(room, conn.namespace, conn.id);

			// Announce to the others first, so by the time the joiner gets its
			// snapshot everyone has a consistent view of the membership.
			if (isNew) {
				this.#notifyPresence(room, conn.namespace, PRESENCE.JOIN, conn.id);
			}
			if (request.presence) syncRooms.push(room);
		}

		// Sync goes out before the ack, so presence is already settled by the
		// time the caller's `subscribe()` resolves.
		for (const room of syncRooms) {
			this.#send(conn, {
				type: FRAME.PRESENCE,
				event: PRESENCE.SYNC,
				room,
				namespace: conn.namespace,
				clientId: null,
				members: this.members(room, conn.namespace),
				timestamp: Date.now(),
			});
		}

		this.#send(conn, { type: FRAME.ACK, id });
	}

	#onUnsub(conn: Connection, id: string, rooms: string[]): void {
		for (const room of rooms ?? []) {
			if (!conn.rooms.delete(room)) continue;
			this.#indexRemove(room, conn.namespace, conn.id);
			this.#notifyPresence(room, conn.namespace, PRESENCE.LEAVE, conn.id);
		}
		this.#send(conn, { type: FRAME.ACK, id });
	}

	async #onPub(
		conn: Connection,
		frame: Extract<ClientFrame, { type: typeof FRAME.PUB }>,
	): Promise<void> {
		// A client may only publish into its own namespace. Accepting the
		// requested one blindly would make the isolation boundary decorative.
		const namespace = conn.namespace;
		if (frame.namespace && frame.namespace !== namespace) {
			return this.#nack(conn, frame.id, {
				code: ERROR_CODE.FORBIDDEN,
				message: `cannot publish into namespace "${frame.namespace}"`,
			});
		}

		const message: WSMessage = {
			room: frame.room,
			namespace,
			from: conn.id,
			payload: frame.payload,
			timestamp: Date.now(),
		};
		const recipients = this.#deliverLocal(namespace, message);
		this.#send(conn, { type: FRAME.ACK, id: frame.id, recipients });
		await this.#propagate({ namespace, message }, recipients);
	}

	async #onBroadcast(
		conn: Connection,
		frame: Extract<ClientFrame, { type: typeof FRAME.BROADCAST }>,
	): Promise<void> {
		const ctx: WSConnectionContext = {
			clientId: conn.id,
			namespace: conn.namespace,
			meta: conn.meta,
			request: conn.request,
		};

		let allowed = false;
		try {
			allowed = this.#allowBroadcast
				? await this.#allowBroadcast(ctx, frame.room)
				: false;
		} catch (e) {
			this.logger?.debug?.(`allowBroadcast threw: ${e}`);
			allowed = false;
		}

		if (!allowed) {
			return this.#nack(conn, frame.id, {
				code: ERROR_CODE.FORBIDDEN,
				message: "broadcast not permitted",
			});
		}

		const message: WSMessage = {
			room: frame.room,
			namespace: "*",
			from: conn.id,
			payload: frame.payload,
			timestamp: Date.now(),
		};
		const recipients = this.#deliverLocal(null, message);
		this.#send(conn, { type: FRAME.ACK, id: frame.id, recipients });
		await this.#propagate({ namespace: null, message }, recipients);
	}

	/**
	 * Delivers to local sockets.
	 *
	 * @param namespace - target namespace, or `null` to cross all of them
	 * @returns how many sockets received it, on this instance
	 */
	#deliverLocal(namespace: string | null, message: WSMessage): number {
		const byNamespace = this.#index.get(message.room);
		if (!byNamespace) return 0;

		let count = 0;
		const deliver = (ids: Set<string>, ns: string) => {
			for (const id of ids) {
				const conn = this.#connections.get(id);
				if (!conn) continue;
				// Receivers always see the namespace they actually live in,
				// even for a broadcast that originated outside it.
				this.#send(conn, { type: FRAME.MSG, ...message, namespace: ns });
				count++;
			}
		};

		if (namespace === null) {
			for (const [ns, ids] of byNamespace) deliver(ids, ns);
		} else {
			const ids = byNamespace.get(namespace);
			if (ids) deliver(ids, namespace);
		}

		return count;
	}

	#notifyPresence(
		room: string,
		namespace: string,
		event: PresenceEventType,
		subjectId: string,
	): void {
		const ids = this.#index.get(room)?.get(namespace);
		if (!ids) return;

		const members = [...ids];
		const timestamp = Date.now();

		for (const id of ids) {
			// The subject gets a full `sync` instead of a delta about itself.
			if (id === subjectId) continue;
			const conn = this.#connections.get(id);
			if (!conn || conn.rooms.get(room) !== true) continue;
			this.#send(conn, {
				type: FRAME.PRESENCE,
				event,
				room,
				namespace,
				clientId: subjectId,
				members,
				timestamp,
			});
		}
	}

	#indexAdd(room: string, namespace: string, clientId: string): void {
		let byNamespace = this.#index.get(room);
		if (!byNamespace) {
			byNamespace = new Map();
			this.#index.set(room, byNamespace);
		}
		let ids = byNamespace.get(namespace);
		if (!ids) {
			ids = new Set();
			byNamespace.set(namespace, ids);
		}
		ids.add(clientId);
	}

	#indexRemove(room: string, namespace: string, clientId: string): void {
		const byNamespace = this.#index.get(room);
		if (!byNamespace) return;
		const ids = byNamespace.get(namespace);
		if (!ids) return;
		ids.delete(clientId);
		if (ids.size === 0) byNamespace.delete(namespace);
		if (byNamespace.size === 0) this.#index.delete(room);
	}

	#onClose(conn: Connection): void {
		clearTimeout(conn.authTimer);
		conn.authTimer = undefined;
		this.#pending.delete(conn);

		// Only drop the registry entry if it still points at *this* socket — a
		// replaced connection must not evict its replacement.
		if (this.#connections.get(conn.id) === conn) {
			this.#connections.delete(conn.id);
		}

		for (const room of conn.rooms.keys()) {
			this.#indexRemove(room, conn.namespace, conn.id);
			this.#notifyPresence(room, conn.namespace, PRESENCE.LEAVE, conn.id);
		}
		conn.rooms.clear();
	}

	#rateLimited(conn: Connection): boolean {
		const limit = this.#options.maxFramesPerSecond;
		if (limit <= 0) return false;
		const now = Date.now();
		if (now - conn.windowStart >= 1_000) {
			conn.windowStart = now;
			conn.windowCount = 0;
		}
		return ++conn.windowCount > limit;
	}

	#sweep(): void {
		const cutoff = Date.now() - this.#options.idleTimeout;
		for (const conn of [...this.#pending, ...this.#connections.values()]) {
			if (conn.lastSeen < cutoff) {
				this.logger?.debug?.(`reaping idle connection ${conn.id}`);
				this.#close(conn, CLOSE.IDLE_TIMEOUT, "idle timeout");
			}
		}
	}

	#nack(
		conn: Connection,
		id: string,
		error: { code: string; message: string },
	): void {
		this.#send(conn, { type: FRAME.NACK, id, error });
	}

	#send(conn: Connection, frame: ServerFrame): void {
		if (conn.socket.readyState !== WebSocket.OPEN) return;
		try {
			conn.socket.send(this.#encode(frame));
		} catch (e) {
			this.logger?.debug?.(`send failed (${conn.id}): ${e}`);
		}
	}

	#close(conn: Connection, code: number, reason: string): void {
		clearTimeout(conn.authTimer);
		conn.authTimer = undefined;
		try {
			if (
				conn.socket.readyState === WebSocket.OPEN ||
				conn.socket.readyState === WebSocket.CONNECTING
			) {
				conn.socket.close(code, reason);
			}
		} catch {
			// Already gone.
		}
		this.#onClose(conn);
	}
}
