import { createWSApp, type WSAppOptions } from "../src/server/mod.ts";
import type { WSService } from "../src/server/mod.ts";
import { PROTOCOL_VERSION } from "../src/protocol/constants.ts";
import type { WSErrorInfo } from "../src/protocol/frames.ts";

export interface TestServer {
	url: string;
	httpUrl: string;
	service: WSService;
	stop(): Promise<void>;
}

/** Grabs a port, releases it, and hands back the number. */
export function freePort(): number {
	const listener = Deno.listen({ port: 0 });
	const { port } = listener.addr as Deno.NetAddr;
	listener.close();
	return port;
}

/** Boots the reference server on `port` (or an ephemeral one). */
export function startServer(
	options: WSAppOptions = {},
	port = 0,
): TestServer {
	const { app, service } = createWSApp("/ws", [], {
		logger: null,
		deminoOptions: { logger: null },
		...options,
	});
	const server = Deno.serve({ port, onListen: () => {}, hostname: "127.0.0.1" }, app);
	const actual = (server.addr as Deno.NetAddr).port;

	return {
		url: `ws://127.0.0.1:${actual}/ws`,
		httpUrl: `http://127.0.0.1:${actual}/ws`,
		service,
		async stop() {
			await service.close();
			await server.shutdown();
		},
	};
}

/**
 * A server that completes the handshake and then goes deliberately silent —
 * never answering a ping, never closing. This is a half-open connection, the
 * failure the heartbeat exists to catch.
 */
export function startSilentServer(port = 0): {
	url: string;
	stop(): Promise<void>;
} {
	const sockets = new Set<WebSocket>();

	const server = Deno.serve(
		{ port, onListen: () => {}, hostname: "127.0.0.1" },
		(req) => {
			if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
				return new Response("nope", { status: 426 });
			}
			const { socket, response } = Deno.upgradeWebSocket(req);
			sockets.add(socket);
			socket.onmessage = (event) => {
				const frame = JSON.parse(event.data);
				// Answer the handshake so the client reaches "open"...
				if (frame.type === "auth") {
					socket.send(JSON.stringify({
						type: "hello",
						clientId: "silent",
						namespace: "default",
						protocol: 1,
					}));
				}
				// ...and then ignore everything else, pings included.
			};
			socket.onclose = () => sockets.delete(socket);
			return response;
		},
	);

	const { port: actual } = server.addr as Deno.NetAddr;
	return {
		url: `ws://127.0.0.1:${actual}/ws`,
		async stop() {
			for (const socket of sockets) {
				try {
					socket.close();
				} catch { /* already gone */ }
			}
			sockets.clear();
			await server.shutdown();
		},
	};
}

/**
 * A server that completes the handshake and then closes the socket the moment
 * it is asked to publish — so the frame is definitely transmitted and its ack
 * definitely cannot arrive.
 */
export function startNoAckServer(port = 0): {
	url: string;
	stop(): Promise<void>;
} {
	const sockets = new Set<WebSocket>();

	const server = Deno.serve(
		{ port, onListen: () => {}, hostname: "127.0.0.1" },
		(req) => {
			if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
				return new Response("nope", { status: 426 });
			}
			const { socket, response } = Deno.upgradeWebSocket(req);
			sockets.add(socket);
			socket.onmessage = (event) => {
				const frame = JSON.parse(event.data);
				if (frame.type === "auth") {
					socket.send(JSON.stringify({
						type: "hello",
						clientId: "no-ack",
						namespace: "default",
						protocol: PROTOCOL_VERSION,
					}));
				} else if (frame.type === "pub") {
					socket.close(1001, "gone mid-publish");
				}
			};
			socket.onclose = () => sockets.delete(socket);
			return response;
		},
	);

	const { port: actual } = server.addr as Deno.NetAddr;
	return {
		url: `ws://127.0.0.1:${actual}/ws`,
		async stop() {
			for (const socket of sockets) {
				try {
					socket.close();
				} catch { /* already gone */ }
			}
			sockets.clear();
			await server.shutdown();
		},
	};
}

/** Polls until `predicate` holds, or throws. */
export async function until(
	predicate: () => boolean,
	message = "condition not met",
	timeout = 5_000,
	step = 10,
): Promise<void> {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await new Promise((r) => setTimeout(r, step));
	}
	throw new Error(`Timed out waiting: ${message}`);
}

export function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

/** A frame as it came off the wire, before anything trusts its shape. */
export interface RawFrame {
	type: string;
	id?: string;
	error?: WSErrorInfo;
	clientId?: string;
	namespace?: string;
	[key: string]: unknown;
}

/**
 * A socket without the client library, so a test can send frames the client
 * would never produce — the only way to reach the server's input validation.
 */
export interface RawSocket {
	/** Every frame received so far, in order. */
	frames: RawFrame[];
	/** Set once the socket closed. */
	closed: { code: number; reason: string } | null;
	/** Sends anything at all, JSON-encoded. */
	send(frame: unknown): void;
	/** Resolves with the first received frame matching `match`. */
	waitFor(match: (frame: RawFrame) => boolean, message?: string): Promise<RawFrame>;
	/** Resolves once the socket is closed, with the code and reason. */
	waitClosed(message?: string): Promise<{ code: number; reason: string }>;
	/** Completes the handshake and resolves with the `hello` frame. */
	auth(frame?: Record<string, unknown>): Promise<RawFrame>;
	close(): Promise<void>;
}

/** Opens a {@link RawSocket} and resolves once it is connected. */
export async function rawConnect(url: string): Promise<RawSocket> {
	const socket = new WebSocket(url);
	const frames: RawFrame[] = [];
	let closed: { code: number; reason: string } | null = null;

	socket.onmessage = (event: MessageEvent) => frames.push(JSON.parse(event.data));
	socket.onclose = (event: CloseEvent) => {
		closed = { code: event.code, reason: event.reason };
	};

	await new Promise<void>((resolve, reject) => {
		socket.onopen = () => resolve();
		socket.onerror = () => reject(new Error(`raw socket failed to open: ${url}`));
	});

	const raw: RawSocket = {
		frames,
		get closed() {
			return closed;
		},
		send(frame) {
			socket.send(JSON.stringify(frame));
		},
		async waitFor(match, message = "matching frame") {
			await until(() => frames.some(match), message);
			const found = frames.find(match);
			if (!found) throw new Error(`no frame matched: ${message}`);
			return found;
		},
		async waitClosed(message = "socket close") {
			await until(() => closed !== null, message);
			if (!closed) throw new Error(`socket did not close: ${message}`);
			return closed;
		},
		auth(frame = {}) {
			raw.send({
				type: "auth",
				id: "auth-1",
				protocol: PROTOCOL_VERSION,
				payload: null,
				...frame,
			});
			return raw.waitFor((f) => f.type === "hello", "hello");
		},
		async close() {
			if (closed === null) {
				socket.close();
				await until(() => closed !== null, "raw socket close");
			}
		},
	};

	return raw;
}
