import { createWSApp, type WSAppOptions } from "../src/server/mod.ts";
import type { WSService } from "../src/server/mod.ts";

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
