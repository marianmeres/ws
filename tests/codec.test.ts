import { assertEquals } from "@std/assert";

import { createWSClient } from "../src/mod.ts";
import type { WSDecoder, WSEncoder, WSMessage } from "../src/protocol/frames.ts";
import { startBinaryHelloServer, startServer, until } from "./_helpers.ts";

/** The simplest codec that puts a real binary frame on the wire. */
const encode: WSEncoder = (frame) => new TextEncoder().encode(JSON.stringify(frame));
const decode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

Deno.test("the stock client connects to a server that answers in binary", async () => {
	const server = startBinaryHelloServer();
	// Bounded so a regression fails here instead of hanging: a client that
	// cannot decode the `hello` never leaves `authenticating`.
	const c = createWSClient({
		url: server.url,
		logger: null,
		pingInterval: 0,
		connectTimeout: 3_000,
	});

	try {
		await c.connect();
		assertEquals(c.connected, true);
		assertEquals(c.clientId, "binary");
	} finally {
		c.dispose();
		await server.stop();
	}
});

Deno.test("a matching binary codec works end to end", async () => {
	const server = startServer({ encode, decode });
	const c = createWSClient({
		url: server.url,
		logger: null,
		pingInterval: 0,
		connectTimeout: 3_000,
		encode,
		decode,
	});

	try {
		await c.connect();
		const seen: WSMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		const { recipients } = await c.publish("chat", { text: "binary" });
		assertEquals(recipients, 1);

		await until(() => seen.length === 1, "binary delivery");
		assertEquals(seen[0].payload, { text: "binary" });
	} finally {
		c.dispose();
		await server.stop();
	}
});
