import assert from "node:assert/strict";
import { test } from "node:test";

import { createWSClient } from "../src/mod.ts";
import type { WSDecoder, WSEncoder, WSRoomMessage } from "../src/protocol/frames.ts";
import { startBinaryHelloServer, startServer, until } from "./_helpers.ts";

/** The simplest codec that puts a real binary frame on the wire. */
const encode: WSEncoder = (frame) => new TextEncoder().encode(JSON.stringify(frame));
const decode: WSDecoder = (raw) =>
	JSON.parse(typeof raw === "string" ? raw : new TextDecoder().decode(raw));

test("the stock client connects to a server that answers in binary", async () => {
	const server = await startBinaryHelloServer();
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
		assert.deepEqual(c.connected, true);
		assert.deepEqual(c.clientId, "binary");
	} finally {
		c.dispose();
		await server.stop();
	}
});

test("a matching binary codec works end to end", async () => {
	const server = await startServer({ encode, decode });
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
		const seen: WSRoomMessage[] = [];
		await c.subscribe("chat", (m) => seen.push(m));

		const { recipients } = await c.publish("chat", { text: "binary" });
		assert.deepEqual(recipients, 1);

		await until(() => seen.length === 1, "binary delivery");
		assert.deepEqual(seen[0].payload, { text: "binary" });
	} finally {
		c.dispose();
		await server.stop();
	}
});
