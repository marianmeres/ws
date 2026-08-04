/**
 * In-memory chat history, implemented as a **decorating `WSPubSubAdapter`**.
 *
 * The library deliberately offers no replay — delivery is at-most-once and the
 * server forgets a message the instant it is fanned out. History is therefore
 * the application's job, and this file is one honest way to do it.
 *
 * Why the adapter seam rather than a hook? Because it is the one place every
 * message passes through: `WSService` calls `adapter.publish(envelope)` after
 * local delivery for *every* publish and broadcast, whoever originated it —
 * a connected client, `service.publish()`, or the HTTP injection route. A real
 * deployment would put Redis or Deno KV here; we tee into a ring buffer and
 * delegate the actual (non-)propagation to the wrapped adapter.
 *
 * Note this is application code inspecting an application payload. The library
 * itself still never looks inside `payload` — see `example/shared.ts`.
 *
 * @module
 */

import {
	type WSBroadcastEnvelope,
	type WSPubSubAdapter,
	WSPubSubLocal,
} from "../src/server.ts";
import type { WSMessage } from "../src/protocol.ts";
import { HISTORY_LIMIT, isChatPayload } from "./shared.ts";

/** Composite key for the buffer map. `|` cannot appear in a validated namespace. */
const key = (namespace: string, room: string) => `${namespace}|${room}`;

/**
 * Records chat messages per `(namespace, room)` and forwards everything to the
 * wrapped adapter unchanged.
 */
export class HistoryAdapter implements WSPubSubAdapter {
	readonly #inner: WSPubSubAdapter;
	readonly #limit: number;
	readonly #buffers = new Map<string, WSMessage[]>();

	constructor(inner: WSPubSubAdapter = new WSPubSubLocal(), limit = HISTORY_LIMIT) {
		this.#inner = inner;
		this.#limit = limit;
	}

	publish(envelope: WSBroadcastEnvelope): Promise<void> {
		this.#record(envelope);
		// Delegate rather than resolve: whatever real adapter is wrapped still
		// has to see every envelope, or peer instances go deaf.
		return this.#inner.publish(envelope);
	}

	onRemote(cb: (envelope: WSBroadcastEnvelope) => void): () => void {
		// Messages arriving *from* peers are history too — a client that
		// reconnects onto this instance should still see them.
		return this.#inner.onRemote((envelope) => {
			this.#record(envelope);
			cb(envelope);
		});
	}

	close(): Promise<void> {
		this.#buffers.clear();
		return this.#inner.close();
	}

	/** Oldest first. Returns a copy, so callers cannot corrupt the buffer. */
	read(namespace: string, room: string): WSMessage[] {
		return [...(this.#buffers.get(key(namespace, room)) ?? [])];
	}

	#record({ namespace, message }: WSBroadcastEnvelope): void {
		// Only real chat lines are worth keeping — typing pings are ephemeral by
		// definition, and unknown shapes are not ours to interpret.
		if (!isChatPayload(message.payload)) return;

		// `namespace: null` means a cross-namespace broadcast, which belongs to
		// no single room history. Announcements are live-only, by design.
		if (namespace === null) return;

		const k = key(namespace, message.room);
		const buffer = this.#buffers.get(k) ?? [];
		buffer.push(message);
		if (buffer.length > this.#limit) buffer.splice(0, buffer.length - this.#limit);
		this.#buffers.set(k, buffer);
	}
}
