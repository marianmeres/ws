/**
 * Cross-instance fan-out seam.
 *
 * The v1 server is in-memory and single-instance. This interface exists so a
 * Redis or Deno-KV adapter can drop in later without a breaking change, and it
 * is deliberately shaped like the adapters in `@marianmeres/stack-sse` so
 * extracting those is mechanical rather than a rewrite.
 *
 * Note the division of labour: **local delivery is the service's job**, always.
 * An adapter only propagates to *peer* instances and receives what peers send.
 * That is why `recipients` counts are instance-local and documented as
 * best-effort telemetry rather than a delivery guarantee.
 *
 * @module
 */

import type { WSMessage } from "../../protocol/frames.ts";

/** A message crossing the instance boundary. */
export interface WSBroadcastEnvelope {
	/** Target namespace, or `null` for a cross-namespace broadcast. */
	namespace: string | null;
	/** The message as it was delivered locally. */
	message: WSMessage;
}

/** Propagates messages between server instances. */
export interface WSPubSubAdapter {
	/**
	 * Hand off to peer instances. Local delivery has already happened.
	 *
	 * A rejection is logged and swallowed — a failed gossip must not fail the
	 * publish that already succeeded locally.
	 */
	publish(envelope: WSBroadcastEnvelope): Promise<void>;
	/**
	 * Register the sink for messages arriving from peers.
	 *
	 * @returns detaches the sink
	 */
	onRemote(cb: (envelope: WSBroadcastEnvelope) => void): () => void;
	/** Release connections and timers. Called by `WSService.close()`. */
	close(): Promise<void>;
}
