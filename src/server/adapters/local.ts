/**
 * Single-instance adapter.
 *
 * @module
 */

import type { WSBroadcastEnvelope, WSPubSubAdapter } from "./abstract.ts";

/**
 * The default adapter: there are no peer instances, so propagation is a no-op
 * and nothing ever arrives from elsewhere.
 *
 * Everything still works — the service delivers locally regardless of adapter.
 * This one simply declines to gossip.
 */
export class WSPubSubLocal implements WSPubSubAdapter {
	publish(_envelope: WSBroadcastEnvelope): Promise<void> {
		// No peers to tell.
		return Promise.resolve();
	}

	onRemote(_cb: (envelope: WSBroadcastEnvelope) => void): () => void {
		return () => {};
	}

	close(): Promise<void> {
		// Nothing held.
		return Promise.resolve();
	}
}
