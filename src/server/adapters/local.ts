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
	/** No-op: there are no peers to tell. */
	publish(_envelope: WSBroadcastEnvelope): Promise<void> {
		return Promise.resolve();
	}

	/** No-op: nothing ever arrives from elsewhere. @returns a no-op detach */
	onRemote(_cb: (envelope: WSBroadcastEnvelope) => void): () => void {
		return () => {};
	}

	/** No-op: nothing is held. */
	close(): Promise<void> {
		return Promise.resolve();
	}
}
