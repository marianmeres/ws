/**
 * Refcounted room registry.
 *
 * N handlers on one room produce exactly one wire subscription; the `unsub`
 * frame goes out when the last handler detaches. This is what lets
 * `subscribe()` hand back a plain unsubscriber that is safe to call from
 * component teardown without any coordination between call sites.
 *
 * @module
 */

import type { SubRequest, WSMessage, WSPresenceEvent } from "../protocol/frames.ts";

/** Receives messages published to a room. */
export type MessageHandler<T = unknown> = (msg: WSMessage<T>) => void;

/** Receives membership changes for a room subscribed with presence enabled. */
export type PresenceHandler = (event: WSPresenceEvent) => void;

interface RoomEntry {
	handlers: Set<MessageHandler>;
	presenceHandlers: Set<PresenceHandler>;
	/** Latest known membership, refreshed by every presence event. */
	members: string[];
}

/** What changed as a result of an `add()`, and therefore what the wire needs. */
export interface AddResult {
	/** The room is new — a `sub` frame is required. */
	created: boolean;
	/** Presence was requested for a room that did not have it — re-`sub`. */
	presenceUpgraded: boolean;
}

/** Tracks rooms, their handlers, and their known membership. */
export class RoomRegistry {
	#rooms = new Map<string, RoomEntry>();

	/** Rooms currently held, in insertion order. */
	get rooms(): string[] {
		return [...this.#rooms.keys()];
	}

	has(room: string): boolean {
		return this.#rooms.has(room);
	}

	wantsPresence(room: string): boolean {
		const entry = this.#rooms.get(room);
		return !!entry && entry.presenceHandlers.size > 0;
	}

	members(room: string): string[] {
		return [...(this.#rooms.get(room)?.members ?? [])];
	}

	/**
	 * Attaches handlers, creating the room entry if needed.
	 *
	 * Presence is enabled by *providing a presence handler* rather than by a
	 * separate boolean — one way to express the intent instead of two that can
	 * disagree.
	 */
	add(
		room: string,
		handler: MessageHandler,
		presenceHandler?: PresenceHandler,
	): AddResult {
		let entry = this.#rooms.get(room);
		const created = !entry;
		const hadPresence = !!entry && entry.presenceHandlers.size > 0;

		if (!entry) {
			entry = { handlers: new Set(), presenceHandlers: new Set(), members: [] };
			this.#rooms.set(room, entry);
		}

		entry.handlers.add(handler);
		if (presenceHandler) entry.presenceHandlers.add(presenceHandler);

		return {
			created,
			presenceUpgraded: !created && !hadPresence && !!presenceHandler,
		};
	}

	/**
	 * Detaches handlers.
	 *
	 * @returns `true` when the room became empty and should be unsubscribed.
	 */
	remove(
		room: string,
		handler: MessageHandler,
		presenceHandler?: PresenceHandler,
	): boolean {
		const entry = this.#rooms.get(room);
		if (!entry) return false;

		entry.handlers.delete(handler);
		if (presenceHandler) entry.presenceHandlers.delete(presenceHandler);

		if (entry.handlers.size === 0 && entry.presenceHandlers.size === 0) {
			this.#rooms.delete(room);
			return true;
		}
		return false;
	}

	/** Force-removes a room and every handler attached to it. */
	removeRoom(room: string): boolean {
		return this.#rooms.delete(room);
	}

	/**
	 * The full subscription set, for re-subscribing after a reconnect in a
	 * single batched frame rather than one round-trip per room.
	 */
	subRequests(): SubRequest[] {
		return [...this.#rooms.entries()].map(([room, entry]) => ({
			room,
			...(entry.presenceHandlers.size > 0 ? { presence: true } : {}),
		}));
	}

	/**
	 * Delivers to every handler for the room.
	 *
	 * Handlers are snapshotted first, so a handler that unsubscribes (or
	 * subscribes) during delivery cannot corrupt the in-flight iteration.
	 */
	deliver(room: string, msg: WSMessage, onError: (e: unknown) => void): void {
		const entry = this.#rooms.get(room);
		if (!entry) return;
		for (const handler of [...entry.handlers]) {
			try {
				handler(msg);
			} catch (e) {
				onError(e);
			}
		}
	}

	/** Updates cached membership, then delivers to presence handlers. */
	deliverPresence(
		room: string,
		event: WSPresenceEvent,
		onError: (e: unknown) => void,
	): void {
		const entry = this.#rooms.get(room);
		if (!entry) return;
		entry.members = event.members;
		for (const handler of [...entry.presenceHandlers]) {
			try {
				handler(event);
			} catch (e) {
				onError(e);
			}
		}
	}

	clear(): void {
		this.#rooms.clear();
	}
}
