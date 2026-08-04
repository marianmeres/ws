/**
 * The chat's state and everything that talks to the wire.
 *
 * Deliberately view-free: it owns a `WSClient` plus a handful of
 * `@marianmeres/vanilla` observables, and the views in `mod.ts` only read them.
 * Keeping the split means the interesting part — how a room subscription,
 * presence, an acknowledged publish and a reconnect actually behave — is
 * readable without any DOM in the way.
 *
 * @module
 */

import { type Observable, observable } from "@marianmeres/vanilla";
// A consumer would write `from "@marianmeres/ws"`. The example points at the
// local source so it always builds against the working tree.
import {
	createWSClient,
	type WSClient,
	type WSMessage,
	type WSPresenceEvent,
	type WSState,
} from "../../src/mod.ts";
import {
	type ChatPayload,
	DEFAULT_ROOM,
	isChatPayload,
	isTypingPayload,
} from "../shared.ts";

/** How long a "… is typing" marker survives without a refresh. */
const TYPING_TTL = 3_000;
/** Minimum gap between outgoing typing pings. */
const TYPING_THROTTLE = 1_500;

/** Who we are, as entered in the login form. */
export interface Session {
	nick: string;
	workspace: string;
}

/** One rendered line. Not a wire type — the view's shape, nothing more. */
export interface Entry {
	/** De-duplication key; the app-level message id when there is one. */
	key: string;
	kind: "chat" | "system" | "notice" | "error";
	/** Display author. */
	nick: string;
	text: string;
	ts: number;
	own: boolean;
	/**
	 * Recipient count from the publish acknowledgement, once it lands. `null`
	 * until then (and forever, for messages we did not send).
	 */
	recipients: number | null;
}

/** A client id is `nick#suffix`; the suffix only exists to keep ids unique. */
export const nickOf = (clientId: string | null): string =>
	clientId ? clientId.split("#")[0] : "server";

let entrySeq = 0;
const localKey = () => `local-${++entrySeq}`;

/** Stable de-dup key for a wire message: ours if it has one, else from+time. */
function messageKey(msg: WSMessage): string {
	const { payload } = msg;
	if (isChatPayload(payload) && payload.id) return payload.id;
	return `${msg.from ?? "server"}:${msg.timestamp}`;
}

/** Everything a view needs, plus the actions it can invoke. */
export interface ChatStore {
	readonly ws: WSClient;
	readonly session: Session;
	/** Live connection state, mirrored from the client's Svelte-store `state`. */
	readonly connection: Observable<WSState>;
	readonly room: Observable<string>;
	readonly entries: Observable<Entry[]>;
	/** Client ids present in the current room. */
	readonly members: Observable<string[]>;
	/** Nicknames currently typing (never includes us). */
	readonly typing: Observable<string[]>;
	/** `true` while a room switch is in flight. */
	readonly joining: Observable<boolean>;

	join(room: string): Promise<void>;
	send(text: string, broadcast: boolean): Promise<void>;
	signalTyping(): void;
	leave(): void;
}

/**
 * Builds the store and starts connecting.
 *
 * @param session   nickname + workspace from the login form
 * @param onTerminated  called when the server rejected us for good (`4001` /
 *                      `4003`) — the one exit that does not retry, so the view
 *                      has to take the user back to the login form.
 */
export function createChatStore(
	session: Session,
	onTerminated: (reason: string) => void,
): ChatStore {
	// Suffix appended to the nickname to form the client id. Held in memory, so
	// it is stable across reconnects (presence does not churn) and unique per
	// tab (two tabs on the same nickname would otherwise keep replacing each
	// other's connection). A reload gets a new one, which is fine — the old
	// socket is gone anyway. The server validates it and can override.
	const tab = Math.random().toString(36).slice(2, 8);

	const ws = createWSClient({
		url: "/ws",
		// The requested namespace. The server's `verify` has the final say — it
		// returns the validated workspace, and that is what we actually get.
		namespace: session.workspace,
		// Called before *every* (re)connect. A real app would return a fresh
		// token here; refresh needs no other machinery.
		auth: () => ({ nick: session.nick, workspace: session.workspace, tab }),
		// Bounds the `connect()` await in `join()` below. Reconnecting carries
		// on in the background regardless — this only stops the UI from waiting
		// on a server that is not there.
		connectTimeout: 8_000,
	});

	const connection = observable<WSState>({
		state: "idle",
		connected: false,
		connecting: false,
		attempt: 0,
		lastError: null,
	});
	const room = observable<string>(DEFAULT_ROOM);
	const entries = observable<Entry[]>([]);
	const members = observable<string[]>([]);
	const typing = observable<string[]>([]);
	const joining = observable(false);

	/** Keys already rendered — history and live traffic overlap by design. */
	let seen = new Set<string>();
	/** nick -> expiry timestamp. */
	const typingUntil = new Map<string, number>();
	let lastTypingSent = 0;

	// The client's `state` follows the Svelte store contract (fires immediately,
	// then on every change), so bridging it into an observable is one line.
	ws.state.subscribe((s) => connection.set(s));

	ws.on("terminated", ({ code, reason }) => {
		onTerminated(`${reason || "connection refused"} (close code ${code})`);
	});

	/** Appends a purely local line — never leaves the browser. */
	function notice(text: string, kind: Entry["kind"] = "notice"): void {
		push({
			key: localKey(),
			kind,
			nick: "",
			text,
			ts: Date.now(),
			own: false,
			recipients: null,
		});
	}

	function push(entry: Entry): void {
		if (seen.has(entry.key)) return;
		seen.add(entry.key);
		// Immutable update — the change guard is reference equality, so mutating
		// the array in place would fire nothing.
		entries.update((list) => [...list, entry]);
	}

	/** Turns a wire message into an entry, or `null` if it is not ours to show. */
	function toEntry(msg: WSMessage): Entry | null {
		const key = messageKey(msg);
		const fromServer = msg.from === null;

		if (isChatPayload(msg.payload)) {
			return {
				key,
				kind: fromServer ? "system" : "chat",
				// A server injection may name itself in the payload; peer
				// messages are labelled from the client id, which the server
				// controls and a client cannot spoof.
				nick: fromServer ? msg.payload.nick : nickOf(msg.from),
				text: msg.payload.text,
				ts: msg.timestamp,
				own: msg.from !== null && msg.from === ws.clientId,
				recipients: null,
			};
		}

		if (isTypingPayload(msg.payload)) return null; // handled separately

		// Something else entirely — most likely a hand-rolled curl payload.
		// Rendering it raw beats silently dropping it.
		return {
			key,
			kind: "system",
			nick: fromServer ? "server" : nickOf(msg.from),
			text: JSON.stringify(msg.payload),
			ts: msg.timestamp,
			own: false,
			recipients: null,
		};
	}

	function onMessage(msg: WSMessage): void {
		if (isTypingPayload(msg.payload)) {
			const nick = msg.payload.nick;
			if (msg.from === ws.clientId) return; // our own ping, echoed back
			typingUntil.set(nick, Date.now() + TYPING_TTL);
			pruneTyping();
			return;
		}
		const entry = toEntry(msg);
		if (entry) push(entry);
	}

	function onPresence(event: WSPresenceEvent): void {
		members.set([...event.members].sort());
		// `sync` carries the whole snapshot and fires again after every
		// reconnect, so announcing it would spam the log on every network blip.
		if (event.event === "join") notice(`→ ${nickOf(event.clientId)} joined`);
		if (event.event === "leave") notice(`← ${nickOf(event.clientId)} left`);
	}

	function pruneTyping(): void {
		const now = Date.now();
		for (const [nick, until] of typingUntil) {
			if (until <= now) typingUntil.delete(nick);
		}
		const next = [...typingUntil.keys()].sort();
		const current = typing.get();
		// Cheap equality check so the view is not woken for an unchanged list.
		if (next.length !== current.length || next.some((n, i) => n !== current[i])) {
			typing.set(next);
		}
	}
	const typingTimer = setInterval(pruneTyping, 1_000);

	/**
	 * Fetches the room backlog over plain HTTP.
	 *
	 * There is no protocol-level replay — an undelivered message is gone — so
	 * history is the application's problem. See `example/history.ts`.
	 */
	async function loadHistory(target: string): Promise<void> {
		const ns = ws.namespace;
		try {
			const res = await fetch(
				`/api/history/${encodeURIComponent(ns)}/${encodeURIComponent(target)}`,
			);
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const { messages } = await res.json() as { messages: WSMessage[] };
			if (room.get() !== target) return; // user switched away mid-fetch

			const backlog: Entry[] = [];
			for (const msg of messages) {
				const key = messageKey(msg);
				if (seen.has(key)) continue; // already arrived live
				seen.add(key);
				const entry = toEntry(msg);
				if (entry) backlog.push(entry);
			}
			if (!backlog.length) return;
			// History is older than anything already on screen, so it goes in
			// front rather than getting merged by timestamp.
			entries.update((list) => [...backlog, ...list]);
		} catch (err) {
			notice(`could not load history: ${(err as Error).message}`, "error");
		}
	}

	async function join(target: string): Promise<void> {
		const previous = room.get();
		joining.set(true);
		try {
			// Gate on a live socket before touching rooms.
			//
			// `connect()` is optional — the first `subscribe()` would start the
			// connection anyway — but while offline `subscribe()` resolves as
			// soon as the room is registered locally, leaving the real `sub`
			// frame for the next connect. The history fetch below would then
			// straddle the handshake and could miss a message published in
			// between. Waiting here removes that window; `connectTimeout`
			// keeps the wait bounded, and retrying continues regardless.
			try {
				await ws.connect();
			} catch (err) {
				// Either the bounded wait elapsed — retrying carries on in the
				// background — or the server refused us for good, which the
				// `terminated` handler is already dealing with. Go on either
				// way: the room registers locally regardless, and the
				// re-subscribe on the next connect establishes it.
				notice(`not connected yet — ${(err as Error).message}`, "notice");
			}

			if (ws.isSubscribed(previous)) await ws.unsubscribe(previous);

			room.set(target);
			entries.set([]);
			members.set([]);
			typingUntil.clear();
			typing.set([]);
			seen = new Set();

			// Subscribe *before* fetching history. `subscribe()` attaches the
			// handler synchronously, before any frame goes out, so a message
			// published while the backlog is in flight is queued into `entries`
			// rather than lost — and the key-based de-dup sorts out the overlap.
			await ws.subscribe(target, onMessage, { presence: onPresence });
			await loadHistory(target);
		} catch (err) {
			notice(`could not join #${target}: ${(err as Error).message}`, "error");
		} finally {
			joining.set(false);
		}
	}

	async function send(text: string, broadcast: boolean): Promise<void> {
		const trimmed = text.trim();
		if (!trimmed) return;

		const target = room.get();
		const payload: ChatPayload = {
			kind: "chat",
			id: crypto.randomUUID(),
			nick: session.nick,
			text: trimmed,
		};

		// Sends issued while offline are buffered and flushed after the
		// re-subscribe, so this is not an error — but it is worth saying, since
		// nothing will appear on screen until the socket is back.
		if (!ws.connected) notice("queued — will flush when the connection is back");

		try {
			// `publish` stays inside our namespace; `broadcast` crosses every
			// namespace and is refused unless the server's `allowBroadcast`
			// gate says otherwise.
			const { recipients } = broadcast
				? await ws.broadcast(target, payload)
				: await ws.publish(target, payload);

			// The message itself already arrived — the server delivers before it
			// acknowledges — so the ack only fills in the recipient count.
			entries.update((list) =>
				list.map((e) => (e.key === payload.id ? { ...e, recipients } : e))
			);
		} catch (err) {
			notice(`✗ ${(err as Error).message}`, "error");
		}
	}

	function signalTyping(): void {
		const now = Date.now();
		if (now - lastTypingSent < TYPING_THROTTLE) return;
		lastTypingSent = now;
		// Best-effort: a dropped typing ping is not worth surfacing, and it must
		// not produce an unhandled rejection while the socket is down.
		ws.publish(room.get(), { kind: "typing", nick: session.nick }).catch(
			() => {},
		);
	}

	function leave(): void {
		clearInterval(typingTimer);
		// Terminal, unlike `disconnect()` — handlers, rooms and buffered sends
		// are all released and the client cannot be reused.
		ws.dispose();
	}

	return {
		ws,
		session,
		connection,
		room,
		entries,
		members,
		typing,
		joining,
		join,
		send,
		signalTyping,
		leave,
	};
}
