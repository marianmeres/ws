/// <reference lib="dom" />
/// <reference lib="dom.iterable" />

/**
 * Client entry point — the views, and nothing else.
 *
 * Bundled to `public/dist/bundle.js` by `@marianmeres/deno-build`:
 * ```
 * deno task example:build     # or example:watch
 * ```
 *
 * Two views swap in `#app`: a login form, then the chat. All state lives in
 * `store.ts`; here we only clone templates and wire them up with
 * `@marianmeres/vanilla`.
 *
 * The look comes from `@marianmeres/vanilla-ui`'s base style layer (copied to
 * `public/vui-base.css` by `example/build-styles.ts`): the markup in
 * `index.html` carries its `.vui-*` classes, and the two places this file
 * builds or repaints a control — the room buttons and the connection badge —
 * go through the kit rather than through CSS of their own.
 *
 * @module
 */

import {
	applyBindings,
	computed,
	createView,
	delegate,
	fromTemplate,
	observable,
	reactTo,
	refs,
	type ViewInstance,
} from "@marianmeres/vanilla";
// The kit's button primitive: markup plus the `.vui-btn` classes vui-base.css
// paints, so a button built here and one written in index.html are the same
// button. (`@marianmeres/vanilla-ui` re-exports all of vanilla too — the kit's
// runtime `.html` components are the part this bundled client does not use.)
import { button } from "@marianmeres/vanilla-ui";
import type { WSConnectionState } from "../../src/mod.ts";
import {
	BROADCAST_ROOM,
	DEFAULT_ROOM,
	DEFAULT_WORKSPACE,
	NICK_RE,
	ROOMS,
	WORKSPACE_RE,
} from "../shared.ts";
import { type ChatStore, createChatStore, type Entry, nickOf } from "./store.ts";
import type { Session } from "./store.ts";

const SESSION_KEY = "ws-chat-session";
const root = document.getElementById("app")!;

const time = new Intl.DateTimeFormat(undefined, {
	hour: "2-digit",
	minute: "2-digit",
});

/* ------------------------------------------------------------------ login */

interface LoginProps {
	initial: Session;
	error: string;
	onSubmit: (session: Session) => void;
}

function createLogin({ initial, error, onSubmit }: LoginProps): ViewInstance {
	return createView((track) => {
		const el = fromTemplate("tpl-login");
		const r = refs(el);

		(r.nick as HTMLInputElement).value = initial.nick;
		(r.workspace as HTMLInputElement).value = initial.workspace;
		r.error.textContent = error;
		r.error.hidden = !error;

		track(
			delegate(el, {
				submit: (e: Event) => {
					e.preventDefault();
					const nick = (r.nick as HTMLInputElement).value.trim();
					const workspace = (r.workspace as HTMLInputElement).value.trim() ||
						DEFAULT_WORKSPACE;

					// The same rules the server enforces in `verify`. Checking
					// here too is purely for the error message — the server is
					// still the authority, and a nick it dislikes closes the
					// socket with a terminal 4001.
					if (!NICK_RE.test(nick)) {
						return fail("2–16 chars: letters, digits, _ or -");
					}
					if (!WORKSPACE_RE.test(workspace)) {
						return fail("workspace: 2–24 chars, same alphabet");
					}
					onSubmit({ nick, workspace });
				},
			}),
		);

		function fail(message: string) {
			r.error.textContent = message;
			r.error.hidden = false;
		}

		queueMicrotask(() => (r.nick as HTMLInputElement).focus());
		return { el };
	});
}

/* ------------------------------------------------------------------- chat */

interface ChatProps {
	store: ChatStore;
	onLeave: () => void;
}

/**
 * Connection state → the semantic role that tints the header badge. `open` is
 * handled by `connected` instead (the socket can be open while a re-subscribe
 * is still in flight); the states left out get the plain badge.
 */
const STATUS_ROLE: Partial<Record<WSConnectionState, string>> = {
	connecting: "warning",
	authenticating: "warning",
	reconnecting: "warning",
	terminated: "destructive",
	disposed: "destructive",
};

function createChat({ store, onLeave }: ChatProps): ViewInstance {
	return createView((track) => {
		const el = fromTemplate("tpl-chat");
		const r = refs(el);
		const broadcast = observable(false);

		r.you.textContent = store.session.nick;
		r.workspace.textContent = store.session.workspace;

		// Room buttons, straight off the shared list. `delegate` below does the
		// listening, so they only need the two data attributes.
		for (const name of ROOMS) {
			const b = button(`#${name}`, { variant: "ghost" });
			b.dataset.on = "click:pickRoom";
			b.dataset.room = name;
			r.rooms.appendChild(b);
		}

		track(
			delegate(el, {
				pickRoom: (_e, target) => {
					const next = target.dataset.room!;
					if (next !== store.room.get()) void store.join(next);
				},
				send: (e: Event) => {
					e.preventDefault();
					const input = r.input as HTMLInputElement;
					const text = input.value;
					input.value = "";
					input.focus();
					void store.send(text, broadcast.get());
				},
				typing: () => store.signalTyping(),
				toggleBroadcast: (_e, target) => {
					broadcast.set((target as HTMLInputElement).checked);
				},
				leave: onLeave,
			}),
		);

		/* --- connection badge ------------------------------------------- */
		// `store.connection` mirrors the client's reactive state, so a dropped
		// socket, the backoff countdown and the recovery all show up here with
		// no polling.
		track(
			store.connection.subscribe(({ state, connected, attempt, lastError }) => {
				// One of vui-base.css's five badge roles, or the plain badge.
				const role = connected ? "success" : STATUS_ROLE[state];
				r.status.className = `status vui-badge${
					role ? ` vui-badge--${role}` : ""
				}`;
				r.status.textContent = connected
					? "connected"
					: attempt > 0
					? `reconnecting… (attempt ${attempt})`
					: state;
				r.status.title = lastError?.message ?? "";
				// Sending while offline is legal — the outbox buffers it — so
				// the composer deliberately stays enabled.
				(r.send as HTMLButtonElement).disabled = false;
			}),
		);

		/* --- room highlight + labels ------------------------------------ */
		track(
			reactTo([store.room, store.joining], () => {
				const current = store.room.get();
				const busy = store.joining.get();
				r.roomName.textContent = `#${current}`;
				el.querySelectorAll<HTMLButtonElement>("[data-room]").forEach((b) => {
					b.setAttribute(
						"aria-pressed",
						String(b.dataset.room === current),
					);
					b.disabled = busy;
				});
				// The broadcast gate is server-side; this is only a hint.
				r.broadcastHint.textContent = current === BROADCAST_ROOM
					? "allowed here"
					: `denied outside #${BROADCAST_ROOM}`;
			}),
		);

		/* --- messages ---------------------------------------------------- */
		// Rendering the whole list on every change is the honest thing at this
		// size — no diffing, no keys, no surprises.
		track(
			store.entries.subscribe((list) => {
				const atBottom = r.log.scrollHeight - r.log.scrollTop -
						r.log.clientHeight < 40;
				r.log.replaceChildren(...list.map(renderEntry));
				r.empty.hidden = list.length > 0;
				if (atBottom) r.log.scrollTop = r.log.scrollHeight;
			}),
		);

		function renderEntry(entry: Entry): HTMLElement {
			const li = fromTemplate<HTMLLIElement>("tpl-entry");
			li.dataset.kind = entry.kind;
			li.classList.toggle("own", entry.own);
			applyBindings(li, {
				nick: entry.nick,
				text: entry.text,
				when: time.format(entry.ts),
				noAuthor: entry.kind !== "chat" && entry.kind !== "system",
				// The publish acknowledgement carries how many sockets the
				// server handed the message to — us included.
				ack: entry.recipients === null ? "" : `✓ ${entry.recipients}`,
				noAck: entry.recipients === null,
			});
			return li;
		}

		/* --- presence ---------------------------------------------------- */
		const memberCount = computed([store.members], () => store.members.get().length);

		track(
			store.members.subscribe((ids) => {
				r.members.replaceChildren(...ids.map((id) => {
					const li = fromTemplate<HTMLLIElement>("tpl-member");
					li.classList.toggle("own", id === store.ws.clientId);
					applyBindings(li, { nick: nickOf(id), id, title: id });
					return li;
				}));
			}),
		);
		track(
			memberCount.subscribe((n) => {
				r.memberCount.textContent = `${n} here`;
			}),
		);

		/* --- typing indicator -------------------------------------------- */
		track(
			store.typing.subscribe((nicks) => {
				r.typing.textContent = nicks.length === 0
					? ""
					: nicks.length === 1
					? `${nicks[0]} is typing…`
					: `${nicks.slice(0, 3).join(", ")} are typing…`;
			}),
		);

		// Join the default room once the view is live.
		void store.join(DEFAULT_ROOM);
		queueMicrotask(() => (r.input as HTMLInputElement).focus());

		return { el };
	});
}

/* ------------------------------------------------------------------- app */

/**
 * Swaps between the two views. Page-level and intentionally not tracked — it
 * lives as long as the tab does.
 */
const current = observable<ViewInstance | null>(null);
let store: ChatStore | null = null;

function show(view: ViewInstance): void {
	current.get()?.destroy();
	current.set(view);
	root.replaceChildren(view.el!);
}

function loadSession(): Session {
	try {
		const saved = JSON.parse(localStorage.getItem(SESSION_KEY) || "null");
		if (saved && typeof saved.nick === "string") {
			return { nick: saved.nick, workspace: saved.workspace || DEFAULT_WORKSPACE };
		}
	} catch { /* ignore */ }
	return { nick: "", workspace: DEFAULT_WORKSPACE };
}

function showLogin(error = ""): void {
	store = null;
	show(createLogin({ initial: loadSession(), error, onSubmit: startChat }));
}

function startChat(session: Session): void {
	try {
		localStorage.setItem(SESSION_KEY, JSON.stringify(session));
	} catch { /* ignore */ }

	store = createChatStore(session, (reason) => {
		// Terminal close — the client gave up on purpose and will not retry, so
		// the only sensible move is back to the form.
		store?.leave();
		showLogin(`Server refused the connection: ${reason}`);
	});

	show(createChat({
		store,
		onLeave: () => {
			store?.leave();
			showLogin();
		},
	}));
}

showLogin();

// Poke at it from the console: `chat.ws.dump()`, `chat.ws.disconnect()`, …
Object.defineProperty(globalThis, "chat", { get: () => store });
