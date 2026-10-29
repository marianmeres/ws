import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

/**
 * The npm package ships all three entry points: the client (`.`), the wire
 * definitions (`./protocol`) and the reference server (`./server`).
 *
 * Nothing under `src/` is Deno-bound any more — the server upgrades through
 * demino 2.0's `info.upgradeWebSocket` — so the whole `src/` tree ships
 * (npmbuild's default when `sourceFiles` is omitted).
 *
 * `@marianmeres/demino` is a runtime dependency because `./server` builds a
 * demino app. `ws` is not: it is the optional peer of
 * `@marianmeres/demino/node`, installed by whoever serves through that
 * adapter (`deno task npm:smoke` does exactly that).
 */
await npmBuild({
	name: denoJson.name,
	version: denoJson.version,
	repository: denoJson.name.replace(/^@/, ""),
	dependencies: versionizeDeps([
		"@marianmeres/clog",
		"@marianmeres/demino",
		"@marianmeres/pubsub",
		"@marianmeres/ticker",
		"@marianmeres/uid",
	], denoJson),
	entryPoints: ["mod", "protocol", "server"],
	packageJsonOverrides: {
		// the client uses the global `WebSocket` (unflagged in Node 22), and so
		// does `@marianmeres/demino`, which declares the same floor
		engines: { node: ">=22" },
	},
});
