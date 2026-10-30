import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

const DEMINO = "@marianmeres/demino";
const [deminoSpec] = versionizeDeps([DEMINO], denoJson);
const deminoRange = deminoSpec.slice(DEMINO.length + 1);
if (!deminoSpec.startsWith(`${DEMINO}@`) || !deminoRange) {
	throw new Error(`deno.json imports: no version for "${DEMINO}"`);
}

/**
 * The npm package ships all three entry points: the client (`.`), the wire
 * definitions (`./protocol`) and the reference server (`./server`).
 *
 * Nothing under `src/` is Deno-bound any more — the server upgrades through
 * demino 2.0's `info.upgradeWebSocket` — so the whole `src/` tree ships
 * (npmbuild's default when `sourceFiles` is omitted).
 *
 * `@marianmeres/demino` is an optional peer, not a dependency: `./server`
 * builds a demino app that the host composes into its own demino, so the two
 * must be one copy (a regular dependency only dedupes when npm happens to
 * manage it), and a client-only install should not pull a server framework.
 * Whoever imports `./server` installs it, as they install `ws`, the optional
 * peer of `@marianmeres/demino/node` (`deno task npm:smoke` does both). It is
 * also a devDependency so the build's own `tsc` pass resolves its types.
 */
await npmBuild({
	name: denoJson.name,
	version: denoJson.version,
	repository: denoJson.name.replace(/^@/, ""),
	dependencies: versionizeDeps([
		"@marianmeres/clog",
		"@marianmeres/pubsub",
		"@marianmeres/ticker",
		"@marianmeres/uid",
	], denoJson),
	peerDependencies: { [DEMINO]: deminoRange },
	peerDependenciesMeta: { [DEMINO]: { optional: true } },
	entryPoints: ["mod", "protocol", "server"],
	packageJsonOverrides: {
		// the client uses the global `WebSocket` (unflagged in Node 22), and so
		// does `@marianmeres/demino`, which declares the same floor
		engines: { node: ">=22" },
		devDependencies: { [DEMINO]: deminoRange },
	},
});
