import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

/**
 * The npm package ships the **client only**.
 *
 * The reference server is Deno-only by construction — it needs
 * `Deno.upgradeWebSocket` and `@marianmeres/demino` — so there is nothing
 * coherent to publish for Node. Shipping a subpath that cannot compile, let
 * alone run, would be worse than not shipping it: `jsr:@marianmeres/ws/server`
 * remains the one way to get it.
 *
 * The client itself is runtime-agnostic (`WebSocket` is a global everywhere it
 * targets), so npm consumers lose nothing they could have used.
 */
await npmBuild({
	name: denoJson.name,
	version: denoJson.version,
	repository: denoJson.name.replace(/^@/, ""),
	sourceFiles: [
		"mod.ts",
		"protocol.ts",
		"protocol/mod.ts",
		"protocol/constants.ts",
		"protocol/frames.ts",
		"protocol/errors.ts",
		"client/ws-client.ts",
		"client/backoff.ts",
		"client/heartbeat.ts",
		"client/outbox.ts",
		"client/rooms.ts",
	],
	dependencies: versionizeDeps([
		"@marianmeres/clog",
		"@marianmeres/pubsub",
		"@marianmeres/ticker",
		"@marianmeres/uid",
	], denoJson),
	entryPoints: ["mod", "protocol"],
});
