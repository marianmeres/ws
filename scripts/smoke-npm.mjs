/**
 * Smoke-tests the built npm package (`deno task npm:smoke`, or
 * `node scripts/smoke-npm.mjs` after `deno task npm:build`).
 *
 * In a throw-away project under the OS temp dir it runs
 * `npm install --install-links <.npm-dist>` - a packed copy, as a consumer
 * gets it, not a symlink into this repo - plus `@marianmeres/demino` (the range
 * the package itself depends on) and `ws` (the range demino's Node adapter
 * declares as its optional peer), and then, on Node, inside that project:
 *
 *  - checks the installed package.json: the three `exports` keys, the runtime
 *    dependencies, no `ws` among them, `engines.node`;
 *  - imports every subpath;
 *  - `createWSApp()` served through `@marianmeres/demino/node`, driven by the
 *    package's own client: authenticates, echoes through `onMessage`, shuts
 *    down. Node has no `Deno.upgradeWebSocket`, so a connect proves the
 *    upgrade came from the adapter's `info.upgradeWebSocket`.
 *
 * Repo tool, Node only (nothing here is shipped).
 */

import { execFileSync } from "node:child_process";
import {
	existsSync,
	lstatSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const DIST = join(ROOT, ".npm-dist");
const PKG = "@marianmeres/ws";
const DEMINO = "@marianmeres/demino";

const EXPECTED_EXPORTS = [".", "./protocol", "./server"];
const EXPECTED_DEPENDENCIES = [
	"@marianmeres/clog",
	DEMINO,
	"@marianmeres/pubsub",
	"@marianmeres/ticker",
	"@marianmeres/uid",
];

/** Runs inside the temp project, where the bare specifiers resolve. */
const CONSUMER = `
import assert from "node:assert/strict";
import { createWSClient } from "${PKG}";
import * as protocol from "${PKG}/protocol";
import { createWSApp, WSService } from "${PKG}/server";
import { serve } from "${DEMINO}/node";

// A hang (a socket, a timer or a server left open) fails the smoke instead
// of stalling it; unref'd, so it never keeps a clean run alive.
setTimeout(() => {
	console.error("smoke: timed out");
	process.exit(1);
}, 15_000).unref();

assert.equal(typeof createWSClient, "function", "createWSClient");
assert.equal(typeof protocol.PROTOCOL_VERSION, "number", "protocol.PROTOCOL_VERSION");
assert.equal(typeof createWSApp, "function", "createWSApp");
assert.equal(typeof WSService, "function", "WSService");
assert.equal(typeof globalThis.Deno, "undefined", "running on Node, not Deno");
console.log("smoke: all three subpaths import");

const { app, service } = createWSApp("/ws", [], {
	logger: null,
	deminoOptions: { logger: null },
	verify: (payload) => (payload === "secret" ? { clientId: "alice" } : null),
	onMessage: (_ctx, payload) => ({ echo: payload }),
});
const handle = await serve(app, { port: 0, hostname: "127.0.0.1", onListen: () => {} });
const client = createWSClient({
	url: \`ws://127.0.0.1:\${handle.port}/ws\`,
	logger: null,
	pingInterval: 0,
	connectTimeout: 3_000,
	auth: () => "secret",
});

try {
	await client.connect();
	assert.equal(client.clientId, "alice", "authenticated clientId");
	console.log(\`smoke: connected to ws://127.0.0.1:\${handle.port}/ws as "alice"\`);

	const reply = await client.send({ text: "hi" }, { ack: true });
	assert.deepEqual(reply, { echo: { text: "hi" } }, "echo reply");
	console.log("smoke: echo round trip");
} finally {
	client.dispose();
	await service.close();
	await handle.shutdown();
}
console.log("smoke: shut down cleanly");
`;

function run(cmd, args, cwd) {
	console.log(`smoke: $ ${[cmd, ...args].join(" ")}`);
	execFileSync(cmd, args, { cwd, stdio: "inherit" });
}

function check(condition, what) {
	if (!condition) throw new Error(`smoke: ${what}`);
}

function readManifest(dir) {
	return JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
}

function checkInstalledManifest(dir) {
	const pkg = readManifest(dir);
	check(pkg.name === PKG, `installed package is "${pkg.name}", expected "${PKG}"`);
	const exportKeys = Object.keys(pkg.exports ?? {}).sort();
	check(
		JSON.stringify(exportKeys) === JSON.stringify([...EXPECTED_EXPORTS].sort()),
		`exports keys are ${JSON.stringify(exportKeys)}`,
	);
	for (const [key, entry] of Object.entries(pkg.exports)) {
		for (const kind of ["types", "import"]) {
			check(
				typeof entry[kind] === "string" && existsSync(join(dir, entry[kind])),
				`exports["${key}"].${kind} does not exist: ${entry?.[kind]}`,
			);
		}
	}
	for (const name of EXPECTED_DEPENDENCIES) {
		check(name in (pkg.dependencies ?? {}), `runtime dependency "${name}" missing`);
	}
	check(!("ws" in (pkg.dependencies ?? {})), `"ws" must not be a runtime dependency`);
	check(typeof pkg.engines?.node === "string", "engines.node missing");
	console.log(
		`smoke: ${pkg.name}@${pkg.version} - exports ${exportKeys.length}/${EXPECTED_EXPORTS.length}, ` +
			`${DEMINO} ${pkg.dependencies[DEMINO]}, engines.node ${pkg.engines.node}`,
	);
}

check(
	existsSync(join(DIST, "package.json")),
	`${DIST} has no package.json - run \`deno task npm:build\` first`,
);
const deminoRange = readManifest(DIST).dependencies?.[DEMINO];
check(
	typeof deminoRange === "string",
	`${DIST}/package.json does not depend on ${DEMINO}`,
);

// `--install-links` on every install: without it a later `npm install` re-links
// the `file:` dependency, and the consumer would run against `.npm-dist/`
// (and its node_modules) instead of the packed copy
const npmInstall = [
	"install",
	"--install-links",
	"--no-audit",
	"--no-fund",
	"--loglevel=error",
];
const project = mkdtempSync(join(tmpdir(), "ws-npm-smoke-"));
try {
	writeFileSync(
		join(project, "package.json"),
		JSON.stringify(
			{ name: "ws-npm-smoke", private: true, type: "module" },
			null,
			"\t",
		),
	);
	// demino at the package's own range, so the consumer's copy and the one
	// `./server` imports dedupe into one, as they do for a real consumer
	run("npm", [...npmInstall, DIST, `${DEMINO}@${deminoRange}`], project);
	checkInstalledManifest(join(project, "node_modules", PKG));

	const deminoManifest = readManifest(join(project, "node_modules", DEMINO));
	const wsRange = deminoManifest.peerDependencies?.ws;
	check(typeof wsRange === "string", `${DEMINO} declares no "ws" peer`);
	run("npm", [...npmInstall, `ws@${wsRange}`], project);
	check(
		!lstatSync(join(project, "node_modules", PKG)).isSymbolicLink(),
		`node_modules/${PKG} is a symlink, not a packed copy`,
	);

	writeFileSync(join(project, "consumer.mjs"), CONSUMER);
	run(process.execPath, ["consumer.mjs"], project);
	console.log("smoke: OK");
} finally {
	rmSync(project, { recursive: true, force: true });
}
