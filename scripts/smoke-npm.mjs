/**
 * Smoke-tests the built npm package (`pnpm run smoke`, or
 * `node scripts/smoke-npm.mjs` after `pnpm run build`).
 *
 * `pnpm pack`s the repo root - the tarball `pnpm publish` would upload - and in
 * a throw-away project under the OS temp dir installs it with npm, as a
 * consumer gets it, then on Node:
 *
 *  1. checks the installed package.json (the three `exports` keys and their
 *     files, the runtime dependencies, `@marianmeres/demino` an optional peer
 *     and neither it nor `ws` a dependency, `engines.node`) and that the
 *     tarball carries `dist/` and none of the repo-only directories;
 *  2. **without demino installed**, imports `.` and `./protocol` - the
 *     client-only install the optional peer exists for;
 *  3. installs demino (the range of the package's optional peer) and `ws` (the
 *     range demino's Node adapter declares as its optional peer), then imports
 *     every subpath and serves `createWSApp()` through
 *     `@marianmeres/demino/node`, driven by the package's own client:
 *     authenticates, echoes through `onMessage`, shuts down. Node has no
 *     `Deno.upgradeWebSocket`, so a connect proves the upgrade came from the
 *     adapter's `info.upgradeWebSocket`.
 *
 * Repo tool, Node only (nothing here is shipped).
 */

import { execFileSync } from "node:child_process";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const PKG = "@marianmeres/ws";
const DEMINO = "@marianmeres/demino";

const EXPECTED_EXPORTS = [".", "./protocol", "./server"];
/** Repo directories the published tarball must not carry. */
const REPO_ONLY = ["src", "tests", "scripts", "tmp", "example", "node_modules"];
const EXPECTED_DEPENDENCIES = [
	"@marianmeres/clog",
	"@marianmeres/pubsub",
	"@marianmeres/ticker",
	"@marianmeres/uid",
];

/** Step 2: runs before demino is installed. */
const CLIENT_ONLY = `
import assert from "node:assert/strict";
import { createWSClient } from "${PKG}";
import { PROTOCOL_VERSION } from "${PKG}/protocol";

assert.equal(typeof createWSClient, "function", "createWSClient");
assert.equal(typeof PROTOCOL_VERSION, "number", "PROTOCOL_VERSION");
console.log("smoke: . and ./protocol import without demino");
`;

/** Step 3: runs after demino and ws are installed. */
const ROUND_TRIP = `
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
		JSON.stringify(exportKeys) === JSON.stringify(EXPECTED_EXPORTS),
		`exports keys are ${JSON.stringify(exportKeys)}`,
	);
	for (const [key, entry] of Object.entries(pkg.exports)) {
		for (const kind of ["types", "default"]) {
			check(
				typeof entry[kind] === "string" && existsSync(join(dir, entry[kind])),
				`exports["${key}"].${kind} does not exist: ${entry?.[kind]}`,
			);
		}
	}
	for (const name of EXPECTED_DEPENDENCIES) {
		check(name in (pkg.dependencies ?? {}), `runtime dependency "${name}" missing`);
	}
	for (const name of ["ws", DEMINO]) {
		check(
			!(name in (pkg.dependencies ?? {})),
			`"${name}" must not be a runtime dependency`,
		);
	}
	check(typeof pkg.peerDependencies?.[DEMINO] === "string", `${DEMINO} is not a peer`);
	check(
		pkg.peerDependenciesMeta?.[DEMINO]?.optional === true,
		`${DEMINO} peer is not optional`,
	);
	check(typeof pkg.engines?.node === "string", "engines.node missing");
	for (const name of REPO_ONLY) {
		check(!existsSync(join(dir, name)), `the tarball carries "${name}/"`);
	}
	console.log(
		`smoke: ${pkg.name}@${pkg.version} - exports ${exportKeys.length}, ` +
			`peer ${DEMINO} ${pkg.peerDependencies[DEMINO]} (optional), ` +
			`engines.node ${pkg.engines.node}`,
	);
	return pkg.peerDependencies[DEMINO];
}

check(
	existsSync(join(ROOT, "dist", "mod.js")),
	"dist/mod.js is missing - run `pnpm run build` first",
);

const npmInstall = ["install", "--no-audit", "--no-fund", "--loglevel=error"];

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
	const packDir = join(project, "pack");
	mkdirSync(packDir);
	run("pnpm", ["pack", "--pack-destination", packDir], ROOT);
	const tarballs = readdirSync(packDir).filter((f) => f.endsWith(".tgz"));
	check(tarballs.length === 1, `expected one tarball, got ${JSON.stringify(tarballs)}`);

	// the package alone, as a client-only consumer installs it: an optional
	// peer is not installed, so no demino may appear
	run("npm", [...npmInstall, join(packDir, tarballs[0])], project);
	const deminoRange = checkInstalledManifest(join(project, "node_modules", PKG));
	check(!existsSync(join(project, "node_modules", DEMINO)), `${DEMINO} got installed`);
	writeFileSync(join(project, "client-only.mjs"), CLIENT_ONLY);
	run(process.execPath, ["client-only.mjs"], project);

	// then demino at the peer range, as whoever imports `./server` installs it,
	// and `ws` at the range demino's Node adapter asks for
	run("npm", [...npmInstall, `${DEMINO}@${deminoRange}`], project);
	const wsRange = readManifest(join(project, "node_modules", DEMINO)).peerDependencies
		?.ws;
	check(typeof wsRange === "string", `${DEMINO} declares no "ws" peer`);
	run("npm", [...npmInstall, `ws@${wsRange}`], project);

	writeFileSync(join(project, "round-trip.mjs"), ROUND_TRIP);
	run(process.execPath, ["round-trip.mjs"], project);
	console.log("smoke: OK");
} finally {
	rmSync(project, { recursive: true, force: true });
}
