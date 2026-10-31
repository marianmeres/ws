/**
 * Manifest sync: `package.json` (npm, the manifest of record) and `deno.json`
 * (JSR) describe the same package, so the two cannot drift silently.
 *
 *  - only `package.json` carries a `version` (`release` bumps it; JSR gets it
 *    via `deno publish --set-version`), and only it carries scripts;
 *  - every runtime dependency is a `deno.json` import with the same range, and
 *    vice versa — except demino, the optional peer;
 *  - the exports maps are twins: `src/<x>.ts` for JSR, `dist/<x>.js` for npm.
 *
 * Runs the same on Node and Deno.
 */

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

interface PackageJson {
	name: string;
	version?: string;
	exports: Record<string, { types: string; default: string }>;
	scripts: Record<string, string>;
	dependencies: Record<string, string>;
	peerDependencies: Record<string, string>;
	peerDependenciesMeta: Record<string, { optional?: boolean }>;
	devDependencies: Record<string, string>;
}

interface DenoJson {
	name: string;
	version?: string;
	tasks?: unknown;
	nodeModulesDir?: string;
	exports: Record<string, string>;
	imports: Record<string, string>;
}

function readJson<T>(file: string): T {
	return JSON.parse(readFileSync(new URL(`../${file}`, import.meta.url), "utf8")) as T;
}

const pkg = readJson<PackageJson>("package.json");
const deno = readJson<DenoJson>("deno.json");

const DEMINO = "@marianmeres/demino";
/** `deno.json` imports that are not npm runtime dependencies, and where they live instead. */
const NOT_RUNTIME: Record<string, keyof PackageJson> = {
	[DEMINO]: "peerDependencies",
};

/** `jsr:@scope/name@^1.2.3` or `npm:name@^1.2.3` -> `^1.2.3` */
function rangeOf(specifier: string): string {
	const m = /^(?:jsr|npm):(?:@[^/@]+\/)?[^/@]+@([^/]+)$/.exec(specifier);
	assert.ok(m, `unrecognized deno.json specifier "${specifier}"`);
	return m[1];
}

test("manifest-sync: package.json is the only manifest with a version and scripts", () => {
	assert.equal(deno.name, pkg.name);
	assert.match(pkg.version ?? "", /^\d+\.\d+\.\d+(-[\w.]+)?$/);
	assert.equal(deno.version, undefined, "deno.json must not carry a version");
	assert.equal(deno.tasks, undefined, "deno.json must not carry tasks");
	// with a package.json present, `deno test` otherwise wants a local node_modules
	assert.equal(deno.nodeModulesDir, "none");
});

test("manifest-sync: dependencies match deno.json imports, range for range", () => {
	const runtime = Object.entries(deno.imports).filter(([k]) => !(k in NOT_RUNTIME));
	assert.ok(runtime.length > 0, "deno.json has no runtime imports");
	assert.deepEqual(
		Object.fromEntries(runtime.map(([k, v]) => [k, rangeOf(v)])),
		pkg.dependencies,
	);
	for (const [name, where] of Object.entries(NOT_RUNTIME)) {
		const section = pkg[where] as Record<string, string>;
		assert.equal(section[name], rangeOf(deno.imports[name]), `${name} in ${where}`);
	}
});

test("manifest-sync: demino is an optional peer, also a devDependency", () => {
	assert.equal(pkg.peerDependenciesMeta[DEMINO]?.optional, true);
	assert.equal(pkg.devDependencies[DEMINO], pkg.peerDependencies[DEMINO]);
	assert.equal(pkg.dependencies[DEMINO], undefined);
});

test("manifest-sync: the npm exports map is the dist/ twin of deno.json's", () => {
	const expected = Object.fromEntries(
		Object.entries(deno.exports).map(([key, src]) => {
			const base = src.replace(/^\.\/src\//, "./dist/").replace(/\.ts$/, "");
			return [key, { types: `${base}.d.ts`, default: `${base}.js` }];
		}),
	);
	assert.deepEqual(pkg.exports, expected);
});

test("manifest-sync: no `publish` script, and both registries get the same version", () => {
	// `publish` is an npm lifecycle hook: `pnpm publish` would run it
	assert.equal(pkg.scripts.publish, undefined);
	assert.match(
		pkg.scripts["publish:all"] ?? "",
		/^pnpm publish && pnpm run publish:jsr$/,
	);
	assert.match(pkg.scripts["publish:next"] ?? "", /^pnpm publish --tag next\b/);
	assert.match(
		pkg.scripts["publish:jsr"] ?? "",
		/deno publish --set-version "\$npm_package_version"$/,
	);
});
