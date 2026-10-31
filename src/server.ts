/**
 * Entry point for `@marianmeres/ws/server`.
 *
 * Kept as a top-level file because every subpath export maps to a flat
 * `src/{name}.ts` (`dist/{name}.js` on npm); the implementation lives in
 * `./server/`.
 *
 * @module
 */

export * from "./server/mod.ts";
