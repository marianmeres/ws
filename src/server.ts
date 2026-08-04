/**
 * Entry point for `@marianmeres/ws/server`.
 *
 * Kept as a top-level file because the npm build maps subpath exports to
 * `src/{name}.ts`; the implementation lives in `./server/`.
 *
 * @module
 */

export * from "./server/mod.ts";
