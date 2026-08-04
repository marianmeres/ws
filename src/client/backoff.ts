/**
 * Reconnect backoff. Pure and injectable, so the curve can be unit tested
 * without waiting on real timers.
 *
 * @module
 */

/**
 * Exponential backoff with **equal jitter**.
 *
 * Returns a delay in `[d/2, d]` where `d = min(max, base * 2^(attempt-1))`.
 *
 * Equal jitter rather than full jitter (`random() * d`): full jitter can
 * produce near-zero waits, which means a server coming back up gets hammered
 * by the very clients it just dropped. Equal jitter keeps a floor while still
 * breaking the lockstep that would otherwise make 10k clients retry in unison.
 *
 * The cap matters as much as the jitter — uncapped `base * 2^n` reaches ~17
 * minutes by attempt 11, which is indistinguishable from "dead" to a user.
 *
 * @param attempt - 1-based attempt number
 * @param base - initial delay in ms
 * @param max - ceiling in ms
 * @param rnd - randomness source, injectable for tests
 */
export function backoffDelay(
	attempt: number,
	base: number,
	max: number,
	rnd: () => number = Math.random,
): number {
	const n = Math.max(1, Math.floor(attempt));
	// 2^n overflows to Infinity well before it matters; Math.min handles it.
	const full = Math.min(max, base * Math.pow(2, n - 1));
	const half = full / 2;
	return Math.round(half + rnd() * half);
}
