/**
 * Recently viewed tables, per browser.
 *
 * Purely a local convenience for the landing page, so localStorage is the right
 * store: it never needs to reach the server or another device, and losing it
 * costs nothing. Every access is guarded because storage throws in private mode
 * and when site data is blocked.
 */
import { writable } from 'svelte/store';
import { browser } from '$app/environment';

const KEY = 'lakevision.recentTables';
const LIMIT = 8;

/** @typedef {{ namespace: string, table: string }} RecentTable */

/** @returns {RecentTable[]} */
function load() {
	if (!browser) return [];
	try {
		const raw = localStorage.getItem(KEY);
		if (!raw) return [];
		const parsed = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		return parsed
			.filter((e) => e && typeof e.namespace === 'string' && typeof e.table === 'string')
			.slice(0, LIMIT);
	} catch {
		return [];
	}
}

export const recentTables = writable(load());

/**
 * Record a visit, most recent first, de-duplicated.
 * @param {string} namespace
 * @param {string} table
 */
export function rememberTable(namespace, table) {
	if (!browser || !namespace || !table) return;
	recentTables.update((current) => {
		const next = [
			{ namespace, table },
			...current.filter((e) => !(e.namespace === namespace && e.table === table))
		].slice(0, LIMIT);
		try {
			localStorage.setItem(KEY, JSON.stringify(next));
		} catch {
			// Not persisting is fine; the in-memory list still works this session.
		}
		return next;
	});
}
