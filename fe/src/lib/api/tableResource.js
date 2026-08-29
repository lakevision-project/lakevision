/**
 * Table-scoped data loading for the table detail page.
 *
 * Replaces eight near-identical reactive blocks in +page.svelte, each of which
 * hand-rolled its own request-sequence counter, loading flag and try/finally.
 * The sequence counter matters: switching tables fires overlapping requests and
 * a slow earlier response must not overwrite a newer one.
 */
import { writable } from 'svelte/store';

/** Thrown when the caller must re-authenticate. */
export class UnauthorizedError extends Error {}
/** Thrown when the user lacks access to a table's data. */
export class ForbiddenError extends Error {}

/**
 * Fetch one table feature (`schema`, `summary`, `sample?...`).
 *
 * @param {string} tableKey  "namespace.table"
 * @param {string} feature   endpoint suffix, may include a query string
 * @param {string} pageSessionId  value for the X-Page-Session-ID header
 */
export async function fetchTableFeature(tableKey, feature, pageSessionId) {
	const res = await fetch(`/api/tables/${encodeURIComponent(tableKey)}/${feature}`, {
		headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
	});

	if (res.ok) return res.json();
	if (res.status === 401) throw new UnauthorizedError('Session expired');
	if (res.status === 403) throw new ForbiddenError('No access');

	// 400 (query validation) and 418 (application error) carry a usable message.
	let detail = res.statusText;
	try {
		const body = await res.json();
		detail = body.detail || body.message || detail;
	} catch {
		/* non-JSON body; fall back to statusText */
	}
	throw new Error(detail);
}

/**
 * A reloadable store for one table feature.
 *
 * Exposes `{ subscribe, load, reset }` where the value is
 * `{ data, loading, error, loaded }`. Stale responses are discarded, so callers
 * can switch tables freely without racing.
 *
 * @param {string} feature
 * @param {{ initial?: any }} [options]
 */
export function createTableResource(feature, { initial = null } = {}) {
	/** @type {{ data: any, loading: boolean, error: Error | null, loaded: boolean }} */
	const empty = { data: initial, loading: false, error: null, loaded: false };
	const store = writable(empty);
	let sequence = 0;

	/**
	 * @param {string} tableKey
	 * @param {string} pageSessionId
	 * @param {{ suffix?: string }} [opts]  extra query string, e.g. "?sample_limit=50"
	 */
	/**
	 * @param {string} tableKey
	 * @param {string} pageSessionId
	 * @param {{ suffix?: string }} [opts]
	 */
	async function load(tableKey, pageSessionId, { suffix = '' } = {}) {
		if (!tableKey) return;
		const mine = ++sequence;
		store.update((s) => ({ ...s, loading: true, error: null }));
		try {
			const data = await fetchTableFeature(tableKey, `${feature}${suffix}`, pageSessionId);
			if (mine === sequence) store.set({ data, loading: false, error: null, loaded: true });
		} catch (/** @type {any} */ err) {
			if (mine !== sequence) return; // superseded by a newer request
			if (err instanceof UnauthorizedError) {
				store.set({ ...empty, error: err });
				throw err; // caller decides whether to redirect to login
			}
			store.set({ data: initial, loading: false, error: err, loaded: true });
		}
	}

	/** Drop any loaded data and cancel in-flight results. */
	function reset() {
		sequence++;
		store.set(empty);
	}

	return { subscribe: store.subscribe, load, reset };
}
