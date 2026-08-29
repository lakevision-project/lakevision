/**
 * Catalog navigation: namespace and table lists, and the current selection.
 *
 * Selection previously lived in module-level `let` bindings mutated from several
 * reactive blocks in +page.svelte, including two `store.subscribe()` calls made
 * *inside* a reactive block -- which created a fresh, never-unsubscribed
 * subscription on every re-run. Centralising it here keeps the derivation in one
 * place and lets components use `$`-prefixed auto-subscriptions, which Svelte
 * unsubscribes on destroy.
 */
import { derived, get, writable } from 'svelte/store';
import { selectedNamespce, selectedTable } from '$lib/stores';

/** Fully-qualified "namespace.table", or '' when the selection is incomplete. */
export const tableKey = derived(
	[selectedNamespce, selectedTable],
	([ns, tbl]) => (ns && tbl ? `${ns}.${tbl}` : '')
);

/** @type {import('svelte/store').Writable<Array<{id:number,text:string}>>} */
export const namespaceList = writable([]);
/** @type {import('svelte/store').Writable<Array<any>>} */
export const tablesInNamespace = writable([]);
/** @type {import('svelte/store').Writable<Array<any>>} */
export const allTables = writable([]);
export const tablesLoading = writable(false);
export const navRefreshing = writable(false);

/** Select a namespace and load its tables. Clears any table selection. */
/** @param {string} namespace */
export async function selectNamespace(namespace) {
	selectedNamespce.set(namespace ?? '');
	selectedTable.set('');
	if (!namespace) {
		tablesInNamespace.set([]);
		return [];
	}
	tablesLoading.set(true);
	try {
		const res = await fetch(`/api/tables?namespace=${encodeURIComponent(namespace)}`);
		if (!res.ok) throw new Error(`Failed to load tables: ${res.statusText}`);
		const tables = await res.json();
		tablesInNamespace.set(tables);
		return tables;
	} finally {
		tablesLoading.set(false);
	}
}

/** Load the full table list across all namespaces. */
export async function loadAllTables({ refresh = false } = {}) {
	tablesLoading.set(true);
	try {
		const res = await fetch(`/api/tables${refresh ? '?refresh=true' : ''}`);
		if (!res.ok) throw new Error(`Failed to load tables: ${res.statusText}`);
		const tables = await res.json();
		allTables.set(tables);
		return tables;
	} finally {
		tablesLoading.set(false);
	}
}

export async function refreshNamespaces() {
	navRefreshing.set(true);
	try {
		const res = await fetch('/api/namespaces?refresh=true');
		if (!res.ok) throw new Error(`Failed to refresh namespaces: ${res.statusText}`);
		const namespaces = await res.json();
		namespaceList.set(namespaces);
		return namespaces;
	} finally {
		navRefreshing.set(false);
	}
}

export async function refreshAllTables() {
	navRefreshing.set(true);
	try {
		return await loadAllTables({ refresh: true });
	} finally {
		navRefreshing.set(false);
	}
}

/**
 * Select a namespace and a table together, loading the table list first so the
 * table exists by the time it is selected.
 *
 * Replaces a promise that resolved off a never-removed EventTarget listener and
 * could hang forever if the table fetch failed.
 */
/**
 * @param {string} namespace
 * @param {string} table
 */
export async function selectNamespaceAndTable(namespace, table) {
	if (get(selectedNamespce) !== namespace) await selectNamespace(namespace);
	selectedTable.set(table);
}

/** Group a flat table list by its `namespace` field, for display. */
/**
 * @param {Array<{namespace: string, text: string, id: number}>} tables
 * @returns {Record<string, Array<any>>}
 */
export function groupByNamespace(tables) {
	return tables.reduce((/** @type {Record<string, Array<any>>} */ acc, tab) => {
		(acc[tab.namespace] = acc[tab.namespace] || []).push(tab);
		return acc;
	}, {});
}
