<script>
	/**
	 * Table detail page: namespace/table selection plus one tab per view.
	 *
	 * The selection is driven by the URL (?namespace=&table=&tab=&sample_limit=)
	 * so a table view can be bookmarked and shared. Previously the query params
	 * were stripped after load, which meant refresh lost your place and the
	 * "copy link" button had to rebuild a URL the app itself would discard.
	 */
	import { browser } from '$app/environment';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import { onMount } from 'svelte';
	import {
		Content,
		CopyButton,
		Tab,
		TabContent,
		Tabs,
		Tile,
		ToastNotification
	} from 'carbon-components-svelte';

	import { healthEnabled, HEALTH_DISABLED_MESSAGE, sample_limit, selectedNamespce, selectedTable } from '$lib/stores';
	import {
		namespaceList,
		selectNamespace,
		selectNamespaceAndTable,
		tableKey
	} from '$lib/api/catalog';
	import CatalogNav from '$lib/components/table/CatalogNav.svelte';
	import SummaryTab from '$lib/components/table/SummaryTab.svelte';
	import PartitionsTab from '$lib/components/table/PartitionsTab.svelte';
	import SnapshotsTab from '$lib/components/table/SnapshotsTab.svelte';
	import SampleDataTab from '$lib/components/table/SampleDataTab.svelte';
	import HealthTab from '$lib/components/table/HealthTab.svelte';
	import QueryRunner from '$lib/components/QueryRunner.svelte';

	export let data;

	/** Tab order, and the `tab` URL value for each. */
	const TABS = ['summary', 'partitions', 'snapshots', 'sample', 'sql', 'health'];

	const pageSessionId = `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;

	let namespaceProperties = null;
	let tableProperties = null;

	namespaceList.set(data.namespaces ?? []);

	// Selection comes from the URL on every render, including the server's, so a
	// shared table link renders correctly without waiting for hydration.
	//
	// Deliberately NOT written into the module-level stores during SSR: those are
	// per-process singletons on the server, so doing that leaked one request's
	// selection into every later paramless request. The stores are updated on the
	// client only (see onMount), where a module instance really is per-user.
	$: urlNamespace = $page.url.searchParams.get('namespace') ?? '';
	$: urlTable = $page.url.searchParams.get('table') ?? '';
	let selectedTab = tabIndexFromParam($page.url.searchParams.get('tab'));
	let urlApplied = false;

	/** @param {string | null} value */
	function tabIndexFromParam(value) {
		const index = value ? TABS.indexOf(value) : 0;
		return index >= 0 ? index : 0;
	}

	// `$`-prefixed reads auto-unsubscribe on destroy. The previous version called
	// selectedNamespce.subscribe()/selectedTable.subscribe() *inside* a reactive
	// block, creating a new never-unsubscribed listener on every re-run.
	//
	// Before hydration completes the URL is authoritative; afterwards the stores
	// are, so client-side navigation and the pickers stay in control.
	$: namespace = urlApplied ? ($selectedNamespce ?? '') : urlNamespace;
	$: table = urlApplied ? ($selectedTable ?? '') : urlTable;

	// --- URL -> state -------------------------------------------------------

	// Hand the URL's selection to the stores on the client, and load the table
	// list so the Table picker is populated for a pre-selected table.
	onMount(async () => {
		const params = $page.url.searchParams;
		const limit = Number(params.get('sample_limit'));
		if (Number.isFinite(limit) && limit > 0) sample_limit.set(limit);

		const ns = params.get('namespace') ?? '';
		const tbl = params.get('table') ?? '';
		if (ns && tbl) await selectNamespaceAndTable(ns, tbl);
		else if (ns) await selectNamespace(ns);
		else {
			// A paramless load must clear any selection left by a previous
			// client-side navigation.
			selectedNamespce.set('');
			selectedTable.set('');
		}
		urlApplied = true;
	});

	// --- state -> URL -------------------------------------------------------

	$: if (browser && urlApplied) syncUrl(namespace, table, selectedTab, $sample_limit);

	function syncUrl(ns, tbl, tabIndex, limit) {
		const params = new URLSearchParams();
		if (ns) params.set('namespace', ns);
		if (tbl) params.set('table', tbl);
		if (tabIndex) params.set('tab', TABS[tabIndex]);
		if (limit && limit !== 100) params.set('sample_limit', String(limit));

		const next = params.toString();
		if (next === $page.url.searchParams.toString()) return;
		goto(next ? `?${next}` : $page.url.pathname, {
			replaceState: true,
			noScroll: true,
			keepFocus: true
		});
	}

	// --- authz-provided extra properties ------------------------------------

	$: if (namespace) loadNamespaceProperties(namespace);
	else namespaceProperties = null;

	$: if (namespace && table) loadTableProperties(`${namespace}.${table}`);
	else tableProperties = null;

	async function loadNamespaceProperties(ns) {
		try {
			const res = await fetch(`/api/namespaces/${encodeURIComponent(ns)}/special-properties`, {
				headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
			});
			namespaceProperties = res.ok ? await res.json() : null;
		} catch {
			namespaceProperties = null;
		}
	}

	async function loadTableProperties(qualified) {
		try {
			const res = await fetch(`/api/tables/${encodeURIComponent(qualified)}/special-properties`, {
				headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
			});
			tableProperties = res.ok ? await res.json() : null;
		} catch {
			tableProperties = null;
		}
	}

	// --- toasts -------------------------------------------------------------

	let toasts = [];
	let toastSeq = 0;

	/** Queue a toast. Multiple toasts stack instead of overwriting each other. */
	function showToast(kind, title, subtitle, timeout = 4000) {
		const id = ++toastSeq;
		toasts = [...toasts, { id, kind, title, subtitle, at: new Date().toLocaleString() }];
		if (timeout) setTimeout(() => dismissToast(id), timeout);
	}

	function dismissToast(id) {
		toasts = toasts.filter((t) => t.id !== id);
	}

	// --- share link ---------------------------------------------------------

	let shareUrl = '';
	function buildShareUrl() {
		shareUrl = browser ? window.location.href : '';
	}
</script>

<CatalogNav />

<Content>
	<div class="toast-stack">
		{#each toasts as toast (toast.id)}
			<ToastNotification
				kind={toast.kind}
				title={toast.title}
				subtitle={toast.subtitle}
				caption={toast.at}
				timeout={0}
				on:close={() => dismissToast(toast.id)}
			/>
		{/each}
	</div>

	{#if !namespace && !table}
		<Tile>
			<div class="empty-state">
				<h3>Explore your lakehouse</h3>
				<p>
					Select a namespace from the sidebar to browse its tables, or choose
					<strong>Show All Tables</strong> to search across every namespace.
				</p>
				<p class="empty-hint">
					{($namespaceList ?? []).length} namespace{($namespaceList ?? []).length === 1 ? '' : 's'} available.
				</p>
			</div>
		</Tile>
	{:else}
		<Tile>
			<div class="tile-header">
				<div class="tile-content">
					<dl class="namespace-table-list">
						<dt>Namespace</dt>
						<dd>{namespace || '—'}</dd>
						<dt>Table</dt>
						<dd>{table || '—'}</dd>
					</dl>
				</div>
				<div class="copy-button-container">
					<CopyButton
						text={shareUrl}
						on:click={buildShareUrl}
						iconDescription="Copy table link"
						feedback="Table link copied"
					/>
				</div>
			</div>
		</Tile>
		<br />

		<Tabs bind:selected={selectedTab}>
			<Tab label="Summary" />
			<Tab label="Partitions" />
			<Tab label="Snapshots" />
			<Tab label="Sample Data" />
			<Tab label="SQL" />
			<Tab label="Health Check" />

			<svelte:fragment slot="content">
				<TabContent>
					{#if selectedTab === 0}
						<SummaryTab
							tableKey={$tableKey}
							{pageSessionId}
							{namespaceProperties}
							{tableProperties}
						/>
					{/if}
				</TabContent>
				<TabContent>
					{#if selectedTab === 1}
						<PartitionsTab tableKey={$tableKey} {pageSessionId} />
					{/if}
				</TabContent>
				<TabContent>
					{#if selectedTab === 2}
						<SnapshotsTab tableKey={$tableKey} {pageSessionId} />
					{/if}
				</TabContent>
				<TabContent>
					{#if selectedTab === 3}
						<SampleDataTab tableKey={$tableKey} {pageSessionId} />
					{/if}
				</TabContent>
				<TabContent>
					{#if selectedTab === 4}
						<br />
						<QueryRunner tableName={$tableKey} {pageSessionId} />
					{/if}
				</TabContent>
				<TabContent>
					{#if selectedTab === 5}
						{#if $healthEnabled}
							<HealthTab {namespace} {table} onToast={showToast} />
						{:else}
							<br />
							<p>{@html HEALTH_DISABLED_MESSAGE}</p>
						{/if}
					{/if}
				</TabContent>
			</svelte:fragment>
		</Tabs>
	{/if}
</Content>

<style>
	.toast-stack {
		position: fixed;
		top: 3.5rem;
		right: 1rem;
		z-index: 9999;
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
		max-width: min(90vw, 24rem);
	}
	.tile-content {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}
	.namespace-table-list {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 25px;
		margin: 0;
		font-size: 1.3em;
	}
	dt {
		font-weight: bold;
	}
	dd {
		margin: 0;
	}
	.copy-button-container {
		align-items: end;
		display: flex;
		justify-content: flex-end;
	}
	.tile-header {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		flex-wrap: wrap;
	}
	.empty-state {
		padding: 2rem 1rem;
		max-width: 40rem;
	}
	.empty-state h3 {
		margin-bottom: 0.75rem;
	}
	.empty-state p {
		margin-bottom: 0.5rem;
	}
	.empty-hint {
		color: var(--cds-text-secondary, #525252);
		font-size: 0.875rem;
	}
</style>
