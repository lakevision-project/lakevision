<script>
	/**
	 * Sidebar navigation: namespace and table pickers, plus the two
	 * "show all" browse modals.
	 */
	import {
		Button,
		ComboBox,
		InlineLoading,
		Modal,
		Search,
		SideNav,
		SideNavItems,
		SideNavLink
	} from 'carbon-components-svelte';
	import { FilterRemove, Renew } from 'carbon-icons-svelte';
	import { selectedNamespce, selectedTable } from '$lib/stores';
	import {
		allTables,
		groupByNamespace,
		loadAllTables,
		namespaceList,
		navRefreshing,
		refreshAllTables,
		refreshNamespaces,
		selectNamespace,
		selectNamespaceAndTable,
		tablesInNamespace,
		tablesLoading
	} from '$lib/api/catalog';

	/**
	 * Open state. Defaults to open on wide viewports and closed below Carbon's
	 * `lg` breakpoint (1056px), where an expanded nav would overlay the content.
	 */
	export let isOpen = true;

	/** Carbon's `lg` breakpoint: below this an expanded nav overlays the content. */
	const LG_BREAKPOINT = 1056;

	/** Viewport width, bound via svelte:window so it tracks resizes. */
	let viewportWidth = LG_BREAKPOINT;
	let userToggled = false;

	$: isNarrow = viewportWidth < LG_BREAKPOINT;
	// Follow the breakpoint until the user takes control of the nav themselves.
	$: if (!userToggled) isOpen = !isNarrow;

	let showNamespaceModal = false;
	let showTableModal = false;
	let namespaceQuery = '';
	let tableQuery = '';

	// Carbon's ComboBox is driven by selectedId; map the selected name to its id
	// so external selection (a URL, or a click in the browse modal) reflects here.
	$: namespaceId = $namespaceList.find((n) => n.text === $selectedNamespce)?.id ?? null;
	$: tableId = $tablesInNamespace.find((t) => t.text === $selectedTable)?.id ?? null;

	$: filteredNamespaces = $namespaceList.filter((ns) =>
		ns.text.toLowerCase().includes(namespaceQuery.toLowerCase())
	);
	$: browseTables = $selectedNamespce ? $tablesInNamespace : $allTables;
	$: filteredTables = browseTables.filter((t) =>
		t.text.toLowerCase().includes(tableQuery.toLowerCase())
	);

	/**
	 * @param {{text: string}} item
	 * @param {string} value
	 */
	function shouldFilterItem(item, value) {
		if (!value) return true;
		return item.text.toLowerCase().includes(value.toLowerCase());
	}

	/** @param {CustomEvent<any>} event */
	function onNamespacePick(event) {
		const picked = $namespaceList.find((n) => n.id === event.detail.selectedId);
		selectNamespace(picked ? picked.text : '');
	}

	/** @param {CustomEvent<any>} event */
	function onTablePick(event) {
		const picked = $tablesInNamespace.find((t) => t.id === event.detail.selectedId);
		selectedTable.set(picked ? picked.text : '');
	}

	async function openAllTables() {
		await loadAllTables();
		showTableModal = true;
	}

	/** @param {string} name */
	function pickNamespace(name) {
		selectNamespace(name);
		showNamespaceModal = false;
	}

	/**
	 * @param {string} namespace
	 * @param {string} table
	 */
	function pickTable(namespace, table) {
		selectNamespaceAndTable(namespace, table);
		showTableModal = false;
	}

	async function clearNamespaceFilter() {
		await selectNamespace('');
		await loadAllTables();
	}
</script>

<svelte:window bind:innerWidth={viewportWidth} />

{#if isNarrow}
	<div class="nav-toggle">
		<button
			type="button"
			class="nav-toggle-button"
			aria-expanded={isOpen}
			aria-label={isOpen ? 'Close catalog navigation' : 'Open catalog navigation'}
			on:click={() => {
				userToggled = true;
				isOpen = !isOpen;
			}}
		>
			<svg width="20" height="20" viewBox="0 0 32 32" fill="currentColor" aria-hidden="true">
				{#if isOpen}
					<path d="M24 9.4L22.6 8 16 14.6 9.4 8 8 9.4 14.6 16 8 22.6 9.4 24 16 17.4 22.6 24 24 22.6 17.4 16z" />
				{:else}
					<path d="M4 6h24v2H4zM4 15h24v2H4zM4 24h24v2H4z" />
				{/if}
			</svg>
		</button>
	</div>
{/if}

<SideNav bind:isOpen>
	<SideNavItems>
		<div class="nav-section">
			<ComboBox
				titleText="Namespace"
				items={$namespaceList}
				selectedId={namespaceId}
				{shouldFilterItem}
				on:select={onNamespacePick}
				let:item
			>
				<div><strong>{item.text}</strong></div>
			</ComboBox>
			<SideNavLink on:click={() => (showNamespaceModal = true)}>Show All Namespaces</SideNavLink>
		</div>

		<div class="nav-section">
			<ComboBox
				titleText="Table"
				disabled={$tablesLoading}
				items={$tablesInNamespace}
				selectedId={tableId}
				{shouldFilterItem}
				on:select={onTablePick}
				let:item
			>
				<div><strong>{item.text}</strong></div>
			</ComboBox>
			<SideNavLink on:click={openAllTables}>Show All Tables</SideNavLink>
		</div>
	</SideNavItems>
</SideNav>

<Modal
	size="sm"
	passiveModal
	bind:open={showNamespaceModal}
	modalHeading="Namespaces"
	on:open
	on:close
>
	<div class="modal-toolbar">
		<Search bind:value={namespaceQuery} placeholder="Search namespaces..." />
		{#if $navRefreshing}
			<InlineLoading description="Refreshing..." />
		{:else}
			<Button
				iconDescription="Refresh namespaces"
				icon={Renew}
				size="small"
				on:click={refreshNamespaces}
			/>
		{/if}
	</div>
	<div class="browse-list">
		<table>
			<caption class="sr-only">Namespaces</caption>
			<thead>
				<tr><th scope="col">#</th><th scope="col">Namespace</th></tr>
			</thead>
			<tbody>
				{#each filteredNamespaces as ns (ns.id)}
					<tr>
						<td>{ns.id}</td>
						<td><button type="button" class="link-button" on:click={() => pickNamespace(ns.text)}>
							{ns.text}
						</button></td>
					</tr>
				{/each}
			</tbody>
		</table>
		{#if filteredNamespaces.length === 0}<p>No namespaces match.</p>{/if}
	</div>
</Modal>

<Modal
	size="sm"
	passiveModal
	bind:open={showTableModal}
	modalHeading={$selectedNamespce ? `Tables in: ${$selectedNamespce}` : 'All Tables'}
	on:open
	on:close
>
	<div class="modal-toolbar">
		<Search bind:value={tableQuery} placeholder="Search tables..." />
		{#if $navRefreshing}
			<InlineLoading description="Refreshing..." />
		{:else if $selectedNamespce}
			<Button
				iconDescription="Clear filter"
				icon={FilterRemove}
				size="small"
				on:click={clearNamespaceFilter}
			/>
		{:else}
			<Button
				iconDescription="Refresh tables"
				icon={Renew}
				size="small"
				on:click={refreshAllTables}
			/>
		{/if}
	</div>
	<div class="browse-list">
		{#each Object.entries(groupByNamespace(filteredTables)) as [ns, rows] (ns)}
			<table>
				<caption>{ns}</caption>
				<thead>
					<tr><th scope="col">#</th><th scope="col">Table</th></tr>
				</thead>
				<tbody>
					{#each rows as row (row.namespace + '.' + row.text)}
						<tr>
							<td>{row.id}</td>
							<td><button
									type="button"
									class="link-button"
									on:click={() => pickTable(row.namespace, row.text)}
								>{row.text}</button></td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/each}
		{#if $tablesLoading}
			<InlineLoading description="Loading..." />
		{:else if filteredTables.length === 0}
			<p>No tables match.</p>
		{/if}
	</div>
</Modal>

<style>
	.nav-toggle {
		position: fixed;
		top: 0;
		left: 0;
		z-index: 8000;
	}
	/* Sized and coloured to sit in the header strip alongside Carbon's own
	   header actions. */
	.nav-toggle-button {
		width: 3rem;
		height: 3rem;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		background: none;
		border: none;
		cursor: pointer;
		color: var(--cds-text-04, #ffffff);
	}
	.nav-toggle-button:hover {
		background-color: var(--cds-hover-secondary, #4c4c4c);
	}
	.nav-toggle-button:focus {
		outline: 2px solid var(--cds-focus, #ffffff);
		outline-offset: -2px;
	}
	.nav-section {
		padding: 1rem 0;
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}
	.modal-toolbar {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		margin-bottom: 1rem;
	}
	.browse-list {
		max-height: 500px;
		overflow-y: auto;
	}
	.browse-list table {
		width: 100%;
		border-collapse: collapse;
		margin-bottom: 1rem;
	}
	.browse-list caption {
		text-align: left;
		font-weight: 600;
		padding: 0.5rem 0;
	}
	.browse-list th,
	.browse-list td {
		border: 1px solid var(--cds-ui-03, #e0e0e0);
		padding: 8px;
		text-align: left;
		max-width: 60vw;
		overflow-wrap: anywhere;
	}
	/* A real button, so keyboard and screen-reader semantics come for free --
	   this replaced a div[role=button] wrapping an <a href="#">. */
	.link-button {
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		color: var(--cds-link-primary, #0f62fe);
		text-decoration: underline;
		cursor: pointer;
		text-align: left;
	}
	.link-button:hover {
		color: var(--cds-link-primary-hover, #0043ce);
	}
	.sr-only {
		position: absolute;
		width: 1px;
		height: 1px;
		overflow: hidden;
		clip: rect(0 0 0 0);
		white-space: nowrap;
	}
</style>
