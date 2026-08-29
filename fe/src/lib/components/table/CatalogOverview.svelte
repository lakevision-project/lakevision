<script>
	/**
	 * Landing view, shown when no table is selected.
	 *
	 * Previously a single sentence on an otherwise blank page. A data catalog's
	 * front door should say what is in it and offer a way in, so this shows
	 * catalog-level counts, the largest namespaces, and recently-viewed tables.
	 */
	import { onMount } from 'svelte';
	import { SkeletonText } from 'carbon-components-svelte';
	import {
		allTables,
		loadAllTables,
		namespaceList,
		selectNamespace,
		selectNamespaceAndTable
	} from '$lib/api/catalog';
	import { recentTables } from '$lib/recent';

	let loading = true;
	/** @type {Error | null} */
	let error = null;

	onMount(async () => {
		try {
			// Cheap: one call, and the result is reused by the browse modal.
			if (($allTables ?? []).length === 0) await loadAllTables();
		} catch (err) {
			error = err;
		} finally {
			loading = false;
		}
	});

	$: tables = $allTables ?? [];
	$: byNamespace = tables.reduce((/** @type {Record<string, number>} */ acc, /** @type {any} */ t) => {
		acc[t.namespace] = (acc[t.namespace] ?? 0) + 1;
		return acc;
	}, /** @type {Record<string, number>} */ ({}));
	$: topNamespaces = Object.entries(byNamespace)
		.sort((a, b) => b[1] - a[1])
		.slice(0, 8);
</script>

<div class="overview">
	<header>
		<h1>Explore your lakehouse</h1>
		<p class="lede">
			Browse Iceberg namespaces and tables, inspect schemas and snapshots, and run
			sample queries.
		</p>
	</header>

	<div class="stats">
		<div class="stat">
			<span class="num">{($namespaceList ?? []).length}</span>
			<span class="lbl">namespaces</span>
		</div>
		<div class="stat">
			<span class="num">{loading ? '—' : tables.length.toLocaleString()}</span>
			<span class="lbl">tables</span>
		</div>
	</div>

	{#if $recentTables.length}
		<section>
			<h2>Recently viewed</h2>
			<ul class="chips">
				{#each $recentTables as item (item.namespace + '.' + item.table)}
					<li>
						<button type="button" on:click={() => selectNamespaceAndTable(item.namespace, item.table)}>
							<span class="chip-ns">{item.namespace}</span>
							<span class="chip-tbl">{item.table}</span>
						</button>
					</li>
				{/each}
			</ul>
		</section>
	{/if}

	<section>
		<h2>Namespaces</h2>
		{#if loading}
			<SkeletonText paragraph lines={4} />
		{:else if error}
			<p class="err">Could not load the table list: {error.message}</p>
		{:else}
			<ul class="ns-list">
				{#each topNamespaces as [name, count] (name)}
					<li>
						<button type="button" class="ns-row" on:click={() => selectNamespace(name)}>
							<span class="ns-name">{name}</span>
							<span
								class="ns-bar"
								style="--w: {Math.max(4, (count / topNamespaces[0][1]) * 100)}%"
							></span>
							<span class="ns-count">{count}</span>
						</button>
					</li>
				{/each}
			</ul>
			{#if Object.keys(byNamespace).length > topNamespaces.length}
				<p class="more">
					and {Object.keys(byNamespace).length - topNamespaces.length} more — use
					<strong>Show All Namespaces</strong> in the sidebar.
				</p>
			{/if}
		{/if}
	</section>
</div>

<style>
	.overview {
		padding: 2rem 0 3rem;
		max-width: 56rem;
	}
	h1 {
		margin: 0 0 0.5rem;
		font-size: 2rem;
		font-weight: 300;
		color: var(--cds-text-01, #161616);
	}
	.lede {
		margin: 0;
		color: var(--cds-text-02, #525252);
		max-width: 40rem;
	}
	.stats {
		display: flex;
		gap: 3rem;
		margin: 2rem 0 2.5rem;
	}
	.stat {
		display: flex;
		flex-direction: column;
	}
	.num {
		font-size: 2.25rem;
		font-weight: 600;
		line-height: 1.1;
		color: var(--cds-text-01, #161616);
		font-variant-numeric: tabular-nums;
	}
	.lbl {
		font-size: 0.75rem;
		text-transform: uppercase;
		letter-spacing: 0.02em;
		color: var(--cds-text-02, #525252);
	}
	section {
		margin-bottom: 2.5rem;
	}
	h2 {
		margin: 0 0 1rem;
		font-size: 0.875rem;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.02em;
		color: var(--cds-text-02, #525252);
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		padding-bottom: 0.5rem;
	}
	.chips {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
	}
	.chips button {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 0.125rem;
		background-color: var(--cds-layer, #f4f4f4);
		border: 1px solid var(--cds-ui-03, #e0e0e0);
		border-radius: 2px;
		padding: 0.5rem 0.75rem;
		cursor: pointer;
		font: inherit;
		text-align: left;
	}
	.chips button:hover {
		border-color: var(--cds-interactive-01, #0f62fe);
	}
	.chip-ns {
		font-size: 0.6875rem;
		color: var(--cds-text-02, #525252);
	}
	.chip-tbl {
		color: var(--cds-text-01, #161616);
	}
	.ns-list {
		list-style: none;
		margin: 0;
		padding: 0;
		display: grid;
		gap: 0.5rem;
	}
	.ns-row {
		display: grid;
		grid-template-columns: minmax(8rem, 14rem) 1fr auto;
		align-items: center;
		gap: 1rem;
		width: 100%;
		background: none;
		border: none;
		padding: 0.25rem 0.25rem;
		font: inherit;
		color: inherit;
		cursor: pointer;
		text-align: left;
	}
	.ns-row:hover {
		background-color: var(--cds-layer, #f4f4f4);
	}
	.ns-row:hover .ns-name {
		color: var(--cds-interactive-01, #0f62fe);
	}
	.ns-row:focus-visible {
		outline: 2px solid var(--cds-interactive-01, #0f62fe);
		outline-offset: -2px;
	}
	.ns-name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.ns-bar {
		height: 0.5rem;
		background-color: var(--cds-ui-03, #e0e0e0);
		position: relative;
	}
	.ns-bar::before {
		content: '';
		position: absolute;
		inset: 0 auto 0 0;
		width: var(--w);
		background-color: var(--cds-interactive-01, #0f62fe);
	}
	.ns-count {
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
	}
	.more {
		margin: 1rem 0 0;
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
	}
	.err {
		color: var(--cds-text-error, #da1e28);
	}
	@media (max-width: 672px) {
		.ns-row {
			grid-template-columns: 1fr auto;
		}
		.ns-bar {
			display: none;
		}
	}
</style>
