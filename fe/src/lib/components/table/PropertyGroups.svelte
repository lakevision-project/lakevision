<script>
	/**
	 * Table properties, grouped by key namespace.
	 *
	 * A flat list does not scale: tables in a real catalog carry 1–26 properties,
	 * and the long tail mixes unrelated concerns (`write.*` file-format settings,
	 * `cluster.*` layout state, vendor `scalar.*`/`vector.*` keys) while multi-line
	 * S3 path values push everything else below the fold.
	 *
	 * Grouping on the first dotted segment restores structure, the shared prefix is
	 * dropped from each displayed key, and values stay on one line with the full
	 * text available on demand.
	 */
	import { CopyButton, Search } from 'carbon-components-svelte';
	import { groupProperties } from '$lib/schemaTypes';

	/** @type {Record<string, any>} */
	export let properties = {};
	/** Groups larger than this start collapsed. */
	/** @type {number} */
	export let previewCount = 5;

	let filter = '';
	/** @type {Record<string, boolean>} */
	let expanded = {};
	/** @type {string | null} */
	let detail = null;

	$: needle = filter.trim().toLowerCase();
	$: groups = groupProperties(properties)
		.map((g) => ({
			...g,
			entries: needle
				? g.entries.filter(
						(/** @type {any} */ e) =>
							e.key.toLowerCase().includes(needle) ||
							String(e.value ?? '').toLowerCase().includes(needle)
					)
				: g.entries
		}))
		.filter((g) => g.entries.length > 0);

	$: total = Object.keys(properties ?? {}).length;

	/** A filtered view should show matches, not hide them behind a toggle. */
	$: showAll = Boolean(needle);

	/** @param {any} value */
	function isLong(value) {
		return String(value ?? '').length > 48;
	}
</script>

{#if total === 0}
	<p class="empty">No table properties set.</p>
{:else}
	{#if total > previewCount}
		<div class="filter">
			<Search
				size="sm"
				bind:value={filter}
				labelText="Filter properties"
				placeholder="Filter properties..."
			/>
		</div>
	{/if}

	{#if groups.length === 0}
		<p class="empty">No properties match “{filter}”.</p>
	{/if}

	{#each groups as group (group.group)}
		{@const isOpen = showAll || expanded[group.group]}
		{@const shown = isOpen ? group.entries : group.entries.slice(0, previewCount)}
		{@const hidden = group.entries.length - shown.length}
		<section class="group">
			<h4>
				<span class="gname">{group.group}</span>
				<span class="gcount">{group.entries.length}</span>
			</h4>
			<dl>
				{#each shown as entry (entry.key)}
					<dt title={entry.key}>{entry.label}</dt>
					<dd>
						{#if isLong(entry.value)}
							<button
								type="button"
								class="value long"
								title="Show full value"
								on:click={() => (detail = detail === entry.key ? null : entry.key)}
							>
								{entry.value}
							</button>
							<CopyButton
								text={String(entry.value)}
								iconDescription="Copy value"
								feedback="Copied"
							/>
						{:else}
							<span class="value">{entry.value === '' ? '—' : entry.value}</span>
						{/if}
						{#if detail === entry.key}
							<pre class="full">{entry.value}</pre>
						{/if}
					</dd>
				{/each}
			</dl>
			{#if hidden > 0}
				<button type="button" class="more" on:click={() => (expanded[group.group] = true)}>
					Show {hidden} more
				</button>
			{:else if isOpen && !showAll && group.entries.length > previewCount}
				<button type="button" class="more" on:click={() => (expanded[group.group] = false)}>
					Show fewer
				</button>
			{/if}
		</section>
	{/each}
{/if}

<style>
	.filter {
		margin-bottom: 1rem;
		max-width: 18rem;
	}
	.group {
		margin-bottom: 1.25rem;
	}
	h4 {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		margin: 0 0 0.375rem;
		font-size: 0.6875rem;
		font-weight: 600;
		letter-spacing: 0.04em;
		text-transform: uppercase;
		color: var(--cds-text-02, #525252);
	}
	.gname {
		font-family: var(--cds-code-01-font-family, ui-monospace, SFMono-Regular, Menlo, monospace);
	}
	.gcount {
		font-variant-numeric: tabular-nums;
		background-color: var(--cds-ui-03, #e0e0e0);
		border-radius: 0.75rem;
		padding: 0 0.375rem;
	}
	dl {
		display: grid;
		grid-template-columns: minmax(7rem, max-content) 1fr;
		gap: 0 1.25rem;
		margin: 0;
	}
	dt,
	dd {
		padding: 0.3125rem 0;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		min-width: 0;
		font-size: 0.8125rem;
	}
	dt {
		color: var(--cds-text-02, #525252);
		font-family: var(--cds-code-01-font-family, ui-monospace, SFMono-Regular, Menlo, monospace);
		overflow-wrap: anywhere;
	}
	dd {
		margin: 0;
		display: flex;
		align-items: flex-start;
		gap: 0.25rem;
		color: var(--cds-text-01, #161616);
		min-width: 0;
	}
	.value {
		min-width: 0;
		overflow-wrap: anywhere;
	}
	/* Long values (S3 paths) stay on one line; the full text is one click away,
	   rather than wrapping to three lines and dominating the panel. */
	.value.long {
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		color: var(--cds-link-01, #0f62fe);
		cursor: pointer;
		text-align: left;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		max-width: 100%;
		font-family: var(--cds-code-01-font-family, ui-monospace, SFMono-Regular, Menlo, monospace);
		font-size: 0.75rem;
	}
	.full {
		grid-column: 1 / -1;
		margin: 0.375rem 0 0;
		padding: 0.5rem 0.625rem;
		background-color: var(--cds-ui-01, #f4f4f4);
		font-size: 0.75rem;
		white-space: pre-wrap;
		overflow-wrap: anywhere;
		width: 100%;
	}
	.more {
		margin-top: 0.5rem;
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		font-size: 0.75rem;
		color: var(--cds-link-01, #0f62fe);
		cursor: pointer;
	}
	.more:hover {
		text-decoration: underline;
	}
	.empty {
		color: var(--cds-text-02, #525252);
		margin: 0;
	}
	@media (max-width: 672px) {
		dl {
			grid-template-columns: 1fr;
		}
		dt {
			border-bottom: none;
			padding-bottom: 0;
		}
	}
</style>
