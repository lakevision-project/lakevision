<script>
	/**
	 * Sample Data tab: a bounded row sample from the selected table.
	 *
	 * The row limit is capped: the full result crosses the wire and is held in
	 * memory, and VirtTable3 additionally builds a search string per row.
	 */
	import { Dropdown } from 'carbon-components-svelte';
	import VirtualTable from '../VirtTable3.svelte';
	import ResourcePanel from './ResourcePanel.svelte';
	import { sample_limit } from '$lib/stores';
	import { createTableResource } from '$lib/api/tableResource';

	export let tableKey = '';
	export let pageSessionId;

	/** Offered row counts. Kept modest deliberately -- see the note above. */
	const SAMPLE_LIMITS = [10, 50, 100, 500, 1000];

	const sample = createTableResource('sample', { initial: [] });

	// Include whatever limit is already in the store (e.g. restored from a URL)
	// so the dropdown can represent it.
	$: limitItems = [...new Set([...SAMPLE_LIMITS, $sample_limit])]
		.filter((n) => Number.isFinite(n) && n > 0)
		.sort((a, b) => a - b)
		.map((n) => ({ id: n, text: String(n) }));

	let lastLoaded = null;

	$: if (!tableKey) {
		sample.reset();
		lastLoaded = null;
	} else if (lastLoaded !== `${tableKey}:${$sample_limit}`) {
		lastLoaded = `${tableKey}:${$sample_limit}`;
		sample.load(tableKey, pageSessionId, { suffix: `?sample_limit=${$sample_limit}` });
	}

	$: rows = $sample.data ?? [];
	$: tableHeight = rows.length > 13 ? 500 : (rows.length + 1) * 35;
</script>

<br />
<div class="sample-label">Select # of rows to sample</div>
<Dropdown
	hideLabel
	items={limitItems}
	selectedId={$sample_limit}
	titleText="Sample Limit"
	itemToString={(item) => item?.text ?? ''}
	on:select={(e) => sample_limit.set(e.detail.selectedId)}
/>

<ResourcePanel
	resource={$sample}
	rows={8}
	columns={5}
	label="sample data"
	emptyMessage="No rows returned."
>
	<VirtualTable
		data={rows}
		columns={rows[0]}
		rowHeight={35}
		{tableHeight}
		enableSearch={true}
		storageKey="sample"
	/>
	<p class="total">Sample items: {rows.length}</p>
</ResourcePanel>

<style>
	.sample-label {
		margin-bottom: 20px;
	}
	.total {
		margin-top: 0.75rem;
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
	}
</style>
