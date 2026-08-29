<script>
	/** Partitions tab: per-partition record/file counts and sizes. */
	import VirtualTable from '../VirtTable3.svelte';
	import ResourcePanel from './ResourcePanel.svelte';
	import { createTableResource } from '$lib/api/tableResource';

	export let tableKey = '';
	export let pageSessionId;

	const partitions = createTableResource('partitions', { initial: [] });

	$: if (tableKey) partitions.load(tableKey, pageSessionId);
	else partitions.reset();
</script>

<br />
<ResourcePanel
	resource={$partitions}
	rows={6}
	columns={6}
	label="partitions"
	emptyMessage="No partition data for this table."
>
	<VirtualTable
		data={$partitions.data}
		columns={$partitions.data[0]}
		rowHeight={35}
		enableSearch={true}
		storageKey="partitions"
	/>
	<p class="total">Total items: {$partitions.data.length}</p>
</ResourcePanel>

<style>
	.total {
		margin-top: 1rem;
		color: var(--cds-text-02, #525252);
	}
</style>
