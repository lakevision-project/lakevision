<script>
	/** Snapshots tab: commit history for the selected table. */
	import VirtualTable from '../VirtTable3.svelte';
	import ResourcePanel from './ResourcePanel.svelte';
	import { createTableResource } from '$lib/api/tableResource';

	export let tableKey = '';
	export let pageSessionId;

	const snapshots = createTableResource('snapshots', { initial: [] });

	$: if (tableKey) snapshots.load(tableKey, pageSessionId);
	else snapshots.reset();
</script>

<br />
<ResourcePanel
	resource={$snapshots}
	rows={6}
	columns={6}
	label="snapshots"
	emptyMessage="No snapshots for this table."
>
	<VirtualTable
		data={$snapshots.data}
		columns={$snapshots.data[0]}
		rowHeight={35}
		enableSearch={true}
		storageKey="snapshots"
	/>
	<p class="total">Total items: {$snapshots.data.length}</p>
</ResourcePanel>

<style>
	.total {
		margin-top: 1rem;
		color: var(--cds-text-02, #525252);
	}
</style>
