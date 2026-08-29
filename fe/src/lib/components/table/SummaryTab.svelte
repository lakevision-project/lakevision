<script>
	/**
	 * Summary tab: schema, summary, properties, partition specs and sort order.
	 *
	 * These five resources load together whenever the selected table changes.
	 */
	import { Grid, Row, Column, ExpandableTile } from 'carbon-components-svelte';
	import JsonTable from '../JsonTable.svelte';
	import VirtualTable from '../VirtTable3.svelte';
	import ResourcePanel from './ResourcePanel.svelte';
	import { createTableResource } from '$lib/api/tableResource';

	export let tableKey = '';
	export let pageSessionId;
	/** Namespace-level properties supplied by the authz plugin, if any. */
	export let namespaceProperties = null;
	/** Table-level authz properties; a `restricted` flag is merged into the summary. */
	export let tableProperties = null;

	const summary = createTableResource('summary', { initial: {} });
	const schema = createTableResource('schema', { initial: [] });
	const properties = createTableResource('properties', { initial: {} });
	const partitionSpecs = createTableResource('partition-specs', { initial: [] });
	const sortOrder = createTableResource('sort-order', { initial: [] });

	const resources = [summary, schema, properties, partitionSpecs, sortOrder];

	$: if (tableKey) {
		for (const resource of resources) resource.load(tableKey, pageSessionId);
	} else {
		for (const resource of resources) resource.reset();
	}

	// The authz plugin can mark a table restricted; surface it alongside the
	// server-provided summary rather than mutating the fetched object.
	$: displaySummary =
		tableProperties && 'restricted' in tableProperties
			? { ...($summary.data ?? {}), Restricted: tableProperties.restricted }
			: ($summary.data ?? {});
</script>

<br />
<Grid>
	<Row>
		<Column aspectRatio="2x1">
			<h5>Summary</h5>
			<ResourcePanel
				resource={$summary}
				skeleton="text"
				rows={6}
				label="summary"
				emptyMessage="No summary available."
			>
				<JsonTable jsonData={displaySummary} orient="kv" />
			</ResourcePanel>
		</Column>
		<Column aspectRatio="2x1">
			<h5>Schema</h5>
			<ResourcePanel
				resource={$schema}
				rows={6}
				columns={5}
				label="schema"
				emptyMessage="No schema information."
			>
				<VirtualTable
					data={$schema.data}
					columns={$schema.data[0]}
					rowHeight={37}
					tableHeight={360}
					defaultColumnWidth={121}
					storageKey="schema"
				/>
			</ResourcePanel>
		</Column>
	</Row>
	<Row>
		<Column aspectRatio="2x1">
			<div class="section-gap">
				<h5>Properties</h5>
				<ResourcePanel
					resource={$properties}
					skeleton="text"
					rows={4}
					label="properties"
					emptyMessage="No table properties set."
				>
					<JsonTable jsonData={$properties.data} orient="kv" />
				</ResourcePanel>
			</div>
		</Column>
		<Column aspectRatio="2x1">
			<div class="section-gap">
				<h5>Partition Specs</h5>
				<ResourcePanel
					resource={$partitionSpecs}
					rows={2}
					columns={3}
					label="partition specs"
					emptyMessage="Table is not partitioned."
				>
					<JsonTable jsonData={$partitionSpecs.data} orient="table" />
				</ResourcePanel>

				<h5>Sort Order</h5>
				<ResourcePanel
					resource={$sortOrder}
					rows={2}
					columns={4}
					label="sort order"
					emptyMessage="No sort order defined."
				>
					<JsonTable jsonData={$sortOrder.data} orient="table" />
				</ResourcePanel>
			</div>
		</Column>
	</Row>
</Grid>
{#if namespaceProperties}
	<ExpandableTile light>
		<div slot="below">{namespaceProperties}</div>
	</ExpandableTile>
{/if}

<style>
	.section-gap {
		margin-top: 2rem;
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}
	h5 {
		margin-bottom: 0.5rem;
	}
</style>
