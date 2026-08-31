<script>
	/**
	 * Summary tab.
	 *
	 * Layout is deliberately hierarchical: the four numbers people come here for
	 * (records, size, files, snapshots) read first as tiles, then identity/location
	 * metadata, then the detail sections. Previously all five panels were bordered
	 * key/value tables of identical weight.
	 */
	import { ExpandableTile, Search } from 'carbon-components-svelte';
	import { Catalog, DataTable, Db2Database, Time } from 'carbon-icons-svelte';
	import JsonTable from '../JsonTable.svelte';
	import SchemaTable from './SchemaTable.svelte';
	import PropertyGroups from './PropertyGroups.svelte';
	import ResourcePanel from './ResourcePanel.svelte';
	import Section from './Section.svelte';
	import StatTile from './StatTile.svelte';
	import KeyValueList from './KeyValueList.svelte';
	import { createTableResource } from '$lib/api/tableResource';
	import {
		averageFileSize,
		compactCount,
		exactCount,
		isAbbreviated,
		parseSize,
		relativeTime
	} from '$lib/format';

	export let tableKey = '';
	export let pageSessionId;
	/** Namespace-level properties supplied by the authz plugin, if any. */
	export let namespaceProperties = null;
	/** Table-level authz properties; a `restricted` flag is surfaced below. */
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

	$: s = $summary.data ?? {};

	// Field names used as partition/sort keys, so the schema can mark them and the
	// three panels stop having to be joined by eye.
	$: partitionFields = ($partitionSpecs.data ?? [])
		.map((/** @type {any} */ p) => p?.Field)
		.filter(Boolean);
	$: sortFields = ($sortOrder.data ?? []).map((/** @type {any} */ o) => o?.Field).filter(Boolean);

	/**
	 * An empty optional panel collapses itself, so a table with no partitioning,
	 * no sort order and no properties does not scroll past three headers that each
	 * say only that something is absent. Gated on `loaded && !error` so a
	 * still-loading or failed panel stays open with its skeleton or error visible.
	 *
	 * @param {{ data: any, error: Error | null, loaded: boolean }} r
	 */
	function isEmptyResource(r) {
		if (!r.loaded || r.error) return false;
		const d = r.data;
		if (d == null) return true;
		if (Array.isArray(d)) return d.length === 0;
		if (typeof d === 'object') return Object.keys(d).length === 0;
		return false;
	}

	// Held here so the control can sit in the section header; SchemaTable applies it.
	let schemaFilter = '';

	// Derived figures the API does not return, computed from what it does.
	$: avgFile = averageFileSize(parseSize(s['Total file size']), s['Total data files']);
	$: lastUpdatedRelative = relativeTime(s['Last updated (UTC)']);
	$: deleteFiles = Number(String(s['Total delete files'] ?? '0').replace(/,/g, '')) || 0;

	// Identity/location metadata, separated from the headline metrics.
	$: identity = {
		Location: s['Location'] ?? '',
		'Current snapshot': s['Current snapshotid'] ?? '',
		'Last updated (UTC)': s['Last updated (UTC)'] ?? '',
		'Format version': s['Format version'] ?? '',
		'Identifier fields': Array.isArray(s['Identifier fields'])
			? s['Identifier fields'].join(', ')
			: (s['Identifier fields'] ?? ''),
		...(tableProperties && 'restricted' in tableProperties
			? { Restricted: String(tableProperties.restricted) }
			: {})
	};
</script>

<div class="summary">
	<!-- Headline metrics -->
	<ResourcePanel resource={$summary} skeleton="text" rows={4} label="summary">
		<div class="stats">
			<StatTile
				label="Total records"
				icon={DataTable}
				value={compactCount(s['Total records'])}
				caption={isAbbreviated(s['Total records']) ? exactCount(s['Total records']) : ''}
			/>
			<StatTile
				label="Total size"
				icon={Db2Database}
				value={s['Total file size'] ?? '—'}
				caption={avgFile ? `${avgFile} avg / file` : ''}
			/>
			<StatTile
				label="Data files"
				icon={Catalog}
				value={compactCount(s['Total data files'])}
				caption={deleteFiles > 0
					? `${exactCount(s['Total delete files'])} delete files`
					: isAbbreviated(s['Total data files'])
						? exactCount(s['Total data files'])
						: 'no delete files'}
				muted={deleteFiles > 0 ? false : false}
			/>
			<StatTile
				label="Snapshots"
				icon={Time}
				value={compactCount(s['Total snapshots'])}
				caption={lastUpdatedRelative ? `updated ${lastUpdatedRelative}` : ''}
			/>
		</div>
	</ResourcePanel>

	<!-- Schema is the primary thing people read, so it leads at full width rather
	     than competing for half with an unbounded property list. -->
	<Section title="Schema" count={($schema.data ?? []).length}>
		<svelte:fragment slot="actions">
			{#if ($schema.data ?? []).length > 8}
				<div class="section-search">
					<Search
						size="sm"
						bind:value={schemaFilter}
						labelText="Filter schema fields"
						placeholder="Filter fields..."
					/>
				</div>
			{/if}
		</svelte:fragment>
		<ResourcePanel resource={$schema} rows={6} columns={5} label="schema">
			<SchemaTable rows={$schema.data} {partitionFields} {sortFields} filter={schemaFilter} />
		</ResourcePanel>
	</Section>

	<!-- Short, fixed-size panels sit together; none of them grows unboundedly. -->
	<div class="columns">
		<div class="col">
			<Section title="Table details" collapsible={false}>
				<ResourcePanel resource={$summary} skeleton="text" rows={5} label="table details">
					<KeyValueList
						data={identity}
						monospaceKeys={['Location', 'Current snapshot']}
						copyableKeys={['Location']}
					/>
				</ResourcePanel>
			</Section>
		</div>
		<div class="col">
			<Section
				title="Partitioning"
				count={($partitionSpecs.data ?? []).length}
				collapseWhenEmpty={isEmptyResource($partitionSpecs)}
			>
				<ResourcePanel
					resource={$partitionSpecs}
					rows={2}
					columns={3}
					label="partition specs"
					emptyMessage="Table is not partitioned."
				>
					<JsonTable jsonData={$partitionSpecs.data} orient="table" />
				</ResourcePanel>
			</Section>

			<Section
				title="Sort order"
				count={($sortOrder.data ?? []).length}
				collapseWhenEmpty={isEmptyResource($sortOrder)}
			>
				<ResourcePanel
					resource={$sortOrder}
					rows={2}
					columns={4}
					label="sort order"
					emptyMessage="No sort order defined."
				>
					<JsonTable jsonData={$sortOrder.data} orient="table" />
				</ResourcePanel>
			</Section>
		</div>
	</div>

	<!-- Last: this is the one panel whose height is driven by the table's own
	     configuration, ranging from 1 to 26 entries across our catalog. -->
	<Section
		title="Properties"
		count={Object.keys($properties.data ?? {}).length}
		collapseWhenEmpty={isEmptyResource($properties)}
	>
		<ResourcePanel resource={$properties} skeleton="text" rows={4} label="properties">
			<PropertyGroups properties={$properties.data} />
		</ResourcePanel>
	</Section>

	{#if namespaceProperties}
		<ExpandableTile light>
			<div slot="below">{namespaceProperties}</div>
		</ExpandableTile>
	{/if}
</div>

<style>
	.summary {
		padding-top: 1.5rem;
		min-width: 0;
	}
	.stats {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(13rem, 1fr));
		gap: 1rem;
		margin-bottom: 2.5rem;
	}
	.columns {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(24rem, 1fr));
		gap: 0 3rem;
		align-items: start;
	}
	.col {
		min-width: 0;
	}
	.section-search {
		width: 13rem;
	}
	.no-match {
		color: var(--cds-text-02, #525252);
		margin: 0.75rem 0 0;
	}
</style>
