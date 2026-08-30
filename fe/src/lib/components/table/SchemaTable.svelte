<script>
	/**
	 * Schema listing with semantic colour.
	 *
	 * Replaces a plain grid where `string`, `double` and `date` were identical grey
	 * text and the Required column read "False" — which takes a beat to parse. Types
	 * are tagged by family, nullability is stated positively, and fields that are
	 * partition or sort keys are marked so the relationship between the Schema,
	 * Partitioning and Sort order panels is visible without joining them by eye.
	 */
	import { Tag } from 'carbon-components-svelte';
	import { typeTagColor } from '$lib/schemaTypes';

	/** Rows from /schema: {Field_id, Field, DataType, Required, Comments}. */
	/** @type {Array<Record<string, any>>} */
	export let rows = [];
	/** Field names used as partition keys. */
	/** @type {string[]} */
	export let partitionFields = [];
	/** Field names used in the sort order. */
	/** @type {string[]} */
	export let sortFields = [];
	/** Free-text filter applied by the parent. */
	/** @type {string} */
	export let filter = '';

	$: needle = filter.trim().toLowerCase();
	$: visible = needle
		? rows.filter((/** @type {any} */ r) =>
				Object.values(r ?? {}).some((v) => String(v ?? '').toLowerCase().includes(needle))
			)
		: rows;

	/** @param {any} row */
	function isRequired(row) {
		const v = row?.Required;
		return v === true || String(v).toLowerCase() === 'true';
	}
</script>

{#if visible.length === 0}
	<p class="empty">
		{rows.length === 0 ? 'No schema information.' : `No fields match “${filter}”.`}
	</p>
{:else}
	<div class="wrap">
		<table>
			<thead>
				<tr>
					<th class="num" scope="col">#</th>
					<th scope="col">Field</th>
					<th scope="col">Type</th>
					<th scope="col">Nullability</th>
					<th scope="col">Comment</th>
				</tr>
			</thead>
			<tbody>
				{#each visible as row (row.Field_id ?? row.Field)}
					{@const partition = partitionFields.includes(row.Field)}
					{@const sort = sortFields.includes(row.Field)}
					<tr>
						<td class="num">{row.Field_id}</td>
						<td>
							<div class="field">
								<span class="name">{row.Field}</span>
								{#if partition}<Tag type="cyan" size="sm">partition</Tag>{/if}
								{#if sort}<Tag type="warm-gray" size="sm">sort</Tag>{/if}
							</div>
						</td>
						<td>
							<Tag type={typeTagColor(row.DataType)} size="sm">{row.DataType}</Tag>
						</td>
						<td class="nullability">
							{#if isRequired(row)}
								<span class="required">required</span>
							{:else}
								<span class="optional">optional</span>
							{/if}
						</td>
						<td class="comment">{row.Comments ?? ''}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}

<style>
	.wrap {
		overflow-x: auto;
	}
	table {
		width: 100%;
		border-collapse: collapse;
		font-size: 0.8125rem;
	}
	th {
		text-align: left;
		font-size: 0.75rem;
		font-weight: 600;
		letter-spacing: 0.02em;
		color: var(--cds-text-02, #525252);
		padding: 0.5rem 0.75rem;
		background-color: var(--cds-ui-01, #f4f4f4);
		white-space: nowrap;
	}
	td {
		padding: 0.375rem 0.75rem;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		color: var(--cds-text-01, #161616);
		vertical-align: middle;
	}
	tbody tr:hover td {
		background-color: var(--cds-layer, #f4f4f4);
	}
	.num {
		width: 3rem;
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
		text-align: right;
	}
	.field {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		flex-wrap: wrap;
	}
	.name {
		font-family: var(--cds-code-01-font-family, ui-monospace, SFMono-Regular, Menlo, monospace);
	}
	.nullability {
		white-space: nowrap;
	}
	.required {
		color: var(--cds-text-01, #161616);
		font-weight: 600;
	}
	.optional {
		color: var(--cds-text-02, #525252);
	}
	.comment {
		color: var(--cds-text-02, #525252);
		overflow-wrap: anywhere;
	}
	.empty {
		color: var(--cds-text-02, #525252);
		margin: 0;
		padding: 0.5rem 0;
	}
</style>
