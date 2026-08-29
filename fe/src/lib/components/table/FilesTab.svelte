<script>
	/**
	 * Files tab: the data and delete files backing the current snapshot.
	 *
	 * Paged against the server rather than loaded whole. A table with 15k files
	 * carries ~300 MB of per-file column statistics, so the endpoint returns a
	 * curated column set one page at a time.
	 */
	import { Button, DataTableSkeleton, Select, SelectItem, ToastNotification } from 'carbon-components-svelte';
	import { CaretLeft, CaretRight } from 'carbon-icons-svelte';
	import VirtualTable from '../VirtTable3.svelte';
	import StatTile from './StatTile.svelte';
	import { compactCount, exactCount, formatBytes, isAbbreviated } from '$lib/format';

	export let tableKey = '';
	export let pageSessionId;

	const PAGE_SIZES = [25, 50, 100, 250];

	let pageSize = 50;
	let offset = 0;
	let items = [];
	let total = 0;
	let loading = false;
	/** @type {Error | null} */
	let error = null;
	let loadedKey = '';
	let sequence = 0;

	// Reset paging when the table changes, then load the first page.
	$: if (tableKey !== loadedKey) {
		loadedKey = tableKey;
		offset = 0;
		items = [];
		total = 0;
		error = null;
		if (tableKey) load();
	}

	async function load() {
		if (!tableKey) return;
		const mine = ++sequence;
		loading = true;
		error = null;
		try {
			const res = await fetch(
				`/api/tables/${encodeURIComponent(tableKey)}/files?offset=${offset}&limit=${pageSize}`,
				{ headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId } }
			);
			if (!res.ok) {
				let detail = res.statusText;
				try {
					const body = await res.json();
					detail = body.detail ?? detail;
				} catch {
					/* non-JSON error body */
				}
				throw new Error(detail);
			}
			const body = await res.json();
			if (mine !== sequence) return; // superseded
			total = body.total ?? 0;
			items = (body.items ?? []).map((row) => ({
				...row,
				// Show a readable size but keep the exact byte count discoverable.
				Size: formatBytes(Number(row['Size (bytes)'])),
				Records: Number(row.Records).toLocaleString()
			}));
		} catch (err) {
			if (mine === sequence) {
				error = /** @type {Error} */ (err);
				items = [];
			}
		} finally {
			if (mine === sequence) loading = false;
		}
	}

	/** @param {number} next */
	function goTo(next) {
		offset = Math.max(0, Math.min(next, Math.max(0, total - 1)));
		load();
	}

	function changePageSize(event) {
		pageSize = Number(event.target.value);
		offset = 0;
		load();
	}

	// Column set for the table; Size (bytes) is dropped in favour of Size.
	const columns = {
		Content: '',
		'File path': '',
		Format: '',
		Partition: '',
		Records: '',
		Size: '',
		Spec: '',
		'Sort order': ''
	};
	let columnWidths = {
		Content: 130,
		'File path': 460,
		Format: 90,
		Partition: 200,
		Records: 110,
		Size: 100,
		Spec: 70,
		'Sort order': 90
	};

	$: page = Math.floor(offset / pageSize) + 1;
	$: pages = Math.max(1, Math.ceil(total / pageSize));
	$: showingFrom = total === 0 ? 0 : offset + 1;
	$: showingTo = Math.min(offset + items.length, total);

	// Totals for the page, so the numbers on screen are explained.
	$: pageRecords = items.reduce((n, r) => n + (Number(String(r.Records).replace(/,/g, '')) || 0), 0);
	$: pageBytes = items.reduce((n, r) => n + (Number(r['Size (bytes)']) || 0), 0);
</script>

<div class="files">
	{#if error}
		<ToastNotification
			hideCloseButton
			kind="error"
			lowContrast
			title="Could not load files"
			subtitle={error.message}
		/>
	{:else if loading && items.length === 0}
		<DataTableSkeleton rowCount={8} columnCount={6} showHeader={false} showToolbar={false} />
	{:else if total === 0}
		<p class="empty">This table has no data files.</p>
	{:else}
		<div class="stats">
			<StatTile
				label="Total files"
				value={compactCount(total)}
				caption={isAbbreviated(total) ? exactCount(total) : ''}
			/>
			<StatTile
				label="Records on this page"
				value={compactCount(pageRecords)}
				caption={`across ${items.length} file${items.length === 1 ? '' : 's'}`}
			/>
			<StatTile label="Size on this page" value={formatBytes(pageBytes)} caption="" />
		</div>

		<div class="toolbar">
			<span class="range">
				Showing <strong>{showingFrom.toLocaleString()}–{showingTo.toLocaleString()}</strong>
				of {total.toLocaleString()} files
				<span class="hint">· double-click a cell to see the full value</span>
			</span>
			<div class="controls">
				<div class="size-select">
					<Select
						size="sm"
						labelText="Per page"
						hideLabel
						selected={String(pageSize)}
						on:change={changePageSize}
					>
						{#each PAGE_SIZES as n}
							<SelectItem value={String(n)} text={`${n} / page`} />
						{/each}
					</Select>
				</div>
				<Button
					size="small"
					kind="ghost"
					iconDescription="Previous page"
					icon={CaretLeft}
					disabled={offset === 0 || loading}
					on:click={() => goTo(offset - pageSize)}
				/>
				<span class="page-no">{page} / {pages.toLocaleString()}</span>
				<Button
					size="small"
					kind="ghost"
					iconDescription="Next page"
					icon={CaretRight}
					disabled={offset + pageSize >= total || loading}
					on:click={() => goTo(offset + pageSize)}
				/>
			</div>
		</div>

		<div class="table-wrap" class:busy={loading}>
			<VirtualTable
				data={items}
				{columns}
				bind:columnWidths
				rowHeight={35}
				tableHeight={Math.min(560, (items.length + 1) * 35)}
				storageKey="files"
			/>
		</div>
	{/if}
</div>

<style>
	.files {
		padding-top: 1.5rem;
		min-width: 0;
	}
	.stats {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(13rem, 1fr));
		gap: 1rem;
		margin-bottom: 1.5rem;
	}
	.toolbar {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		flex-wrap: wrap;
		margin-bottom: 0.75rem;
	}
	.range {
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
	}
	.hint {
		color: var(--cds-text-02, #525252);
		opacity: 0.85;
	}
	.controls {
		display: flex;
		align-items: center;
		gap: 0.25rem;
	}
	.size-select {
		width: 8.5rem;
		margin-right: 0.5rem;
	}
	.page-no {
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
		min-width: 4.5rem;
		text-align: center;
		font-variant-numeric: tabular-nums;
	}
	.table-wrap {
		transition: opacity 0.15s ease;
	}
	.table-wrap.busy {
		opacity: 0.55;
	}
	.empty {
		color: var(--cds-text-02, #525252);
	}
</style>
