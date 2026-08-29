<script>
	import { Modal } from 'carbon-components-svelte';
	import { Search } from 'carbon-components-svelte';
	import { createVirtualizer } from '@tanstack/svelte-virtual';
	import { onMount } from 'svelte';
	import { page } from '$app/stores';

	export let data = [];
	/**
	 * Column definition object; callers typically pass `data[0]`, which is
	 * undefined while a table is being cleared or before the first row arrives.
	 */
	export let columns = [];
	export let rowHeight = 40; // Default height if no array is provided
    export let rowHeights = null; // New prop to accept an array of heights
	export let tableHeight = 500;
	export let defaultColumnWidth = 200;
	export let enableSearch = false;
	export let disableVirtualization = false; // New prop to disable virtualization
	let containerRef;

	// Never call Object.keys on a null/undefined `columns`: clearing the table
	// selection briefly leaves it unset while this component is still mounted,
	// which threw "Cannot convert undefined or null to object".
	$: columnKeys = columns && typeof columns === 'object' ? Object.keys(columns) : [];

	// Sorting state
	let sortKey = null;
	let sortOrder = 'asc';

	// Popover state
	let showPopover = false;
	let popoverContent = '';
	let popoverPosition = { top: 0, left: 0 };
	export let columnWidths = {};
	/**
	 * Namespaces persisted column widths. Every instance previously shared one
	 * "columnWidths" key, so resizing a column in one table corrupted the widths
	 * of every other table -- they have entirely different column sets.
	 */
	export let storageKey = 'default';
	let startX, startWidth, columnKey;
	let searchQuery = '';

	$: storageId = `lakevision.columnWidths.${storageKey}`;

	onMount(() => {
		try {
			const stored = localStorage.getItem(storageId);
			if (stored) columnWidths = { ...columnWidths, ...JSON.parse(stored) };
		} catch (err) {
			// Malformed or unavailable storage (private mode, disabled cookies)
			// must not take the table down.
			console.warn('Ignoring stored column widths:', err);
		}
	});

	let reset_cw = $page.url.searchParams.get('reset_cw');

	$: formattedData = data.map((row) => ({
		original: row,
		searchString: Object.values(row)
			.map((value) => formatForSearch(value))
			.join(' ')
	}));

	function formatForSearch(value) {
		if (value === null || value === undefined) { return ''; }
		if (Array.isArray(value)) {
			return value.map((item) => formatForSearch(item)).join(' ');
		}
		if (typeof value === 'object') {
			return Object.entries(value)
				.map(([key, val]) => `${key}: ${formatForSearch(val)}`)
				.join(' ');
		}
		return String(value).toLowerCase();
	}

	$: filteredData = formattedData
		.filter(({ searchString }) => searchString.includes(searchQuery.toLowerCase()))
		.map(({ original }) => original);

	$: displayedData = sortKey ? [...filteredData].sort(compareRows) : filteredData;

	/** Compare two rows on the active sort column, numerically where possible. */
	function compareRows(a, b) {
		const direction = sortOrder === 'asc' ? 1 : -1;
		const left = a?.[sortKey];
		const right = b?.[sortKey];
		if (left === right) return 0;
		if (left === null || left === undefined) return 1; // nulls last
		if (right === null || right === undefined) return -1;
		const leftNum = Number(left);
		const rightNum = Number(right);
		if (!Number.isNaN(leftNum) && !Number.isNaN(rightNum)) {
			return (leftNum - rightNum) * direction;
		}
		return String(left).localeCompare(String(right)) * direction;
	}

	// Virtualizer instance
	$: rowVirtualizer = createVirtualizer({
		count: displayedData.length,
		getScrollElement: () => containerRef,
		// Use the array of heights if provided, otherwise use the single rowHeight prop
		estimateSize: (index) => (rowHeights && rowHeights[index] ? rowHeights[index] : rowHeight)
	});

	function handleSort(columnKey) {
		if (sortKey === columnKey) {
			sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
		} else {
			sortKey = columnKey;
			sortOrder = 'asc';
		}
	}

	function handleDoubleClick(event, content) {
		popoverContent = content;
		showPopover = true;
		const rect = event.target.getBoundingClientRect();
		popoverPosition = {
			top: rect.top + rect.height + window.scrollY,
			left: rect.left + window.scrollX
		};
	}

	function formatValue(value, depth = 0) {
		if (value === null || value === undefined) {
			return '';
		}
		if (Array.isArray(value)) {
			return value.map((item) => formatValue(item, depth + 1)).join('\n');
		}
		if (typeof value === 'object') {
			return Object.entries(value)
				.map(([key, val]) => {
					const indent = '  '.repeat(depth);
					return `${indent}${key}: ${formatValue(val, depth + 1)}`;
				})
				.join(',\n');
		}
		return value;
	}

	function handleMouseDown(event, key) {
		startX = event.clientX;
		startWidth = columnWidths[key] || 200;
		columnKey = key;
		document.addEventListener('mousemove', handleMouseMove);
		document.addEventListener('mouseup', handleMouseUp);
	}
	function handleMouseMove(event) {
		const newWidth = startWidth + (event.clientX - startX);
		columnWidths[columnKey] = Math.max(newWidth, 50);
	}
	function handleMouseUp() {
		document.removeEventListener('mousemove', handleMouseMove);
		document.removeEventListener('mouseup', handleMouseUp);
		// Persist once per drag rather than on every mousemove event.
		saveColumnWidths();
	}
	function saveColumnWidths() {
		try {
			localStorage.setItem(storageId, JSON.stringify(columnWidths));
		} catch (err) {
			console.warn('Could not persist column widths:', err);
		}
	}
	function resetColumnWidths() {
		columnWidths = columnKeys.reduce((acc, key) => {
			acc[key] = defaultColumnWidth;
			return acc;
		}, {});
		try {
			localStorage.removeItem(storageId);
		} catch {
			/* nothing to clean up */
		}
		return '';
	}
	function escapeHtml(text) {
		return String(text)
			.replace(/&/g, '&amp;')
			.replace(/</g, '&lt;')
			.replace(/>/g, '&gt;')
			.replace(/"/g, '&quot;')
			.replace(/'/g, '&#039;');
	}
	function highlightMatch(text, query) {
		if (!query || !text) return escapeHtml(text);
		const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
		return escapeHtml(text).replace(regex, '<mark>$1</mark>');
	}
</script>

{#if reset_cw}
	{resetColumnWidths()}
{/if}
{#if enableSearch}
	<div class="search-container">
		<Search bind:value={searchQuery} placeholder="Search..." expandable />
	</div>
{/if}
<div
	bind:this={containerRef}
	class="table-container"
	style="height: {disableVirtualization ? 'auto' : tableHeight + 'px'}"
>
	<div class="sticky-header">
		{#each columnKeys as key}
			<div
				class="header-cell"
				style="width: {columnWidths[key] || defaultColumnWidth}px"
				title={key}
				on:click={() => handleSort(key)}
				role="button"
				tabindex="0"
				on:keypress={(e) => {
					if (e.key === 'Enter' || e.key === ' ') handleSort(key);
				}}
			>
				{key}
				{#if sortKey === key}
					{sortOrder === 'asc' ? ' ▲' : ' ▼'}
				{/if}
				<!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
				<div
					class="resize-handle"
					on:mousedown={(event) => handleMouseDown(event, key)}
					role="separator"
					aria-label="Resize column"
				/>
			</div>
		{/each}
	</div>

	{#if !disableVirtualization}
		<div style="position: relative; height: {$rowVirtualizer.getTotalSize()}px;">
			{#each $rowVirtualizer.getVirtualItems() as virtualRow (virtualRow.key)}
				<div
					class="row virtual"
					style="transform: translateY({virtualRow.start}px); height: {virtualRow.size}px;"
				>
					{#each columnKeys as key}
						<div
							class="cell"
							role="button"
							tabindex="0"
							style="width: {columnWidths[key] || defaultColumnWidth}px"
							on:dblclick={(event) =>
								handleDoubleClick(event, displayedData[virtualRow.index]?.[key])}
						>
							<slot name="cell" row={displayedData[virtualRow.index]} columnKey={key}>
								{@html highlightMatch(
									formatValue(displayedData[virtualRow.index]?.[key]),
									searchQuery
								)}
							</slot>
						</div>
					{/each}
				</div>
			{/each}
		</div>
	{:else}
		<div class="simple-body">
			{#each displayedData as row, rowIndex (row.id ?? rowIndex)}
				<div class="row">
					{#each columnKeys as key}
						<div
							class="cell"
							role="button"
							tabindex="0"
							style="width: {columnWidths[key] || defaultColumnWidth}px"
							on:dblclick={(event) => handleDoubleClick(event, row[key])}
						>
							<slot name="cell" {row} columnKey={key} {searchQuery}>
								{@html highlightMatch(formatValue(row[key]), searchQuery)}
							</slot>
						</div>
					{/each}
				</div>
			{/each}
		</div>
	{/if}
</div>

{#if showPopover}
	<Modal passiveModal bind:open={showPopover} modalHeading="" on:open on:close>
		<br />
		<pre>{formatValue(popoverContent)}</pre>
		<br />
	</Modal>
{/if}

<style>
	.table-container {
		overflow-y: auto;
		overflow-x: auto;
		position: relative;
	}
	.sticky-header {
		position: sticky;
		top: 0;
		background-color: var(--cds-ui-01, #f4f4f4);
		z-index: 2;
		display: flex;
		width: fit-content;
	}
	.header-cell,
	.cell {
		position: relative;
		padding: 6px 12px;
		/* Horizontal rules only: a full grid on every cell made dense data noisy. */
		border: none;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		text-align: left;
		white-space: nowrap;
		width: 200px;
		overflow: hidden;
		text-overflow: ellipsis;
		box-sizing: border-box;
		display: flex;
		align-items: center;
	}
	.header-cell {
		background-color: var(--cds-ui-01, #f4f4f4);
		color: var(--cds-text-01, #161616);
		cursor: pointer;
		user-select: none;
		font-size: 0.75rem;
		font-weight: 600;
		letter-spacing: 0.02em;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
	}
	.header-cell:hover {
		background-color: var(--cds-ui-03, #e0e0e0);
	}
	.cell {
		color: var(--cds-text-01, #161616);
		font-size: 0.8125rem;
		/* Numeric columns line up when digits are tabular. */
		font-variant-numeric: tabular-nums;
	}
	.row:hover .cell,
	.simple-body .row:hover .cell {
		background-color: var(--cds-layer, #f4f4f4);
	}
	.resize-handle {
		position: absolute;
		right: 0;
		top: 0;
		width: 5px;
		height: 100%;
		cursor: ew-resize;
		background-color: transparent;
	}
	.row.virtual {
		display: flex;
		position: absolute;
		width: fit-content;
	}
	.simple-body .row {
		display: flex;
		width: fit-content;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
	}
	.simple-body .row:last-child {
		border-bottom: none;
	}
	.search-container {
		display: flex;
		justify-content: flex-end;
		margin-bottom: 10px;
	}
</style>
