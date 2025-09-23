<script>
	import { Modal } from 'carbon-components-svelte';
	import { Search } from 'carbon-components-svelte';
	import { createVirtualizer } from '@tanstack/svelte-virtual';
	import { onMount } from 'svelte';
	import { page } from '$app/stores';

	export let data = [];
	export let columns = [];
	export let rowHeight = 40; // Default height if no array is provided
    export let rowHeights = null; // New prop to accept an array of heights
	export let tableHeight = 500;
	export let defaultColumnWidth = 200;
	export let enableSearch = false;
	export let disableVirtualization = false; // New prop to disable virtualization
	let containerRef;

	// Sorting state
	let sortKey = null;
	let sortOrder = 'asc';

	// Popover state
	let showPopover = false;
	let popoverContent = '';
	let popoverPosition = { top: 0, left: 0 };
	export let columnWidths = {};
	let startX, startWidth, columnKey;
	let searchQuery = '';

	onMount(() => {
		const storedWidths = localStorage.getItem('columnWidths');
		if (storedWidths) {
			columnWidths = JSON.parse(storedWidths);
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

	$: displayedData = [...filteredData].sort((a, b) => {
		if (!sortKey) return 0;
		if (a[sortKey] < b[sortKey]) return sortOrder === 'asc' ? -1 : 1;
		if (a[sortKey] > b[sortKey]) return sortOrder === 'asc' ? 1 : -1;
		return 0;
	});

	// Virtualizer instance
	$: rowVirtualizer = createVirtualizer({
		count: filteredData.length,
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
		saveColumnWidths();
	}
	function handleMouseUp() {
		document.removeEventListener('mousemove', handleMouseMove);
		document.removeEventListener('mouseup', handleMouseUp);
	}
	function saveColumnWidths() {
		localStorage.setItem('columnWidths', JSON.stringify(columnWidths));
	}
	function resetColumnWidths() {
		columnWidths = Object.keys(columns).reduce((acc, key) => {
			acc[key] = defaultColumnWidth;
			return acc;
		}, {});
		localStorage.removeItem('columnWidths');
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
		{#each Object.keys(columns) as key}
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
					{#each Object.keys(columns) as key}
						<div
							class="cell"
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
			{#each displayedData as row (row.id)}
				<div class="row">
					{#each Object.keys(columns) as key}
						<div
							class="cell"
							style="width: {columnWidths[key] || defaultColumnWidth}px"
							on:dblclick={(event) => handleDoubleClick(event, row[key])}
						>
							<slot name="cell" {row} columnKey={key}>
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
		background-color: #f4f4f4;
		z-index: 2;
		display: flex;
		width: fit-content;
	}
	.header-cell,
	.cell {
		position: relative;
		padding: 8px;
		border: 1px solid #ddd;
		text-align: left;
		white-space: nowrap;
		width: 200px;
		overflow: hidden;
		text-overflow: ellipsis;
		box-sizing: border-box;
		display: flex;
		align-items: center;
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
		border-bottom: 1px solid #ddd;
	}
	.simple-body .row:last-child {
		border-bottom: none;
	}
	.search-container {
		display: flex;
		justify-content: flex-end;
		margin-bottom: 10px;
	}
	:global(mark) {
		background-color: rgb(254, 254, 0);
		color: inherit;
		padding: 0;
		font-weight: bold;
	}
</style>
