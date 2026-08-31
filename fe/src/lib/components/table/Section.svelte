<script>
	/**
	 * A titled panel with an optional count badge and collapse control.
	 *
	 * Gives the page a visible structure: previously every heading was a bare <h5>
	 * of equal weight, so Summary, Schema, Properties, Partition Specs and Sort
	 * Order all competed for attention.
	 */
	/** @type {string} */
	export let title;
	/** Item count shown as a badge; omit when not meaningful. */
	/** @type {number | null} */
	export let count = null;
	/** Set false to start collapsed. */
	export let open = true;
	/** Set false for a static panel with no collapse affordance. */
	export let collapsible = true;
	/**
	 * Collapse automatically once the panel is known to be empty.
	 *
	 * An empty Partitioning / Sort order / Properties panel costs as much vertical
	 * space as a populated one while saying only "not partitioned". Callers pass
	 * this as `true` only when the data has actually loaded without error, so a
	 * still-loading or failed panel stays open and its skeleton/error stays
	 * visible.
	 *
	 * Applied once per transition into the empty state rather than reactively, so
	 * a user who expands an empty panel is not immediately re-collapsed.
	 */
	/** @type {boolean} */
	export let collapseWhenEmpty = false;

	let autoCollapsed = false;
	$: if (collapsible && collapseWhenEmpty && !autoCollapsed) {
		open = false;
		autoCollapsed = true;
	} else if (!collapseWhenEmpty && autoCollapsed) {
		// Re-arm, so switching to another empty table collapses again.
		autoCollapsed = false;
	}
</script>

<section class="section">
	<div class="header">
		{#if collapsible}
			<button type="button" class="toggle" aria-expanded={open} on:click={() => (open = !open)}>
				<svg
					class="chev"
					class:closed={!open}
					width="16"
					height="16"
					viewBox="0 0 32 32"
					aria-hidden="true"
				>
					<path fill="currentColor" d="M16 22L6 12l1.4-1.4 8.6 8.6 8.6-8.6L26 12z" />
				</svg>
				<h3>{title}</h3>
				{#if count !== null}<span class="badge">{count}</span>{/if}
			</button>
		{:else}
			<div class="static-header">
				<h3>{title}</h3>
				{#if count !== null}<span class="badge">{count}</span>{/if}
			</div>
		{/if}
		<div class="actions"><slot name="actions" /></div>
	</div>

	{#if open}
		<div class="body"><slot /></div>
	{/if}
</section>

<style>
	.section {
		margin-bottom: 1.75rem;
		min-width: 0;
	}
	.header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		padding-bottom: 0.5rem;
		margin-bottom: 0.875rem;
	}
	.toggle,
	.static-header {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		color: inherit;
		cursor: pointer;
		text-align: left;
		min-width: 0;
	}
	.static-header {
		cursor: default;
	}
	h3 {
		margin: 0;
		font-size: 1rem;
		font-weight: 600;
		color: var(--cds-text-01, #161616);
		white-space: nowrap;
	}
	.badge {
		font-size: 0.75rem;
		padding: 0.0625rem 0.4rem;
		border-radius: 0.75rem;
		background-color: var(--cds-ui-03, #e0e0e0);
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
	}
	.chev {
		flex-shrink: 0;
		transition: transform 0.15s ease;
		color: var(--cds-text-02, #525252);
	}
	.chev.closed {
		transform: rotate(-90deg);
	}
	.toggle:hover h3 {
		color: var(--cds-interactive-01, #0f62fe);
	}
	.toggle:focus-visible {
		outline: 2px solid var(--cds-interactive-01, #0f62fe);
		outline-offset: 2px;
	}
	.actions {
		display: flex;
		align-items: center;
		gap: 0.25rem;
		flex-shrink: 0;
	}
	.body {
		min-width: 0;
		overflow-x: auto;
	}
</style>
