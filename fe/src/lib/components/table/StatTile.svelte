<script>
	/**
	 * One headline metric.
	 *
	 * The point of these is hierarchy: a table's record count and size are what
	 * people come to the page for, and previously they were rendered in the same
	 * bordered key/value grid as `write.parquet.row-group-size-bytes`, so nothing
	 * read first.
	 */
	/** @type {string} */
	export let label;
	/** Primary value, already formatted for display. */
	/** @type {string | number} */
	export let value;
	/** Optional smaller line beneath (exact count, derived figure, format version). */
	/** @type {string} */
	export let caption = '';
	/** Optional icon component (carbon-icons-svelte). */
	/** @type {any} */
	export let icon = null;
	/** Muted styling for a zero / not-applicable metric. */
	export let muted = false;
</script>

<div class="tile" class:muted>
	<div class="head">
		{#if icon}
			<span class="icon"><svelte:component this={icon} size={16} /></span>
		{/if}
		<span class="label">{label}</span>
	</div>
	<p class="value" title={caption || undefined}>{value}</p>
	{#if caption}
		<p class="caption">{caption}</p>
	{:else}
		<!-- Reserve the line so tiles in a row stay the same height. -->
		<p class="caption" aria-hidden="true">&nbsp;</p>
	{/if}
</div>

<style>
	.tile {
		background-color: var(--cds-layer, #f4f4f4);
		border-left: 3px solid var(--cds-interactive-01, #0f62fe);
		padding: 1rem 1.25rem;
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
		min-width: 0;
	}
	.tile.muted {
		border-left-color: var(--cds-ui-03, #e0e0e0);
	}
	.head {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		color: var(--cds-text-02, #525252);
	}
	.icon {
		display: inline-flex;
		flex-shrink: 0;
	}
	.label {
		font-size: 0.75rem;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	.value {
		margin: 0;
		font-size: 1.75rem;
		line-height: 1.15;
		font-weight: 600;
		color: var(--cds-text-01, #161616);
		/* Tabular figures so digits line up between tiles. */
		font-variant-numeric: tabular-nums;
		overflow-wrap: anywhere;
	}
	.muted .value {
		color: var(--cds-text-02, #525252);
		font-weight: 400;
	}
	.caption {
		margin: 0;
		font-size: 0.75rem;
		color: var(--cds-text-02, #525252);
		font-variant-numeric: tabular-nums;
		overflow-wrap: anywhere;
	}
	@media (max-width: 672px) {
		.value {
			font-size: 1.5rem;
		}
	}
</style>
