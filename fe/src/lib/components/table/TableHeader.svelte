<script>
	/**
	 * Identity header for the selected table.
	 *
	 * Replaces a definition list that spent two tall rows restating the two values
	 * already visible in the sidebar pickers. This states the table once, as a
	 * heading, with the namespace as breadcrumb context and the actions inline.
	 */
	import { CopyButton, Tag } from 'carbon-components-svelte';

	export let namespace = '';
	export let table = '';
	/** Link to copy for sharing this exact view. */
	export let shareUrl = '';
	export let onCopyRequest = () => {};
	/** Optional short facts shown as tags (format version, partitioned, etc). */
	/** @type {Array<{text: string, type?: string}>} */
	export let tags = [];
</script>

<header class="header">
	<div class="identity">
		<nav class="crumbs" aria-label="Location">
			<span class="crumb">{namespace || '—'}</span>
		</nav>
		<h1 class="name" title={table}>{table || '—'}</h1>
		{#if tags.length}
			<div class="tags">
				{#each tags as tag (tag.text)}
					<Tag type={tag.type ?? 'cool-gray'} size="sm">{tag.text}</Tag>
				{/each}
			</div>
		{/if}
	</div>
	<div class="actions">
		<CopyButton
			text={shareUrl}
			on:click={onCopyRequest}
			iconDescription="Copy link to this table"
			feedback="Link copied"
		/>
	</div>
</header>

<style>
	.header {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 1rem;
		padding: 1.25rem 0 1rem;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		flex-wrap: wrap;
	}
	.identity {
		min-width: 0;
		display: flex;
		flex-direction: column;
		gap: 0.375rem;
	}
	.crumbs {
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
	}
	.name {
		margin: 0;
		font-size: 1.75rem;
		line-height: 1.2;
		font-weight: 400;
		color: var(--cds-text-01, #161616);
		overflow-wrap: anywhere;
	}
	.tags {
		display: flex;
		flex-wrap: wrap;
		gap: 0.25rem;
		margin-top: 0.125rem;
	}
	.actions {
		display: flex;
		align-items: center;
		gap: 0.25rem;
		flex-shrink: 0;
	}
	@media (max-width: 672px) {
		.name {
			font-size: 1.375rem;
		}
	}
</style>
