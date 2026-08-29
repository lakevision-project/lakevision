<script>
	/**
	 * Uniform loading / error / empty presentation for one table resource.
	 *
	 * Loading states were previously a mix of spinners, skeletons and (in the
	 * Summary tab's schema panel) nothing at all, so panels jumped as data
	 * arrived. A skeleton sized like the real content keeps the layout stable.
	 */
	import { DataTableSkeleton, SkeletonText, ToastNotification } from 'carbon-components-svelte';
	import { ForbiddenError } from '$lib/api/tableResource';

	/** The `{data, loading, error, loaded}` value from a table resource store. */
	export let resource;
	/** Skeleton shape while loading: 'table' or 'text'. */
	export let skeleton = 'table';
	export let rows = 5;
	export let columns = 4;
	/** Shown when the request succeeded but returned nothing. */
	export let emptyMessage = 'No data.';
	/** What failed, used in the error heading. */
	export let label = 'data';

	$: isEmpty =
		resource.loaded &&
		!resource.error &&
		(resource.data == null ||
			(Array.isArray(resource.data) && resource.data.length === 0) ||
			(!Array.isArray(resource.data) &&
				typeof resource.data === 'object' &&
				Object.keys(resource.data).length === 0));
</script>

{#if resource.loading}
	{#if skeleton === 'text'}
		<SkeletonText paragraph lines={rows} />
	{:else}
		<DataTableSkeleton rowCount={rows} columnCount={columns} showHeader={false} showToolbar={false} />
	{/if}
{:else if resource.error instanceof ForbiddenError}
	<ToastNotification
		hideCloseButton
		kind="warning"
		lowContrast
		title="No Access"
		subtitle="You don't have access to the table data"
	/>
{:else if resource.error}
	<ToastNotification
		hideCloseButton
		kind="error"
		lowContrast
		title="Could not load {label}"
		subtitle={resource.error.message}
	/>
{:else if isEmpty}
	<p class="empty">{emptyMessage}</p>
{:else}
	<slot />
{/if}

<style>
	.empty {
		color: var(--cds-text-02, #525252);
		padding: 0.5rem 0;
	}
</style>
