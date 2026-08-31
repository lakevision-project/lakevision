<script>
	/**
	 * Key/value display for metadata.
	 *
	 * Replaces a fully-bordered table where every cell had equal weight. Keys are
	 * secondary, values primary, and values that are paths/ids/numbers get
	 * monospace with tabular figures so they are actually readable and comparable.
	 */
	import { CopyButton } from 'carbon-components-svelte';

	/** @type {Record<string, any>} */
	export let data = {};
	/** Keys whose values should be monospaced and copyable (paths, ids). */
	/** @type {string[]} */
	export let monospaceKeys = [];
	/** Show a copy button for these keys. */
	/** @type {string[]} */
	export let copyableKeys = [];

	$: entries = Object.entries(data ?? {});

	/** @param {string} key @param {any} value */
	function isMono(key, value) {
		if (monospaceKeys.includes(key)) return true;
		if (typeof value === 'number') return true;
		const s = String(value ?? '');
		// Paths, URIs and long digit strings read far better monospaced.
		return /^(s3|gs|abfss?|file|hdfs):\/\//.test(s) || /^\d{6,}$/.test(s);
	}
</script>

{#if entries.length === 0}
	<p class="empty">No values.</p>
{:else}
	<dl class="kv">
		{#each entries as [key, value] (key)}
			<dt>{key}</dt>
			<dd class:mono={isMono(key, value)}>
				<span class="val"
					>{value === '' || value === null || value === undefined ? '—' : value}</span
				>
				{#if copyableKeys.includes(key) && value}
					<span class="copy">
						<CopyButton text={String(value)} iconDescription="Copy {key}" feedback="Copied" />
					</span>
				{/if}
			</dd>
		{/each}
	</dl>
{/if}

<style>
	.kv {
		display: grid;
		/* Consistent key column, but capped relative to the container so a narrow
		   column does not starve the value: a full 15rem key inside a 520px column
		   left an S3 path only ~200px and wrapped it to three lines. */
		grid-template-columns: minmax(0, min(var(--lv-key-col, 15rem), 38%)) 1fr;
		gap: 0 1.5rem;
		margin: 0;
	}
	dt,
	dd {
		padding: 0.5rem 0;
		border-bottom: 1px solid var(--cds-ui-03, #e0e0e0);
		min-width: 0;
	}
	dt {
		font-size: 0.8125rem;
		color: var(--cds-text-02, #525252);
	}
	dd {
		margin: 0;
		color: var(--cds-text-01, #161616);
		display: flex;
		align-items: flex-start;
		gap: 0.5rem;
		overflow-wrap: anywhere;
	}
	dd.mono .val {
		font-family: var(--cds-code-01-font-family, ui-monospace, SFMono-Regular, Menlo, monospace);
		font-size: 0.8125rem;
		font-variant-numeric: tabular-nums;
	}
	.val {
		min-width: 0;
		flex: 1;
	}
	.copy {
		flex-shrink: 0;
		/* Pull the 32px Carbon button back into the row's line height so it does
		   not add vertical space to every row that has one. .val's flex: 1
		   already holds it against the right edge. */
		margin: -0.375rem 0;
	}
	.empty {
		color: var(--cds-text-02, #525252);
		margin: 0;
		padding: 0.5rem 0;
	}
	@media (max-width: 672px) {
		.kv {
			grid-template-columns: 1fr;
			gap: 0;
		}
		dt {
			border-bottom: none;
			padding-bottom: 0;
		}
	}
</style>
