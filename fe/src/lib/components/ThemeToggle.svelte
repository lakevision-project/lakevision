<script>
	/**
	 * Header control for light / dark / follow-OS.
	 *
	 * Rendered as a HeaderGlobalAction so it sits with the other header icons and
	 * inherits their keyboard and focus behaviour.
	 */
	import { HeaderGlobalAction } from 'carbon-components-svelte';
	import { Asleep, Awake, Screen } from 'carbon-icons-svelte';
	import { resolvedTheme, setTheme, themePreference } from '$lib/theme';

	/** Cycle order for an explicit choice. */
	const ORDER = ['light', 'dark', 'system'];

	const LABEL = {
		light: 'Theme: light (click for dark)',
		dark: 'Theme: dark (click to follow system)',
		system: 'Theme: following system (click for light)'
	};

	$: icon = $themePreference === 'system' ? Screen : $resolvedTheme === 'dark' ? Asleep : Awake;

	function cycle() {
		// From 'system', step to whichever explicit theme is the opposite of what
		// is currently showing, so the first click always visibly changes something.
		if ($themePreference === 'system') {
			setTheme($resolvedTheme === 'dark' ? 'light' : 'dark');
			return;
		}
		const next = ORDER[(ORDER.indexOf($themePreference) + 1) % ORDER.length];
		setTheme(next);
	}
</script>

<HeaderGlobalAction
	iconDescription={LABEL[$themePreference] ?? 'Toggle theme'}
	{icon}
	on:click={cycle}
/>
