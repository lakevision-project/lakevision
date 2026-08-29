/**
 * Theme selection: light, dark, or follow the OS setting.
 *
 * Carbon ships every theme in `css/all.css`, scoped to `:root[theme=...]`, so
 * switching is a matter of setting that attribute on <html>. Tokens do the rest,
 * which is why component styles must use var(--cds-*) rather than raw colours.
 */
import { writable } from 'svelte/store';
import { browser } from '$app/environment';

const STORAGE_KEY = 'lakevision.theme';

/** @typedef {'light' | 'dark' | 'system'} ThemePreference */

/** Carbon theme name for each resolved mode. */
const CARBON_THEME = { light: 'white', dark: 'g100' };

/** @returns {ThemePreference} */
function readStoredPreference() {
	if (!browser) return 'system';
	try {
		const stored = localStorage.getItem(STORAGE_KEY);
		if (stored === 'light' || stored === 'dark' || stored === 'system') return stored;
	} catch {
		// Private mode or blocked storage: fall back to following the OS.
	}
	return 'system';
}

function prefersDark() {
	return browser && window.matchMedia?.('(prefers-color-scheme: dark)').matches;
}

/** The user's choice: 'light' | 'dark' | 'system'. */
export const themePreference = writable(readStoredPreference());

/** What is actually rendered right now: 'light' | 'dark'. */
export const resolvedTheme = writable('light');

/**
 * @param {ThemePreference} preference
 * @returns {'light' | 'dark'}
 */
function resolve(preference) {
	if (preference === 'system') return prefersDark() ? 'dark' : 'light';
	return preference;
}

/** Apply a preference to the document and remember it. */
function apply(preference) {
	const mode = resolve(preference);
	resolvedTheme.set(mode);
	if (!browser) return;
	document.documentElement.setAttribute('theme', CARBON_THEME[mode]);
	// Lets plain CSS and form controls follow along without extra rules.
	document.documentElement.style.colorScheme = mode;
}

/** @param {ThemePreference} preference */
export function setTheme(preference) {
	themePreference.set(preference);
	if (browser) {
		try {
			localStorage.setItem(STORAGE_KEY, preference);
		} catch {
			// Not persisting is acceptable; the session still switches.
		}
	}
	apply(preference);
}

/**
 * Wire up the theme on the client. Returns a cleanup function so the OS
 * listener is removed on destroy rather than leaking.
 */
export function initTheme() {
	if (!browser) return () => {};
	let current = readStoredPreference();
	themePreference.set(current);
	apply(current);

	const media = window.matchMedia('(prefers-color-scheme: dark)');
	const onChange = () => {
		if (current === 'system') apply('system');
	};
	media.addEventListener('change', onChange);

	const unsubscribe = themePreference.subscribe((value) => {
		current = value;
	});

	return () => {
		media.removeEventListener('change', onChange);
		unsubscribe();
	};
}
