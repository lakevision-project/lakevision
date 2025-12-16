import { writable } from 'svelte/store';

export const selectedNamespce   = writable(null);
export const selectedTable      = writable(null);
export const sample_limit       = writable(100);

export const user = writable(null);

export const healthEnabled = writable(false);
export const HEALTH_DISABLED_MESSAGE = 'Feature is disabled. Please contact your app administrator or <a href="https://github.com/lakevision-project/lakevision?tab=readme-ov-file#configuration" target="_blank">read the documentation</a> to enable DB connection.';