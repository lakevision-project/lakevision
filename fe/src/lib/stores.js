import { writable } from 'svelte/store';

export const selectedNamespce   = writable(null);
export const selectedTable      = writable(null);
export const sample_limit       = writable(100);

export const healthEnabled = writable(false);
export const HEALTH_DISABLED_MESSAGE = 'Feature is disabled, please contact you app administrator to enable it.';