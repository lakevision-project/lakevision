import { writable } from 'svelte/store';

export const selectedNamespce   = writable(null);
export const selectedTable      = writable(null);
export const sample_limit       = writable(100);