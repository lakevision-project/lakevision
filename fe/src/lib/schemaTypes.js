/**
 * Semantic grouping for Iceberg column types.
 *
 * Colour is applied per *family* rather than per type: a catalog surfaces dozens
 * of concrete types (decimal(7,2), decimal(38,0), timestamptz, list<float>, …) and
 * a distinct hue for each would be noise. Five families stay learnable.
 *
 * Names map to Carbon tag colours so both themes are handled by tokens.
 */

/** @typedef {'string' | 'numeric' | 'temporal' | 'boolean' | 'complex' | 'other'} TypeFamily */

/**
 * @param {string | null | undefined} rawType
 * @returns {TypeFamily}
 */
export function typeFamily(rawType) {
	const t = String(rawType ?? '')
		.trim()
		.toLowerCase();
	if (!t) return 'other';

	// Order matters: check container types before their element types.
	if (/^(list|map|struct)\b|[<{]/.test(t)) return 'complex';
	if (/^bool/.test(t)) return 'boolean';
	if (/^(timestamp|timestamptz|date|time)\b/.test(t)) return 'temporal';
	if (/^(int|long|float|double|decimal|short|byte)\b/.test(t)) return 'numeric';
	if (/^(string|uuid|binary|fixed)\b/.test(t)) return 'string';
	return 'other';
}

/**
 * Carbon `Tag` type for a column's data type.
 * @param {string | null | undefined} rawType
 */
export function typeTagColor(rawType) {
	return {
		string: 'blue',
		numeric: 'teal',
		temporal: 'purple',
		boolean: 'green',
		complex: 'magenta',
		other: 'cool-gray'
	}[typeFamily(rawType)];
}

/**
 * Split a property key into a group and the remainder.
 *
 * Iceberg property keys are dotted namespaces (`write.parquet.compression-codec`,
 * `cluster.zcube-list.current`). Grouping on the first segment turns one flat list
 * of unrelated concerns into a few coherent ones, and lets the redundant prefix be
 * dropped from the displayed key.
 *
 * @param {string} key
 * @returns {{ group: string, rest: string }}
 */
export function splitPropertyKey(key) {
	const idx = key.indexOf('.');
	if (idx <= 0) return { group: 'general', rest: key };
	return { group: key.slice(0, idx), rest: key.slice(idx + 1) };
}

/**
 * Group properties by key prefix, largest group first.
 * @param {Record<string, any>} properties
 * @returns {Array<{ group: string, entries: Array<{ key: string, label: string, value: any }> }>}
 */
export function groupProperties(properties) {
	/** @type {Record<string, Array<{key: string, label: string, value: any}>>} */
	const groups = {};
	for (const [key, value] of Object.entries(properties ?? {})) {
		const { group, rest } = splitPropertyKey(key);
		(groups[group] ??= []).push({ key, label: rest, value });
	}
	return Object.entries(groups)
		.map(([group, entries]) => ({
			group,
			entries: entries.sort((a, b) => a.label.localeCompare(b.label))
		}))
		.sort((a, b) => b.entries.length - a.entries.length || a.group.localeCompare(b.group));
}
