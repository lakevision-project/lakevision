/**
 * Display formatting helpers.
 *
 * The API returns some values pre-formatted by the backend's humanize
 * ("4,807,645,031", "363.0 GB"). These helpers parse those back where a compact
 * form is wanted, and fall through to the original string when a value is not
 * numeric, so an unexpected shape degrades instead of showing NaN.
 */

/**
 * Compact a count: 4807645031 -> "4.81 B".
 *
 * B/M/K rather than SI prefixes: this is a data catalog, and "4.81 B" reads as
 * billions to the people using it.
 *
 * @param {number | string | null | undefined} value
 */
export function compactCount(value) {
	const n = toNumber(value);
	if (n === null) return value == null || value === '' ? '—' : String(value);
	const abs = Math.abs(n);
	if (abs >= 1e12) return `${trim(n / 1e12)} T`;
	if (abs >= 1e9) return `${trim(n / 1e9)} B`;
	if (abs >= 1e6) return `${trim(n / 1e6)} M`;
	if (abs >= 10_000) return `${trim(n / 1e3)} K`;
	return n.toLocaleString();
}

/**
 * Full precision with thousands separators, for the caption under a compact value.
 * @param {number | string | null | undefined} value
 */
export function exactCount(value) {
	const n = toNumber(value);
	if (n === null) return value == null || value === '' ? '' : String(value);
	return n.toLocaleString();
}

/**
 * True when the compact form differs from the exact one, so the caption is only
 * rendered when it adds information.
 * @param {number | string | null | undefined} value
 */
export function isAbbreviated(value) {
	const n = toNumber(value);
	return n !== null && Math.abs(n) >= 10_000;
}

/**
 * Average file size, given a total size and a file count.
 * @param {number | string | null | undefined} totalBytes
 * @param {number | string | null | undefined} fileCount
 */
export function averageFileSize(totalBytes, fileCount) {
	const bytes = toNumber(totalBytes);
	const files = toNumber(fileCount);
	if (bytes === null || !files) return null;
	return formatBytes(bytes / files);
}

/**
 * Bytes to a readable size, mirroring the backend's decimal humanize output.
 * @param {number} bytes
 */
export function formatBytes(bytes) {
	if (!Number.isFinite(bytes)) return '—';
	const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
	let value = bytes;
	let unit = 0;
	while (value >= 1000 && unit < units.length - 1) {
		value /= 1000;
		unit += 1;
	}
	return `${value < 10 && unit > 0 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`;
}

/**
 * Parse a size string the backend already formatted ("363.0 GB") back to bytes,
 * so derived values can be computed client-side.
 * @param {string | number | null | undefined} value
 * @returns {number | null}
 */
export function parseSize(value) {
	if (typeof value === 'number') return value;
	if (typeof value !== 'string') return null;
	const match = value.trim().match(/^([\d.,]+)\s*([kMGTP]?B)$/i);
	if (!match) return null;
	const amount = Number(match[1].replace(/,/g, ''));
	if (!Number.isFinite(amount)) return null;
	const factors = { b: 1, kb: 1e3, mb: 1e6, gb: 1e9, tb: 1e12, pb: 1e15 };
	return amount * (factors[match[2].toLowerCase()] ?? 1);
}

/**
 * Relative time ("3 days ago"). Callers keep the absolute value as a tooltip.
 * @param {string | null | undefined} value
 */
export function relativeTime(value) {
	if (!value) return '';
	// The backend emits "YYYY-MM-DD HH:MM:SS" in UTC with no zone marker.
	const iso = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(value)
		? `${value.replace(' ', 'T')}Z`
		: value;
	const then = new Date(iso).getTime();
	if (!Number.isFinite(then)) return '';
	let amount = Math.round((Date.now() - then) / 1000);
	if (amount < 0) return 'just now';
	if (amount < 45) return 'just now';
	const ladder = [
		['second', 60],
		['minute', 60],
		['hour', 24],
		['day', 30],
		['month', 12]
	];
	let unit = 'second';
	for (const [name, size] of ladder) {
		if (amount < size) {
			unit = name;
			return `${amount} ${unit}${amount === 1 ? '' : 's'} ago`;
		}
		amount = Math.floor(amount / size);
		unit = name;
	}
	return `${amount} year${amount === 1 ? '' : 's'} ago`;
}

/** @param {number} n */
function trim(n) {
	return n >= 100 ? Math.round(n).toString() : n.toFixed(2).replace(/\.?0+$/, '');
}

/**
 * @param {any} value
 * @returns {number | null}
 */
function toNumber(value) {
	if (typeof value === 'number') return Number.isFinite(value) ? value : null;
	if (typeof value !== 'string') return null;
	const cleaned = value.replace(/,/g, '').trim();
	if (cleaned === '') return null;
	const n = Number(cleaned);
	return Number.isFinite(n) ? n : null;
}
