<script>
	// --- MOVED FROM LAYOUT.SVELTE ---
	import { browser } from '$app/environment';
	import { afterNavigate } from '$app/navigation';
	import {
		ComboBox,
		SideNav,
		SideNavItems,
		SideNavLink,
		Modal,
		InlineLoading,
		Search
	} from 'carbon-components-svelte';
	import { selectedNamespce, selectedTable, sample_limit } from '$lib/stores';
	import { page } from '$app/stores';
	import { Renew, FilterRemove, Information } from 'carbon-icons-svelte';
	import { healthEnabled, HEALTH_DISABLED_MESSAGE } from '$lib/stores';
	let namespace;
	let table;
	$: tableKey = namespace && table ? `${namespace}.${table}` : '';
	let dropdown1_selectedId = '';
	let dropdown2_selectedId = '';
	let navpop = false;
	let tabpop = false;
	let nav_loading = false;
	// Extract query parameters
	$: q_ns = $page.url.searchParams.get('namespace') ?? '';
	$: q_tab = $page.url.searchParams.get('table') ?? '';
	$: q_sample_limit_raw = $page.url.searchParams.get('sample_limit');
	$: q_sample_limit = q_sample_limit_raw && !Number.isNaN(+q_sample_limit_raw)
	  ? +q_sample_limit_raw
	  : null;

	let initializedFromQuery = false;

	function clearQueryParamsOnce() {
		if (!browser) return;
		const sp = new URLSearchParams($page.url.searchParams);
		sp.delete('namespace');
		sp.delete('table');
		sp.delete('sample_limit');
		const qs = sp.toString();
		if (qs !== $page.url.searchParams.toString()) {
			goto(qs ? `?${qs}` : $page.url.pathname, { replaceState: true, noScroll: true });
		}
	}

	async function initFromQuery() {
		if (initializedFromQuery) return;
		if (!q_ns) return; // need at least the namespace

		// sample limit
		if (q_sample_limit !== null) {
		sample_limit.set(q_sample_limit);
		}

		// select namespace and load its tables
		setNamespace(q_ns);
		await get_tables(q_ns);

		// then select the table (if provided)
		if (q_tab) setTable(q_tab);

		initializedFromQuery = true;
		// Clear params so the URL doesn’t look “stuck”
		clearQueryParamsOnce();
		}


	afterNavigate(() => {
		// Handles all navigations, including first load and param changes
		if ($page.url.searchParams.has('namespace')) {
			// If the URL has params, try to initialize.
			// The initializedFromQuery flag will prevent re-running if it just ran.
			initFromQuery();
		} else {
			// If the URL *doesn't* have params (e.g., after clearQueryParamsOnce()
			// or navigating to '/'), reset the flag so the *next*
			// param navigation will work.
			initializedFromQuery = false;
		}
	});

	export let data;
	let isSideNavOpen = true;
	/**
	 * @type {never[]}
	 */
	let tables = [];
	let all_tables = [];
	let loading = false;
	const tableLoadedEvent = new EventTarget();
	let namespaces = data.namespaces;
	let searchNamespaceQuery = '';
	let searchTableQuery = '';
	let showRulesInfoModal = false;
	let cc = 0;
	/**
	 * @param {null} namespace
	 */
	async function get_tables(namespace) {
		loading = true;
		try {
			if (!namespace) {
				const res = await fetch(`/api/tables`);
				if (res.ok) {
					all_tables = await res.json();
				} else {
					console.error('Failed to fetch data:', res.statusText);
				}
				loading = false;
				return;
			} else {
				selectedNamespce.set(namespace);
				dropdown2_selectedId = '';
				const res = await fetch(`/api/tables?namespace=${namespace}`);
				if (res.ok) {
					tables = await res.json();
				} else if (res.status === 401) {
					// Session expired
					//CustomEvent eve = new CustomEvent('session-expired');
					//window.dispatchEvent();
					// handleLogout(); // This function is in layout, might need event dispatching
				} else {
					console.error('Failed to fetch data:', res.statusText);
				}
			}
		} finally {
			loading = false; // Stop loading indicator
			tableLoadedEvent.dispatchEvent(new Event('tablesLoaded'));
		}
	}

	async function refreshNamespaces() {
		nav_loading = true;
		try {
			const res = await fetch('/api/namespaces?refresh=true');
			if (res.ok) {
				const data = await res.json();
				namespaces = data;
			} else if (res.status === 401) {
				// handleLogout();
			}
		} finally {
			nav_loading = false;
		}
	}

	async function refreshTables() {
		nav_loading = true;
		try {
			const res = await fetch('/api/tables?refresh=true');
			if (res.ok) {
				const data = await res.json();
				all_tables = data;
			} else if (res.status === 401) {
				// handleLogout();
			}
		} finally {
			nav_loading = false;
		}
	}

	$: filteredNamespaces = namespaces.filter((ns) =>
		ns.text.toLowerCase().includes(searchNamespaceQuery.toLowerCase())
	);

	$: filteredTables = ((namespace == '' || !namespace) ? all_tables : tables).filter((tb) =>
		tb.text.toLowerCase().includes(searchTableQuery.toLowerCase())
	);

	function waitForTables() {
		return new Promise((resolve) => {
			tableLoadedEvent.addEventListener('tablesLoaded', () => {
				resolve(tables); // Resolve when the event is triggered
			});
		});
	}

	function shouldFilterItem(item, value) {
		if (!value) return true;
		return item.text.toLowerCase().includes(value.toLowerCase());
	}

	const formatSelected = (id, items) => items.find((item) => item.id === id)?.text ?? '';

	function findItemIdByText(items, text) {
		const needle = (text ?? '').toLowerCase();
		const item = items.find((it) => (it.text ?? '').toLowerCase() === needle);
		return item ? item.id : null;
	}

	function resetQueryParams() {
		const url = window.location.origin + window.location.pathname;
		window.history.replaceState(null, '', url);
	}

	$: {
    // This reactive block handles changes triggered by the Namespace ComboBox selection
		if (browser) {
			const currentSelectedId = dropdown1_selectedId; // Capture value at start of block run
			const nsText = formatSelected(currentSelectedId, data.namespaces);
			const currentStoreValue = get(selectedNamespce);

			if (nsText) {
				// A namespace IS selected in the dropdown
				if (nsText !== currentStoreValue) {
					// And it's DIFFERENT from the currently active namespace store
					// console.log(`NS Dropdown changed to: ${nsText}`);
					get_tables(nsText); // Fetch tables for the new namespace
									// get_tables updates selectedNamespce store and clears table dropdown ID
				}
			} else {
				// Namespace selection IS CLEARED in the dropdown (selectedId is not valid)
				if (currentStoreValue !== '') {
					// Only act if the store wasn't already empty (avoids redundant updates)
					// console.log('NS Dropdown cleared');
					selectedNamespce.set('');
					selectedTable.set('');
					tables = []; // *** Explicitly clear the tables array for the ComboBox ***
					dropdown2_selectedId = ''; // Clear the table dropdown's selected ID
				}
			}
		}
	}

	$: if (browser) {
		selectedTable.set(formatSelected(dropdown2_selectedId, tables));
	}

	function setNamespace(nsp) {
		dropdown2_selectedId = '';
		const id = findItemIdByText(data.namespaces, nsp);
		dropdown1_selectedId = id;
		navpop = false;
	}

	function setTableDynamic(table) {
		waitForTables().then((result) => {
			const id = findItemIdByText(tables, table);
			dropdown2_selectedId = id;
			tabpop = false;
		});
	}

	function setTable(table) {
		const id = findItemIdByText(tables, table);
		dropdown2_selectedId = id;
		tabpop = false;
	}

	function setNamespaceAndTable(nsp, tab) {
		if (namespace == nsp) {
			setTable(tab);
		} else {
			setNamespace(nsp);
			setTableDynamic(tab);
		}
	}

	// Group tables by namespace
	function groupByNamespace(tables) {
		return tables.reduce((acc, tab) => {
			(acc[tab.namespace] = acc[tab.namespace] || []).push(tab);
			return acc;
		}, {});
	}

	function resetNsAndTableSelection() {
		dropdown1_selectedId = '';
		get_tables('');
	}

	// --- ORIGINAL PAGE SCRIPT ---
	import {
		Tile,
		ExpandableTile,
		Dropdown,
		Content,
		Tabs,
		Tab,
		TabContent,
		Grid,
		Row,
		Column,
		CopyButton,
		ToastNotification,
		DataTableSkeleton,
		Loading,
		Button,
		ButtonSet,
		TextInput,
		Checkbox,
		FormGroup,
		Select,
		SelectItem,
		Tag,
		Toggle
	} from 'carbon-components-svelte';
	import {
		Run,
		Calendar,
		// Renew, // Already imported
		CheckmarkFilled,
		WarningAltFilled,
		Edit,
		TrashCan
	} from 'carbon-icons-svelte';
	// import { selectedNamespce, selectedTable, sample_limit } from '$lib/stores'; // Already imported
	import JsonTable from '../lib/components/JsonTable.svelte';
	import { BarChartSimple } from '@carbon/charts-svelte';
	import '@carbon/charts-svelte/styles.css';
	import options from './options';
	import VirtualTable from '../lib/components/VirtTable3.svelte';
	import { goto } from '$app/navigation';
	import QueryRunner from '../lib/components/QueryRunner.svelte';
	import { get } from 'svelte/store';
	import { onMount } from 'svelte';

	let ns_props;
	let tab_props;
	let error = '';
	let url = '';
	let pageSessionId = Date.now().toString(36) + Math.random().toString(36).substring(2);

	// --- Toast Notification State ---
	let toastProps = {
		open: false,
		kind: 'info',
		title: '',
		subtitle: ''
	};
	let toastTimeout;

	function showToast(kind, title, subtitle, timeout = 4000) {
		toastProps = { open: true, kind, title, subtitle };
		if (toastTimeout) clearTimeout(toastTimeout);
		toastTimeout = setTimeout(() => {
			toastProps.open = false;
		}, timeout);
	}

	$: {
		selectedNamespce.subscribe((value) => {
			namespace = value;
		});
		selectedTable.subscribe((value) => {
			table = value;
		});
		if (namespace) ns_props = get_namespace_special_properties(namespace);
		if (namespace && table) tab_props = get_table_special_properties(namespace + '.' + table);
	}

	let partitions = [];
	let partitions_loading = false;
	let snapshots = [];
	let snapshots_loading = false;
	let sample_data = [];
	let sample_data_loading = false;
	let sample_data_fetched = false;
	let schema = [];
	let schema_loading = false;
	let summary = [];
	let summary_loading = false;
	let partition_specs = [];
	let partition_specs_loading = false;
	let sort_order = [];
	let sort_order_loading = false;
	let properties = [];
	let properties_loading = false;
	let data_change = [];
	let data_change_loading = false;
	let access_allowed = true;

	// --- STATE & LOGIC FOR INSIGHTS TAB ---

	let insights_loading = true;
	let insightsLoaded = false;
	let insightRuns = [];
	let allRules = [];
	let ruleIdToNameMap = new Map();
	let openRunModal = false;
	let openScheduleModal = false;
	let expandedRules = {};
	let insightsSubTab = 0;

	let runningJobs = [];
	let runningJobsLoading = true;
	const runningJobsColumns = { 'Job ID': '', Status: '', Details: '', 'Started At': '' };
	let runningJobsColumnWidths = { 'Job ID': 300, Status: 120, Details: 400, 'Started At': 220 };

	const virtualTableColumns = { 'Job Type': '', Timestamp: '', 'Rules & Results': '' };
	let columnWidths = { 'Job Type': 150, Timestamp: 250, 'Rules & Results': 600 };

	let manualRunData = { namespace: '', tableName: '', rules_requested: [] };
	let manualRunSelectAll = false;
	let manualRunIndeterminate = false;
	let scheduleRunData = {
		namespace: '',
		tableName: '',
		rules_requested: [],
		cron_schedule: '0 0 * * 0',
		frequency: 'weekly',
		created_by: 'testuser'
	};
	let scheduleRunSelectAll = false;
	let scheduleRunIndeterminate = false;

	// --- Scheduled Runs Management ---
	let showEditScheduleModal = false;
	let showDeleteConfirmModal = false;
	let scheduledJobs = [];
	let schedulesLoading = true;
	let schedulesLoaded = false; // New flag
	let scheduleToEdit = null;
	let scheduleToDelete = null;

	const scheduledJobsColumns = {
		Rules: '',
		Schedule: '',
		Enabled: '',
		'Next Run': '',
		'Last Run': '',
		Actions: ''
	};
	let scheduledJobsColumnWidths = {
		Rules: 350,
		Schedule: 120,
		Enabled: 120,
		'Next Run': 220,
		'Last Run': 220,
		Actions: 100
	};

	async function fetchSchedules() {
		if (!namespace || !table) return;
		schedulesLoading = true;
		try {
			const response = await fetch(`/api/schedules?namespace=${namespace}&table_name=${table}`);
			if (!response.ok) throw new Error('Failed to fetch schedules');
			scheduledJobs = await response.json();
		} catch (error) {
			console.error('Error fetching schedules:', error);
			showToast('error', 'Error Fetching Schedules', error.message);
		} finally {
			schedulesLoading = false;
			schedulesLoaded = true; // Mark as loaded
		}
	}

	async function handleUpdateSchedule() {
		if (!scheduleToEdit) return;
		try {
			const response = await fetch(`/api/schedules/${scheduleToEdit.id}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					rules_requested: scheduleToEdit.rules_requested,
					cron_schedule: scheduleToEdit.cron_schedule,
					is_enabled: scheduleToEdit.is_enabled
				})
			});
			if (!response.ok) {
				const error = await response.json();
				throw new Error(error.detail || 'Failed to update schedule');
			}
			showToast('success', 'Success', 'Schedule updated successfully!');
			showEditScheduleModal = false;
			await fetchSchedules();
		} catch (error) {
			console.error('Error updating schedule:', error);
			showToast('error', 'Update Failed', error.message);
		}
	}

	async function handleDeleteSchedule() {
		if (!scheduleToDelete) return;
		try {
			const response = await fetch(`/api/schedules/${scheduleToDelete.id}`, { method: 'DELETE' });
			if (response.status !== 204) {
				throw new Error('Failed to delete schedule');
			}
			showToast('success', 'Success', 'Schedule deleted successfully!');
			showDeleteConfirmModal = false;
			await fetchSchedules();
		} catch (error) {
			console.error('Error deleting schedule:', error);
			showToast('error', 'Delete Failed', error.message);
		}
	}

	function openEditModal(schedule) {
		scheduleToEdit = JSON.parse(JSON.stringify(schedule));
		showEditScheduleModal = true;
	}

	function openDeleteModal(schedule) {
		scheduleToDelete = schedule;
		showDeleteConfirmModal = true;
	}

	$: if (namespace && table) {
		manualRunData.namespace = namespace;
		manualRunData.tableName = table;
		scheduleRunData.namespace = namespace;
		scheduleRunData.tableName = table;
	}

	$: {
		if (allRules.length > 0) {
			const allSelectedManual = manualRunData.rules_requested.length === allRules.length;
			const someSelectedManual = manualRunData.rules_requested.length > 0 && !allSelectedManual;
			manualRunSelectAll = allSelectedManual;
			manualRunIndeterminate = someSelectedManual;
			const allSelectedSchedule = scheduleRunData.rules_requested.length === allRules.length;
			const someSelectedSchedule =
				scheduleRunData.rules_requested.length > 0 && !allSelectedSchedule;
			scheduleRunSelectAll = allSelectedSchedule;
			scheduleRunIndeterminate = someSelectedSchedule;
		}
	}

	function toggleSelectAllManual(event) {
		if (event.currentTarget.checked) {
			manualRunData.rules_requested = allRules.map((rule) => rule.id);
		} else {
			manualRunData.rules_requested = [];
		}
	}
	function toggleSelectAllSchedule(event) {
		if (event.currentTarget.checked) {
			scheduleRunData.rules_requested = allRules.map((rule) => rule.id);
		} else {
			scheduleRunData.rules_requested = [];
		}
	}

	function handleFrequencyChange() {
		const freq = scheduleRunData.frequency;
		switch (freq) {
			case 'weekly':
				scheduleRunData.cron_schedule = '0 0 * * 0';
				break;
			case 'biweekly':
				scheduleRunData.cron_schedule = '0 0 1,15 * *';
				break;
			case 'monthly':
				scheduleRunData.cron_schedule = '0 0 1 * *';
				break;
			case 'bimonthly':
				scheduleRunData.cron_schedule = '0 0 1 */2 *';
				break;
		}
	}

    function displayScheduleFrequency(freq) {
		switch (freq) {
			case '0 0 * * 0':
				return 'weekly';
			case '0 0 1,15 * *':
				return 'biweekly';
			case '0 0 1 * *':
				return 'monthly';
			case '0 0 1 */2 *':
				return 'bimonthly';
		}
        return 'custom';
	}

	async function fetchTableInsights() {
		if (!namespace || !table) return;
		insights_loading = true;
		try {
			const response = await fetch(`/api/namespaces/${namespace}/${table}/insights?size=100`);
			if (!response.ok) throw new Error('Failed to fetch jobs');

			const data = await response.json();
			data.sort((a, b) => new Date(b.run_timestamp) - new Date(a.run_timestamp));

			const newExpandedState = {};
			data.forEach((run) => {
				run.results.forEach((result) => {
					const key = `${run.id}-${result.code}`;
					newExpandedState[key] = true;
				});
			});
			expandedRules = newExpandedState;
			insightRuns = data;
		} catch (error) {
			console.error('Error fetching table jobs:', error);
			showToast('error', 'Refresh Failed', 'Could not refresh jobs data.');
		} finally {
			insights_loading = false;
			insightsLoaded = true;
		}
	}

	async function fetchRunningJobs() {
		if (!namespace || !table) return;
		runningJobsLoading = true;
		try {
			const response = await fetch(`/api/jobs/running?namespace=${namespace}&table_name=${table}`);
			if (!response.ok) throw new Error('Failed to fetch running jobs');
			const data = await response.json();
			runningJobs = data.map((job) => ({
				id: job.run_id,
				'Job ID': job.run_id,
				Status: job.status,
				Details: job.details,
				'Started At': job.started_at
			}));
		} catch (error) {
			console.error('Error fetching running jobs:', error);
			runningJobs = [];
		} finally {
			runningJobsLoading = false;
		}
	}

	async function fetchAllRules() {
		try {
			const response = await fetch('/api/lakehouse/insights/rules');
			if (!response.ok) throw new Error('Failed to fetch rules');
			allRules = await response.json();
			ruleIdToNameMap = new Map(allRules.map((rule) => [rule.id, rule.name]));
		} catch (error) {
			console.error('Error fetching rules:', error);
		}
	}

	async function handleManualRunSubmit() {
		if (manualRunData.rules_requested.length === 0) {
			showToast('warning', 'Validation Error', 'Please select at least one rule to check.');
			return;
		}
		try {
			const response = await fetch('/api/start-run', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					namespace: manualRunData.namespace,
					table_name: manualRunData.tableName,
					rules_requested: manualRunData.rules_requested
				})
			});
			if (response.status !== 202) throw new Error('Failed to start job');
			const result = await response.json();
			showToast('success', 'Job Started', `Job ID: ${result.run_id}`);
			openRunModal = false;
			setTimeout(() => {
				fetchRunningJobs();
				fetchTableInsights();
			}, 3000);
		} catch (error) {
			console.error('Error starting manual job:', error);
			showToast('error', 'Error Starting Job', error.message);
		}
	}

	async function handleScheduleSubmit() {
		if (scheduleRunData.rules_requested.length === 0) {
			showToast('warning', 'Validation Error', 'Please select at least one rule for the schedule.');
			return;
		}
		try {
			const response = await fetch('/api/schedules', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					namespace: scheduleRunData.namespace,
					table_name: scheduleRunData.tableName || null,
					rules_requested: scheduleRunData.rules_requested,
					cron_schedule: scheduleRunData.cron_schedule,
					created_by: scheduleRunData.created_by
				})
			});
			if (response.status !== 201) throw new Error('Failed to create schedule');
			const result = await response.json();
			showToast('success', 'Schedule Created', `Schedule ID: ${result.id}`);
			openScheduleModal = false;
		} catch (error) {
			console.error('Error creating schedule:', error);
			showToast('error', 'Error Creating Schedule', error.message);
		}
	}

	let lastSampleLimit = null;
	let sampleLimits = [
		{ id: 10, text: '10' },
		{ id: 50, text: '50' },
		{ id: 100, text: '100' },
		{ id: 500, text: '500' },
		{ id: 1000, text: '1000' },
		{ id: 10000, text: '10000' }
	];
	if (!sampleLimits.find((item) => item.id === get(sample_limit))) {
		sampleLimits.push({ id: get(sample_limit), text: get(sample_limit).toString() });
	}
	sampleLimits = sampleLimits.sort((a, b) => {
		if (a.id < b.id) return -1;
		if (a.id > b.id) return 1;
		return 0;
	});
	let selectedLimit = sampleLimits.find((item) => item.id === get(sample_limit));
	sample_limit.set(selectedLimit.id);
	selectedLimit = sampleLimits.find((item) => item.id === selectedLimit.id);
	$: if (selectedLimit) {
		sample_limit.set(selectedLimit.id);
	}
	async function get_namespace_special_properties(namespace_name) {
		ns_props = await fetch(`/api/namespaces/${namespace_name}/special-properties`, {
			headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
		}).then((res) => res.json());
		return ns_props;
	}
	async function get_table_special_properties(table_id) {
		tab_props = await fetch(`/api/tables/${table_id}/special-properties`, {
			headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
		}).then((res) => res.json());
		return tab_props;
	}
	async function get_data(table_id, feature) {
		let loading = true;
		if (!table_id || table_id == null || table_id == '.' || !table) {
			loading = false;
			return;
		}
		try {
			const res = await fetch(`/api/tables/${table_id}/${feature}`, {
				headers: { 'Content-Type': 'application/json', 'X-Page-Session-ID': pageSessionId }
			});
			const statusCode = res.status;
			if (res.ok) {
				const data = await res.json();
				return data;
			} else if (statusCode == 403) {
				console.log('No Access');
				error = 'No Access';
				access_allowed = false;
				return error;
			} else if (res.status === 401) {
				goto(
					'/api/login?namespace=' + namespace + '&table=' + table + '&sample_limit=' + $sample_limit
				);
			} else {
				console.error('Failed to fetch data:', res.statusText);
				error = res.statusText;
			}
		} finally {
			loading = false;
		}
	}
	let selected = 0;
	$: reset(table);
	let summaryReq = 0;
	let propertiesReq = 0;
	let schemaReq = 0;
	let partitionSpecsReq = 0;
	let sortOrderReq = 0;
	let partitionsReq = 0;
	let snapshotsReq = 0;
	let sampleReq = 0;
	$: tableKey = namespace && table ? `${namespace}.${table}` : '';
	async function fetchSampleData() {
		const myReq = ++sampleReq;
		sample_data_loading = true;
		try {
			const data = await get_data(tableKey, `sample?sample_limit=${$sample_limit}`);
			if (myReq === sampleReq) {
				sample_data = data;
				lastSampleLimit = $sample_limit;
				sample_data_fetched = true;
			}
		} catch (err) {
			if (myReq === sampleReq) {
				error = err.message;
			}
		} finally {
			if (myReq === sampleReq) {
				sample_data_loading = false;
			}
		}
	}
	$: if (selected === 3 && tableKey && !sample_data_loading && (!sample_data_fetched || $sample_limit !== lastSampleLimit)) {
		fetchSampleData();
	}
	$: if (tableKey && selected == 0) {
		const myReq = ++summaryReq;
		summary_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'summary');
				if (myReq === summaryReq) {
					summary = data;
					if (tab_props && 'restricted' in tab_props) {
						summary['Restricted'] = tab_props['restricted'];
					}
				}
			} catch (err) {
				if (myReq === summaryReq) {
					error = err.message;
				}
			} finally {
				if (myReq === summaryReq) {
					summary_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 0) {
		const myReq = ++propertiesReq;
		properties_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'properties');
				if (myReq === propertiesReq) {
					properties = data;
				}
			} catch (err) {
				if (myReq === propertiesReq) {
					error = err.message;
				}
			} finally {
				if (myReq === propertiesReq) {
					properties_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 0) {
		const myReq = ++schemaReq;
		schema_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'schema');
				if (myReq === schemaReq) {
					schema = data;
				}
			} catch (err) {
				if (myReq === schemaReq) {
					error = err.message;
				}
			} finally {
				if (myReq === schemaReq) {
					schema_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 0) {
		const myReq = ++partitionSpecsReq;
		partition_specs_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'partition-specs');
				if (myReq === partitionSpecsReq) {
					partition_specs = data;
				}
			} catch (err) {
				if (myReq === partitionSpecsReq) {
					error = err.message;
				}
			} finally {
				if (myReq === partitionSpecsReq) {
					partition_specs_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 0) {
		const myReq = ++sortOrderReq;
		sort_order_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'sort-order');
				if (myReq === sortOrderReq) {
					sort_order = data;
				}
			} catch (err) {
				if (myReq === sortOrderReq) {
					error = err.message;
				}
			} finally {
				if (myReq === sortOrderReq) {
					sort_order_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 1) {
		const myReq = ++partitionsReq;
		partitions_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'partitions');
				if (myReq === partitionsReq) {
					partitions = data;
				}
			} catch (err) {
				if (myReq === partitionsReq) {
					error = err.message;
				}
			} finally {
				if (myReq === partitionsReq) {
					partitions_loading = false;
				}
			}
		})();
	}
	$: if (tableKey && selected == 2) {
		const myReq = ++snapshotsReq;
		snapshots_loading = true;
		(async () => {
			try {
				const data = await get_data(tableKey, 'snapshots');
				if (myReq === snapshotsReq) {
					snapshots = data;
				}
			} catch (err) {
				if (myReq === snapshotsReq) {
					error = err.message;
				}
			} finally {
				if (myReq === snapshotsReq) {
					snapshots_loading = false;
				}
			}
		})();
	}

	$: {
		if (selected === 5 && table && $healthEnabled) {
			if (!insightsLoaded) {
				fetchRunningJobs();
				fetchTableInsights();
				if (allRules.length === 0) {
					fetchAllRules();
				}
			}
		}
	}

	$: if (insightsSubTab === 2 && !schedulesLoaded && $healthEnabled) {
		fetchSchedules();
	}

	function set_copy_url() {
		url = window.location.origin;
		url = url + '/?namespace=' + namespace + '&table=' + table + '&sample_limit=' + $sample_limit;
	}
	function reset(table) {
		sample_data_fetched = false;
		lastSampleLimit = null;
		partitions = [];
		snapshots = [];
		sample_data = [];
		data_change = [];
		insightRuns = [];
		allRules = [];
		insightsLoaded = false;
		runningJobs = [];
		scheduledJobs = [];
		schedulesLoaded = false;
		partitions_loading = false;
		sort_order_loading = false;
		snapshots_loading = false;
		sample_data_loading = false;
		data_change_loading = false;
		properties_loading = false;
		partition_specs_loading = false;
		schema_loading = false;
		summary_loading = false;
		schema = [];
		summary = [];
		partition_specs = [];
		properties = [];
		sort_order = [];
	}
</script>

<SideNav bind:isOpen="{isSideNavOpen}">
	<SideNavItems>
		<br />
		<ComboBox
			titleText="Namespace"
			items="{namespaces}"
			bind:selectedId="{dropdown1_selectedId}"
			{shouldFilterItem}
			let:item
		>
			<div>
				<strong>{item.text}</strong>
			</div>
		</ComboBox>
		<br />
		<SideNavLink on:click="{() => (navpop = true)}">Show All Namespaces</SideNavLink>

		<br /> <br /><br /> <br /><br /> <br />
		<ComboBox
			titleText="Table"
			disabled="{loading}"
			items="{tables}"
			bind:selectedId="{dropdown2_selectedId}"
			{shouldFilterItem}
			let:item
		>
			<div>
				<strong>{item.text}</strong>
			</div>
		</ComboBox>
		<br />
		<SideNavLink on:click="{() => {
			get_tables('');
			tabpop = true;
		}}">Show All Tables</SideNavLink>
	</SideNavItems>
</SideNav>
{#if navpop}
	<Modal size="sm" passiveModal bind:open="{navpop}" modalHeading="Namespaces" on:open on:close>
		<div class="renew">
			<Search
				on:expand
				on:collapse
				bind:value="{searchNamespaceQuery}"
				placeholder="Search namespaces..."
				class="search-box"
			/>
		</div>
		<div class="renew">
			{#if nav_loading}
				<div class="loading-container">
					<InlineLoading description="Refreshing..." />
				</div>
			{:else}
				<Button
					iconDescription="Refresh namespaces"
					icon="{Renew}"
					size="sm"
					on:click="{refreshNamespaces}"
				/>
			{/if}
		</div>
		<div class="table-container">
			<table>
				{#each filteredNamespaces as ns}
					<tr>
						<td>{ns.id}</td> <td><div
								role="button"
								tabindex="0"
								on:keypress="{(e) => {
									if (e.key === 'Enter' || e.key === ' ') setNamespace(ns.text);
								}}"
								on:click="{() => setNamespace(ns.text)}"
								><a href="{'#'}"> {ns.text}</a></div
							></td>
					</tr>
				{/each}
			</table>
		</div>
	</Modal>
{/if}
{#if tabpop}
	<Modal
		size="sm"
		passiveModal
		bind:open="{tabpop}"
		modalHeading="{'Tables in: ' + namespace}"
		on:open
		on:close
	>
		<div class="renew">
			<Search
				on:expand
				on:collapse
				bind:value="{searchTableQuery}"
				placeholder="Search tables..."
				class="search-box"
			/>
		</div>
		<div class="renew">
			{#if nav_loading}
				<div class="loading-container">
					<InlineLoading description="Refreshing..." />
				</div>
			{:else}
				{#if namespace}
					<Button
						iconDescription="Clear filter"
						icon="{FilterRemove}"
						size="sm"
						on:click="{() => {
							resetNsAndTableSelection();
						}}"
					/>
				{:else}
					<Button
						iconDescription="Refresh tables"
						icon="{Renew}"
						size="sm"
						on:click="{refreshTables}"
					/>
				{/if}
			{/if}
		</div>
		<div class="table-container">
			<table>
				{#each Object.entries(groupByNamespace(filteredTables)) as [namespace, rows]}
					<thead>
						<tr>
							<th colspan="2">{namespace}</th>
						</tr>
					</thead>
					<tbody>
						{#each rows as tabs}
							<tr>
								<td>{tabs.id}</td>
								<td><div
										role="button"
										tabindex="0"
										on:keypress="{(e) => {
											if (e.key === 'Enter' || e.key === ' ') {
												setNamespaceAndTable(tabs.namespace, tabs.text);
											}
										}}"
										on:click="{() => {
											setNamespaceAndTable(tabs.namespace, tabs.text);
										}}"
									>
										<a href="{'#'}">{tabs.text}</a></div
									></td>
							</tr>
						{/each}
					</tbody>
				{/each}
			</table>
			{#if loading}
				<div class="loading-container">
					<InlineLoading description="Loading..." />
				</div>
			{:else if filteredTables.length == 0}
				No data
			{/if}
		</div>
	</Modal>
{/if}

<Content>
	{#if toastProps.open}
		<ToastNotification
			kind="{toastProps.kind}"
			title="{toastProps.title}"
			subtitle="{toastProps.subtitle}"
			caption="{new Date().toLocaleString()}"
			timeout="{0}"
			on:close="{() => (toastProps.open = false)}"
			style="position: fixed; top: 10%; left: 50%; transform: translate(-50%, -50%); z-index: 9999; min-width: 300px;"
		/>
	{/if}
	<Tile>
		<div class="tile-header">
			<div class="tile-content">
				<dl class="namespace-table-list">
					<dt>Namespace</dt>
					<dd>{namespace}</dd>
					<dt>Table</dt>
					<dd>{table}</dd>
				</dl>
			</div>
			<div class="copy-button-container">
				<CopyButton
					text="{url}"
					on:click="{set_copy_url}"
					iconDescription="Copy table link"
					feedback="Table link copied"
				/>
			</div>
		</div>
	</Tile>
	<br />
	<!-- svelte-ignore a11y-click-events-have-key-events -->
	<Tabs bind:selected>
		<Tab label="Summary" />
		<Tab label="Partitions" />
		<Tab label="Snapshots" />
		<Tab label="Sample Data" />
		<Tab label="SQL" />
		<Tab label="Health Check" />

		<svelte:fragment slot="content">
			<TabContent><br />
				<Grid>
					<Row>
						<Column aspectRatio="2x1">
							<h5>Summary</h5>
							{#if !summary_loading && properties}
								<JsonTable jsonData="{summary}" orient="kv" />
							{:else}
								<Loading withOverlay="{false}" small />
							{/if}
						</Column>
						<Column aspectRatio="2x1">
							<h5>Schema</h5>
							{#if !schema_loading && schema.length > 0}
								<VirtualTable
									data="{schema}"
									columns="{schema[0]}"
									rowHeight="{37}"
									tableHeight="{360}"
									defaultColumnWidth="{121}"
								/>
							{/if}
						</Column>
					</Row>
					<Row>
						<Column aspectRatio="2x1">
							<br /><br />
							<h5>Properties</h5>
							{#if !properties_loading && properties}
								<JsonTable jsonData="{properties}" orient="kv" />
							{/if}
						</Column>
						<Column aspectRatio="2x1">
							<br /><br />
							<h5>Partition Specs</h5>
							{#if !partition_specs_loading && partition_specs}
								<JsonTable jsonData="{partition_specs}" orient="table" />
							{/if}
							<br />
							<h5>Sort Order</h5>
							{#if !sort_order_loading && sort_order}
								<JsonTable jsonData="{sort_order}" orient="table" />
							{/if}
						</Column>
					</Row>
				</Grid>
				{#if ns_props}
					<ExpandableTile light>
						<div slot="below">{ns_props}</div>
					</ExpandableTile>
				{/if}
			</TabContent>
			<TabContent>
				<br />
				{#if partitions_loading}
					<Loading withOverlay="{false}" small />
				{:else if !access_allowed}
					<ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data">
					</ToastNotification>
				{:else if partitions.length > 0}
					<VirtualTable
						data="{partitions}"
						columns="{partitions[0]}"
						rowHeight="{35}"
						enableSearch="true"
					/>
					<br />
					Total items: {partitions.length}
				{:else}
					No data
				{/if}
			</TabContent>
			<TabContent
				><br />
				{#if snapshots_loading}
					<Loading withOverlay="{false}" small />
				{:else if snapshots.length > 0}
					<VirtualTable
						data="{snapshots}"
						columns="{snapshots[0]}"
						rowHeight="{35}"
						enableSearch="true"
					/>
					<br />
					Total items: {snapshots.length}
				{:else}
					No data
				{/if}
			</TabContent>
			<TabContent
				><br />
				<div class="sample-lable">Select # of rows to sample</div>
				<Dropdown
					hideLabel
					items="{sampleLimits}"
					selectedId="{selectedLimit.id}"
					selectedItem="{selectedLimit}"
					label="Sample Limit"
					titleText="Sample Limit"
					itemToString="{(item) => item?.text}"
					on:select="{(e) => {
						selectedLimit = e.detail.selectedItem;
						sample_limit.set(selectedLimit.id);
						fetchSampleData();
					}}"
				/>
				{#if sample_data_loading}
					<Loading withOverlay="{false}" small />
				{:else if !access_allowed}
					<ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data">
					</ToastNotification>
				{:else if sample_data.length > 0}
					<VirtualTable
						data="{sample_data}"
						columns="{sample_data[0]}"
						rowHeight="{35}"
						tableHeight="{sample_data.length > 13 ? 500 : (sample_data.length + 1) * 35}"
						enableSearch="true"
					/>
					<br />
					Sample items: {sample_data.length}
				{:else}
					<br />
					No data
				{/if}
			</TabContent>
			<TabContent>
				<br />
				<QueryRunner tableName="{namespace + '.' + table}" {pageSessionId} />
			</TabContent>

			<TabContent>
				{#if $healthEnabled}
					<br />
					<ButtonSet>
						<Button icon="{Run}" on:click="{() => {
								manualRunData.rules_requested = allRules.map(rule => rule.id);
								openRunModal = true;
							}
						}">Run Health Check</Button>
						<Button icon="{Calendar}" kind="secondary" on:click="{() => {
							scheduleRunData.rules_requested = allRules.map(rule => rule.id);
							openScheduleModal = true;
						}}">
							Schedule Health Check
						</Button>
						<Button
							kind="ghost"
							hasIconOnly
							class="cds--btn--icon-only"
							icon="{Renew}"
							iconDescription="Refresh Data"
							tooltipPosition="right"
							on:click="{() => {
								fetchRunningJobs();
								fetchTableInsights();
								fetchSchedules();
							}}"
						/>
					</ButtonSet>
					<div style="margin-top: 1.5rem;">
						<Tabs bind:selected="{insightsSubTab}">
							<Tab label="Completed Jobs" />
							<Tab label="In-Progress Jobs" />
							<Tab label="Scheduled Jobs" />
						</Tabs>
						<div class="tab-content-container">
							{#if insightsSubTab === 0}
								{#if insights_loading}
									<DataTableSkeleton rowCount="{5}" columnCount="{3}" />
								{:else if insightRuns.length === 0}
									<p>No health checks for this table.</p>
								{:else}
									<div class="insights-virtual-table-container">
										<VirtualTable
											data="{insightRuns}"
											columns="{virtualTableColumns}"
											disableVirtualization="{true}"
											bind:columnWidths
										>
											<div slot="cell" let:row let:columnKey>
												{#if columnKey === 'Job Type'}
													<Tag
														type="{row.run_type === 'manual' ? 'cyan' : 'green'}"
														title="{row.run_type}">{row.run_type}</Tag
													>
												{:else if columnKey === 'Timestamp'}
													{new Date(row.run_timestamp).toLocaleString()}
												{:else if columnKey === 'Rules & Results'}
													{@const codesWithResults = new Set(row.results.map((r) => r.code))}
													{@const sortedRules = row.rules_requested.slice().sort((a, b) => {
														const aHasResults = codesWithResults.has(a);
														const bHasResults = codesWithResults.has(b);
														return bHasResults - aHasResults;
													})}
													<div class="rules-cell-container">
														{#each sortedRules as ruleId}
															{@const hasResults = codesWithResults.has(ruleId)}
															{@const compositeKey = `${row.id}-${ruleId}`}
															{@const ruleResults = row.results.filter((r) => r.code === ruleId)}
															<div
																class="rule-item"
																role="button"
																tabindex="0"
																on:click="{() => {
																	if (hasResults) {
																		expandedRules[compositeKey] = !expandedRules[compositeKey];
																	}
																}}"
															>
																<div class="rule-item-header">
																	{#if hasResults}
																		<WarningAltFilled
																			size="{16}"
																			style="color: var(--cds-support-03, #ff832b);"
																		/>
																	{:else}
																		<CheckmarkFilled
																			size="{16}"
																			style="color: var(--cds-support-02, #24a148);"
																		/>
																	{/if}
																	<span>{ruleIdToNameMap.get(ruleId) || ruleId}</span>
																</div>

																{#if expandedRules[compositeKey]}
																	<div class="rule-details">
																		{#each ruleResults as result}
																			<div class="message-card">
																				<p><strong>Message:</strong> {result.message}</p>
																				<p>
																					<strong>Suggested Action:</strong>
																					{result.suggested_action}
																				</p>
																			</div>
																		{/each}
																	</div>
																{/if}
															</div>
														{/each}
													</div>
												{/if}
											</div>
										</VirtualTable>
									</div>
								{/if}
							{:else if insightsSubTab === 1}
								<div class="running-jobs-section">
									{#if runningJobsLoading}
										<DataTableSkeleton rowCount="{2}" columnCount="{4}" />
									{:else if runningJobs.length > 0}
										<VirtualTable
											data="{runningJobs}"
											columns="{runningJobsColumns}"
											disableVirtualization="{true}"
											bind:columnWidths="{runningJobsColumnWidths}"
										>
											<div slot="cell" let:row let:columnKey>
												{#if columnKey === 'Status'}
													<Tag type="blue">{row.Status}</Tag>
												{:else if columnKey === 'Started At'}
													{row['Started At'] ? new Date(row['Started At']).toLocaleString() : 'N/A'}
												{:else}
													{row[columnKey]}
												{/if}
											</div>
										</VirtualTable>
									{:else}
										<p>There are no running jobs for this table.</p>
									{/if}
								</div>
							{:else if insightsSubTab === 2}
								{#if schedulesLoading}
									<DataTableSkeleton rowCount="{3}" columnCount="{5}" />
								{:else if scheduledJobs.length === 0}
									<p>There are no scheduled jobs for this table.</p>
								{:else}
									<VirtualTable
										data="{scheduledJobs}"
										columns="{scheduledJobsColumns}"
										disableVirtualization="{true}"
										bind:columnWidths="{scheduledJobsColumnWidths}"
									>
										<div slot="cell" let:row let:columnKey>
											{#if columnKey === 'Rules'}
												{row.rules_requested.map((id) => ruleIdToNameMap.get(id) || id).join(', ')}
											{:else if columnKey === 'Schedule'}
												{displayScheduleFrequency(row.cron_schedule)}
											{:else if columnKey === 'Enabled'}
												<Tag type="{row.is_enabled ? 'green' : 'gray'}"
													>{row.is_enabled ? 'Enabled' : 'Disabled'}</Tag
												>
											{:else if columnKey === 'Next Run'}
												{new Date(row.next_run_timestamp).toLocaleString()}
											{:else if columnKey === 'Last Run'}
												{row.last_run_timestamp
													? new Date(row.last_run_timestamp).toLocaleString()
													: 'N/A'}
											{:else if columnKey === 'Actions'}
												<div class="action-buttons">
													<Button
														kind="ghost"
														icon="{Edit}"
														iconDescription="Edit"
														on:click="{() => openEditModal(row)}"
													/>
													<Button
														kind="ghost"
														icon="{TrashCan}"
														iconDescription="Delete"
														on:click="{() => openDeleteModal(row)}"
													/>
												</div>
											{/if}
										</div>
									</VirtualTable>
								{/if}
							{/if}
						</div>
					</div>
				{:else}
                    <br />
                    <p>
                        {@html HEALTH_DISABLED_MESSAGE}
                    </p>
                {/if}
			</TabContent>
		</svelte:fragment>
	</Tabs>
</Content>

<Modal
	bind:open="{openRunModal}"
	modalHeading="Run New Health Check"
	primaryButtonText="Start Job"
	secondaryButtonText="Cancel"
	on:submit="{handleManualRunSubmit}"
	on:click:button--secondary="{() => (openRunModal = false)}"
>
	<div class="readonly-field">
		<span class="readonly-label">Namespace</span><TextInput
			value="{manualRunData.namespace}"
			readOnly
		/>
	</div>
	<div class="readonly-field">
		<span class="readonly-label">Table Name</span><TextInput
			value="{manualRunData.tableName}"
			readOnly
		/>
	</div>
	<hr class="modal-divider" />
	<FormGroup>
		<legend class="bx--label legend-with-icon">
			<span>Rules to check</span>
			<button class="info-button" on:click="{() => showRulesInfoModal = true}" title="View rule descriptions">
				<Information size="{16}" />
			</button>
		</legend>
		<Checkbox
			labelText="Select All"
			checked="{manualRunSelectAll}"
			indeterminate="{manualRunIndeterminate}"
			on:change="{toggleSelectAllManual}"
		/>
		<hr class="modal-divider-light" />
		{#if allRules.length > 0}
			<div class="rules-grid">
				{#each allRules as rule}
					<Checkbox
						labelText="{rule.name}"
						value="{rule.id}"
						bind:group="{manualRunData.rules_requested}"
					/>
				{/each}
			</div>
		{:else}
			<p>Loading rules...</p>
		{/if}
	</FormGroup>
</Modal>
<Modal
	bind:open="{openScheduleModal}"
	modalHeading="Schedule New Health Check"
	primaryButtonText="Create Schedule"
	secondaryButtonText="Cancel"
	on:submit="{handleScheduleSubmit}"
	on:click:button--secondary="{() => (openScheduleModal = false)}"
>
	<div class="readonly-field">
		<span class="readonly-label">Namespace</span><TextInput
			value="{scheduleRunData.namespace}"
			readOnly
		/>
	</div>
	<div class="readonly-field">
		<span class="readonly-label">Table Name</span><TextInput
			value="{scheduleRunData.tableName}"
			readOnly
		/>
	</div>
	<hr class="modal-divider" />
	<FormGroup>
		<legend class="bx--label legend-with-icon">
			<span>Rules to Check</span>
			<button class="info-button" on:click="{() => showRulesInfoModal = true}" title="View rule descriptions">
				<Information size="{16}" />
			</button>
		</legend>
		<Checkbox
			labelText="Select All"
			checked="{scheduleRunSelectAll}"
			indeterminate="{scheduleRunIndeterminate}"
			on:change="{toggleSelectAllSchedule}"
		/>
		<hr class="modal-divider-light" />
		<div class="rules-grid">
			{#each allRules as rule}
				<Checkbox
					labelText="{rule.name}"
					value="{rule.id}"
					bind:group="{scheduleRunData.rules_requested}"
				/>
			{/each}
		</div>
	</FormGroup>
	<hr class="modal-divider" />
	<FormGroup legendText="Frequency">
		<Select bind:selected="{scheduleRunData.frequency}" on:change="{handleFrequencyChange}">
			<SelectItem value="weekly" text="Weekly" />
			<SelectItem value="biweekly" text="Biweekly (1st and 15th)" />
			<SelectItem value="monthly" text="Monthly (1st)" />
			<SelectItem value="bimonthly" text="Bimonthly (1st of even months)" />
			<SelectItem value="custom" text="Custom" />
		</Select>
	</FormGroup>
	<FormGroup legendText="Cron Schedule">
		<TextInput
			bind:value="{scheduleRunData.cron_schedule}"
			required
			readonly="{scheduleRunData.frequency !== 'custom'}"
			helperText="Format: minute hour day(month) month day(week)"
		/>
	</FormGroup>
</Modal>

{#if scheduleToEdit}
	<Modal
		bind:open="{showEditScheduleModal}"
		modalHeading="Edit Schedule"
		primaryButtonText="Save Changes"
		secondaryButtonText="Cancel"
		on:submit="{handleUpdateSchedule}"
		on:close="{() => (scheduleToEdit = null)}"
		on:click:button--secondary="{() => (showEditScheduleModal = false)}"
	>
		<FormGroup legendText="Rules to Schedule">
			<Checkbox
				labelText="Select All"
				on:change="{(e) => {
					if (e.currentTarget.checked) {
						scheduleToEdit.rules_requested = allRules.map((r) => r.id);
					} else {
						scheduleToEdit.rules_requested = [];
					}
				}}"
			/>
			<hr class="modal-divider-light" />
			<div class="rules-grid">
				{#each allRules as rule}
					<Checkbox
						labelText="{rule.name}"
						value="{rule.id}"
						bind:group="{scheduleToEdit.rules_requested}"
					/>
				{/each}
			</div>
		</FormGroup>
		<hr class="modal-divider" />
		<FormGroup legendText="Cron Schedule">
			<TextInput labelText="Cron String" bind:value="{scheduleToEdit.cron_schedule}" />
		</FormGroup>
		<hr class="modal-divider" />
		<Toggle labelText="Schedule Enabled" bind:toggled="{scheduleToEdit.is_enabled}" />
	</Modal>
{/if}

{#if scheduleToDelete}
	<Modal
		danger
		bind:open="{showDeleteConfirmModal}"
		modalHeading="Confirm Deletion"
		primaryButtonText="Delete"
		secondaryButtonText="Cancel"
		on:submit="{handleDeleteSchedule}"
		on:close="{() => (scheduleToDelete = null)}"
		on:click:button--secondary="{() => (showDeleteConfirmModal = false)}"
	>
		<p>Are you sure you want to delete this scheduled job?</p>
		<strong>{scheduleToDelete.cron_schedule}</strong>
	</Modal>
{/if}

<Modal
	passiveModal
	bind:open="{showRulesInfoModal}"
	modalHeading="Available Health Check Rules"
	size="lg"
>
	<table class="rules-table">
		<thead>
			<tr>
				<th>Rule Name</th>
				<th>Description</th>
			</tr>
		</thead>
		<tbody>
			{#each allRules as rule (rule.id)}
				<tr>
					<td>{rule.name}</td>
					<td>{rule.description}</td>
				</tr>
			{/each}
		</tbody>
	</table>
</Modal>

<style>
	table {
		width: 100%;
		border-collapse: collapse;
		margin-top: 0px;
	}

	td {
		border: 0.25px solid #ccc;
		padding: 8px;
		text-align: left;
		max-width: 200px;
		word-wrap: break-word;
	}
	.table-container {
		height: 500px;
		max-height: 500px;
		overflow-y: auto;
		padding: 10px;
	}
	.renew {
		display: flex;
		justify-content: flex-end;
		margin-bottom: 10px;
		padding-right: 10px;
	}
	.loading-container {
		margin-left: auto;
	}

	/* --- ORIGINAL PAGE STYLES --- */
	.sample-lable {
		margin-bottom: 20px;
	}
	.tile-content {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}
	.namespace-table-list {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 25px;
		margin: 0;
		font-size: 1.3em;
	}
	dt {
		font-weight: bold;
	}
	dd {
		margin: 0;
	}
	.copy-button-container {
		align-items: end;
		display: flex;
		justify-content: flex-end;
	}
	.tile-header {
		display: flex;
		justify-content: space-between;
	}
	.readonly-field {
		display: flex;
		align-items: center;
		gap: 1rem;
		margin-bottom: 1rem;
	}
	.readonly-label {
		flex-basis: 120px;
		flex-shrink: 0;
		font-weight: bold;
	}
	.rules-grid {
		display: grid;
		grid-template-columns: repeat(2, 1fr);
		gap: 0.5rem 1rem;
	}
	.modal-divider {
		margin: 1.5rem 0;
		border: none;
		border-top: 1px solid #e0e0e0;
	}
	.modal-divider-light {
		margin: 0.75rem 0;
		border: none;
		border-top: 1px solid #f4f4f4;
	}
	
	.running-jobs-section {
		margin-bottom: 2rem;
	}
	hr {
		margin: 2rem 0;
	}
	.insights-virtual-table-container :global(.cell:nth-child(1)),
	.insights-virtual-table-container :global(.cell:nth-child(2)) {
		align-items: center !important; /* Vertically center */
		justify-content: center; /* Horizontally center */
	}
	.insights-virtual-table-container :global(.cell:nth-child(3)) {
		align-items: flex-start !important;
		padding: 0 !important;
	}
	.action-buttons {
		display: flex;
		justify-content: flex-end;
	}
	.tab-content-container {
		padding: 1rem 0;
	}
</style>
