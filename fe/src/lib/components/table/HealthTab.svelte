<script>
	/**
	 * Health Check tab: run, schedule and review table health checks.
	 *
	 * Only mounted when the health feature is enabled; the parent renders the
	 * disabled message otherwise.
	 */
	import {
		Button,
		ButtonSet,
		Checkbox,
		DataTableSkeleton,
		FormGroup,
		Modal,
		Select,
		SelectItem,
		Tab,
		Tabs,
		Tag,
		TextInput,
		Toggle
	} from 'carbon-components-svelte';
	import {
		Calendar,
		CheckmarkFilled,
		Edit,
		Information,
		Renew,
		Run,
		TrashCan,
		WarningAltFilled
	} from 'carbon-icons-svelte';
	import VirtualTable from '../VirtTable3.svelte';
	import { user } from '$lib/stores';

	export let namespace = '';
	export let table = '';

	/** Notify the parent so toasts stay in one place. */
	export let onToast = () => {};

	const CRON_PRESETS = [
		{ value: 'weekly', label: 'Weekly', cron: '0 0 * * 0' },
		{ value: 'biweekly', label: 'Biweekly (1st and 15th)', cron: '0 0 1,15 * *' },
		{ value: 'monthly', label: 'Monthly (1st)', cron: '0 0 1 * *' },
		{ value: 'bimonthly', label: 'Bimonthly (1st of even months)', cron: '0 0 1 */2 *' }
	];

	let subTab = 0;
	let allRules = [];
	let ruleNames = new Map();
	let insightRuns = [];
	let runningJobs = [];
	let scheduledJobs = [];
	let insightsLoading = true;
	let runningLoading = true;
	let schedulesLoading = true;
	let loadedFor = null;
	let schedulesLoadedFor = null;
	let expandedRules = {};

	let showRunModal = false;
	let showScheduleModal = false;
	let showRulesInfoModal = false;
	let showEditModal = false;
	let showDeleteModal = false;
	let scheduleToEdit = null;
	let scheduleToDelete = null;

	let runRules = [];
	let scheduleRules = [];
	let scheduleFrequency = 'weekly';
	let scheduleCron = '0 0 * * 0';

	const runsColumns = { 'Job Type': '', Timestamp: '', 'Rules & Results': '' };
	let runsColumnWidths = { 'Job Type': 150, Timestamp: 250, 'Rules & Results': 600 };
	const runningColumns = { 'Job ID': '', Status: '', Details: '', 'Started At': '' };
	let runningColumnWidths = { 'Job ID': 300, Status: 120, Details: 400, 'Started At': 220 };
	const schedulesColumns = {
		Rules: '', Schedule: '', Enabled: '', 'Next Run': '', 'Last Run': '', Actions: ''
	};
	let schedulesColumnWidths = {
		Rules: 350, Schedule: 120, Enabled: 120, 'Next Run': 220, 'Last Run': 220, Actions: 100
	};

	$: qualified = namespace && table ? `${namespace}.${table}` : '';

	// Reload when the table changes; reset first so stale rows never show.
	$: if (qualified && loadedFor !== qualified) {
		loadedFor = qualified;
		schedulesLoadedFor = null;
		insightRuns = [];
		runningJobs = [];
		scheduledJobs = [];
		loadRules();
		loadInsights();
		loadRunningJobs();
	}

	$: if (subTab === 2 && qualified && schedulesLoadedFor !== qualified) {
		schedulesLoadedFor = qualified;
		loadSchedules();
	}

	$: allRunSelected = allRules.length > 0 && runRules.length === allRules.length;
	$: someRunSelected = runRules.length > 0 && !allRunSelected;
	$: allSchedSelected = allRules.length > 0 && scheduleRules.length === allRules.length;
	$: someSchedSelected = scheduleRules.length > 0 && !allSchedSelected;

	async function loadRules() {
		if (allRules.length) return;
		try {
			const res = await fetch('/api/lakehouse/insights/rules');
			if (!res.ok) throw new Error(res.statusText);
			allRules = await res.json();
			ruleNames = new Map(allRules.map((r) => [r.id, r.name]));
		} catch (err) {
			onToast('error', 'Could not load rules', err.message);
		}
	}

	async function loadInsights() {
		insightsLoading = true;
		try {
			const res = await fetch(
				`/api/namespaces/${encodeURIComponent(namespace)}/${encodeURIComponent(table)}/insights?size=100`
			);
			if (!res.ok) throw new Error(res.statusText);
			const runs = await res.json();
			runs.sort((a, b) => new Date(b.run_timestamp) - new Date(a.run_timestamp));
			insightRuns = runs;
			// Start collapsed: auto-expanding every rule of every run produced a
			// wall of message cards on tables with many findings.
			expandedRules = {};
		} catch (err) {
			onToast('error', 'Could not load health checks', err.message);
		} finally {
			insightsLoading = false;
		}
	}

	async function loadRunningJobs() {
		runningLoading = true;
		try {
			const res = await fetch(
				`/api/jobs/running?namespace=${encodeURIComponent(namespace)}&table_name=${encodeURIComponent(table)}`
			);
			if (!res.ok) throw new Error(res.statusText);
			runningJobs = (await res.json()).map((job) => ({
				id: job.run_id,
				'Job ID': job.run_id,
				Status: job.status,
				Details: job.details,
				'Started At': job.started_at
			}));
		} catch {
			runningJobs = [];
		} finally {
			runningLoading = false;
		}
	}

	async function loadSchedules() {
		schedulesLoading = true;
		try {
			const res = await fetch(
				`/api/schedules?namespace=${encodeURIComponent(namespace)}&table_name=${encodeURIComponent(table)}`
			);
			if (!res.ok) throw new Error(res.statusText);
			scheduledJobs = await res.json();
		} catch (err) {
			onToast('error', 'Could not load schedules', err.message);
		} finally {
			schedulesLoading = false;
		}
	}

	export function refresh() {
		loadInsights();
		loadRunningJobs();
		if (schedulesLoadedFor) loadSchedules();
	}

	function openRunModal() {
		runRules = allRules.map((r) => r.id);
		showRunModal = true;
	}

	function openScheduleModal() {
		scheduleRules = allRules.map((r) => r.id);
		showScheduleModal = true;
	}

	async function submitRun() {
		if (!runRules.length) {
			onToast('warning', 'Nothing selected', 'Select at least one rule to check.');
			return;
		}
		try {
			const res = await fetch('/api/start-run', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ namespace, table_name: table, rules_requested: runRules })
			});
			if (res.status !== 202) throw new Error('Failed to start job');
			const result = await res.json();
			onToast('success', 'Job started', `Job ID: ${result.run_id}`);
			showRunModal = false;
			setTimeout(refresh, 3000);
		} catch (err) {
			onToast('error', 'Could not start job', err.message);
		}
	}

	function applyFrequency() {
		const preset = CRON_PRESETS.find((p) => p.value === scheduleFrequency);
		if (preset) scheduleCron = preset.cron;
	}

	function describeCron(cron) {
		return CRON_PRESETS.find((p) => p.cron === cron)?.value ?? 'custom';
	}

	async function submitSchedule() {
		if (!scheduleRules.length) {
			onToast('warning', 'Nothing selected', 'Select at least one rule for the schedule.');
			return;
		}
		try {
			const res = await fetch('/api/schedules', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					namespace,
					table_name: table || null,
					rules_requested: scheduleRules,
					cron_schedule: scheduleCron,
					// Previously hardcoded to "testuser".
					created_by: $user?.email ?? $user?.id ?? 'unknown'
				})
			});
			if (res.status !== 201) throw new Error('Failed to create schedule');
			const result = await res.json();
			onToast('success', 'Schedule created', `Schedule ID: ${result.id}`);
			showScheduleModal = false;
			if (schedulesLoadedFor) loadSchedules();
		} catch (err) {
			onToast('error', 'Could not create schedule', err.message);
		}
	}

	function openEdit(schedule) {
		scheduleToEdit = structuredClone(schedule);
		showEditModal = true;
	}

	async function submitEdit() {
		if (!scheduleToEdit) return;
		try {
			const res = await fetch(`/api/schedules/${scheduleToEdit.id}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					rules_requested: scheduleToEdit.rules_requested,
					cron_schedule: scheduleToEdit.cron_schedule,
					is_enabled: scheduleToEdit.is_enabled
				})
			});
			if (!res.ok) {
				const body = await res.json().catch(() => ({}));
				throw new Error(body.detail || 'Failed to update schedule');
			}
			onToast('success', 'Schedule updated', 'Changes saved.');
			showEditModal = false;
			loadSchedules();
		} catch (err) {
			onToast('error', 'Update failed', err.message);
		}
	}

	async function submitDelete() {
		if (!scheduleToDelete) return;
		try {
			const res = await fetch(`/api/schedules/${scheduleToDelete.id}`, { method: 'DELETE' });
			if (res.status !== 204) throw new Error('Failed to delete schedule');
			onToast('success', 'Schedule deleted', 'The schedule was removed.');
			showDeleteModal = false;
			loadSchedules();
		} catch (err) {
			onToast('error', 'Delete failed', err.message);
		}
	}

	function toggleAll(event, target) {
		const ids = event.currentTarget.checked ? allRules.map((r) => r.id) : [];
		if (target === 'run') runRules = ids;
		else scheduleRules = ids;
	}

	function formatTime(value) {
		return value ? new Date(value).toLocaleString() : 'N/A';
	}
</script>

<br />
<ButtonSet>
	<Button icon={Run} on:click={openRunModal}>Run Health Check</Button>
	<Button icon={Calendar} kind="secondary" on:click={openScheduleModal}>
		Schedule Health Check
	</Button>
	<Button
		kind="ghost"
		hasIconOnly
		class="cds--btn--icon-only"
		icon={Renew}
		iconDescription="Refresh Data"
		tooltipPosition="right"
		on:click={refresh}
	/>
</ButtonSet>

<div class="subtabs">
	<Tabs bind:selected={subTab}>
		<Tab label="Completed Jobs" />
		<Tab label="In-Progress Jobs" />
		<Tab label="Scheduled Jobs" />
	</Tabs>
	<div class="subtab-content">
		{#if subTab === 0}
			{#if insightsLoading}
				<DataTableSkeleton rowCount={5} columnCount={3} />
			{:else if insightRuns.length === 0}
				<p>No health checks for this table.</p>
			{:else}
				<div class="runs-table">
					<VirtualTable
						data={insightRuns}
						columns={runsColumns}
						disableVirtualization={true}
						bind:columnWidths={runsColumnWidths}
						storageKey="health-runs"
					>
						<div slot="cell" let:row let:columnKey>
							{#if columnKey === 'Job Type'}
								<Tag type={row.run_type === 'manual' ? 'cyan' : 'green'} title={row.run_type}>
									{row.run_type}
								</Tag>
							{:else if columnKey === 'Timestamp'}
								{formatTime(row.run_timestamp)}
							{:else if columnKey === 'Rules & Results'}
								{@const codesWithResults = new Set(row.results.map((r) => r.code))}
								{@const sortedRules = row.rules_requested
									.slice()
									.sort(
										(a, b) => codesWithResults.has(b) - codesWithResults.has(a)
									)}
								{@const findingCount = row.rules_requested.filter((r) =>
									codesWithResults.has(r)
								).length}
								<div class="rules-cell">
									{#if findingCount > 0}
										<p class="finding-count">
											{findingCount} of {row.rules_requested.length} rules reported findings
										</p>
									{/if}
									{#each sortedRules as ruleId (ruleId)}
										{@const hasResults = codesWithResults.has(ruleId)}
										{@const key = `${row.id}-${ruleId}`}
										{@const results = row.results.filter((r) => r.code === ruleId)}
										<div class="rule-item">
											<button
												type="button"
												class="rule-header"
												disabled={!hasResults}
												aria-expanded={hasResults ? !!expandedRules[key] : undefined}
												on:click={() => {
													if (hasResults) expandedRules[key] = !expandedRules[key];
												}}
											>
												{#if hasResults}
													<WarningAltFilled
														size={16}
														style="color: var(--cds-support-03, #ff832b);"
													/>
												{:else}
													<CheckmarkFilled
														size={16}
														style="color: var(--cds-support-02, #24a148);"
													/>
												{/if}
												<span>{ruleNames.get(ruleId) || ruleId}</span>
											</button>
											{#if expandedRules[key]}
												<div class="rule-details">
													{#each results as result}
														<div class="message-card">
															<p><strong>Message:</strong> {result.message}</p>
															<p><strong>Suggested Action:</strong> {result.suggested_action}</p>
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
		{:else if subTab === 1}
			{#if runningLoading}
				<DataTableSkeleton rowCount={2} columnCount={4} />
			{:else if runningJobs.length}
				<VirtualTable
					data={runningJobs}
					columns={runningColumns}
					disableVirtualization={true}
					bind:columnWidths={runningColumnWidths}
					storageKey="health-running"
				>
					<div slot="cell" let:row let:columnKey>
						{#if columnKey === 'Status'}
							<Tag type="blue">{row.Status}</Tag>
						{:else if columnKey === 'Started At'}
							{formatTime(row['Started At'])}
						{:else}
							{row[columnKey]}
						{/if}
					</div>
				</VirtualTable>
			{:else}
				<p>There are no running jobs for this table.</p>
			{/if}
		{:else if subTab === 2}
			{#if schedulesLoading}
				<DataTableSkeleton rowCount={3} columnCount={5} />
			{:else if scheduledJobs.length === 0}
				<p>There are no scheduled jobs for this table.</p>
			{:else}
				<VirtualTable
					data={scheduledJobs}
					columns={schedulesColumns}
					disableVirtualization={true}
					bind:columnWidths={schedulesColumnWidths}
					storageKey="health-schedules"
				>
					<div slot="cell" let:row let:columnKey>
						{#if columnKey === 'Rules'}
							{row.rules_requested.map((id) => ruleNames.get(id) || id).join(', ')}
						{:else if columnKey === 'Schedule'}
							{describeCron(row.cron_schedule)}
						{:else if columnKey === 'Enabled'}
							<Tag type={row.is_enabled ? 'green' : 'gray'}>
								{row.is_enabled ? 'Enabled' : 'Disabled'}
							</Tag>
						{:else if columnKey === 'Next Run'}
							{formatTime(row.next_run_timestamp)}
						{:else if columnKey === 'Last Run'}
							{formatTime(row.last_run_timestamp)}
						{:else if columnKey === 'Actions'}
							<div class="row-actions">
								<Button
									kind="ghost"
									icon={Edit}
									iconDescription="Edit"
									on:click={() => openEdit(row)}
								/>
								<Button
									kind="ghost"
									icon={TrashCan}
									iconDescription="Delete"
									on:click={() => {
										scheduleToDelete = row;
										showDeleteModal = true;
									}}
								/>
							</div>
						{/if}
					</div>
				</VirtualTable>
			{/if}
		{/if}
	</div>
</div>

<Modal
	bind:open={showRunModal}
	modalHeading="Run New Health Check"
	primaryButtonText="Start Job"
	secondaryButtonText="Cancel"
	on:submit={submitRun}
	on:click:button--secondary={() => (showRunModal = false)}
>
	<div class="readonly-field">
		<span class="readonly-label">Namespace</span><TextInput value={namespace} readOnly />
	</div>
	<div class="readonly-field">
		<span class="readonly-label">Table Name</span><TextInput value={table} readOnly />
	</div>
	<hr />
	<FormGroup>
		<legend class="bx--label legend-with-icon">
			<span>Rules to check</span>
			<button
				type="button"
				class="info-button"
				on:click={() => (showRulesInfoModal = true)}
				title="View rule descriptions"
			>
				<Information size={16} />
			</button>
		</legend>
		<Checkbox
			labelText="Select All"
			checked={allRunSelected}
			indeterminate={someRunSelected}
			on:change={(e) => toggleAll(e, 'run')}
		/>
		<hr class="light" />
		{#if allRules.length}
			<div class="rules-grid">
				{#each allRules as rule (rule.id)}
					<Checkbox labelText={rule.name} value={rule.id} bind:group={runRules} />
				{/each}
			</div>
		{:else}
			<p>Loading rules...</p>
		{/if}
	</FormGroup>
</Modal>

<Modal
	bind:open={showScheduleModal}
	modalHeading="Schedule New Health Check"
	primaryButtonText="Create Schedule"
	secondaryButtonText="Cancel"
	on:submit={submitSchedule}
	on:click:button--secondary={() => (showScheduleModal = false)}
>
	<div class="readonly-field">
		<span class="readonly-label">Namespace</span><TextInput value={namespace} readOnly />
	</div>
	<div class="readonly-field">
		<span class="readonly-label">Table Name</span><TextInput value={table} readOnly />
	</div>
	<hr />
	<FormGroup>
		<legend class="bx--label legend-with-icon">
			<span>Rules to Check</span>
			<button
				type="button"
				class="info-button"
				on:click={() => (showRulesInfoModal = true)}
				title="View rule descriptions"
			>
				<Information size={16} />
			</button>
		</legend>
		<Checkbox
			labelText="Select All"
			checked={allSchedSelected}
			indeterminate={someSchedSelected}
			on:change={(e) => toggleAll(e, 'schedule')}
		/>
		<hr class="light" />
		<div class="rules-grid">
			{#each allRules as rule (rule.id)}
				<Checkbox labelText={rule.name} value={rule.id} bind:group={scheduleRules} />
			{/each}
		</div>
	</FormGroup>
	<hr />
	<FormGroup legendText="Frequency">
		<Select bind:selected={scheduleFrequency} on:change={applyFrequency}>
			{#each CRON_PRESETS as preset}
				<SelectItem value={preset.value} text={preset.label} />
			{/each}
			<SelectItem value="custom" text="Custom" />
		</Select>
	</FormGroup>
	<FormGroup legendText="Cron Schedule">
		<TextInput
			bind:value={scheduleCron}
			required
			readonly={scheduleFrequency !== 'custom'}
			helperText="Format: minute hour day(month) month day(week)"
		/>
	</FormGroup>
</Modal>

{#if scheduleToEdit}
	<Modal
		bind:open={showEditModal}
		modalHeading="Edit Schedule"
		primaryButtonText="Save Changes"
		secondaryButtonText="Cancel"
		on:submit={submitEdit}
		on:close={() => (scheduleToEdit = null)}
		on:click:button--secondary={() => (showEditModal = false)}
	>
		<FormGroup legendText="Rules to Schedule">
			<Checkbox
				labelText="Select All"
				checked={scheduleToEdit.rules_requested.length === allRules.length}
				on:change={(e) => {
					scheduleToEdit.rules_requested = e.currentTarget.checked
						? allRules.map((r) => r.id)
						: [];
				}}
			/>
			<hr class="light" />
			<div class="rules-grid">
				{#each allRules as rule (rule.id)}
					<Checkbox
						labelText={rule.name}
						value={rule.id}
						bind:group={scheduleToEdit.rules_requested}
					/>
				{/each}
			</div>
		</FormGroup>
		<hr />
		<FormGroup legendText="Frequency">
			<!-- The edit modal previously offered only a raw cron field, so a
			     schedule created as "weekly" reopened as an opaque string. -->
			<Select
				selected={describeCron(scheduleToEdit.cron_schedule)}
				on:change={(e) => {
					const preset = CRON_PRESETS.find((p) => p.value === e.target.value);
					if (preset) scheduleToEdit.cron_schedule = preset.cron;
				}}
			>
				{#each CRON_PRESETS as preset}
					<SelectItem value={preset.value} text={preset.label} />
				{/each}
				<SelectItem value="custom" text="Custom" />
			</Select>
		</FormGroup>
		<FormGroup legendText="Cron Schedule">
			<TextInput labelText="Cron String" bind:value={scheduleToEdit.cron_schedule} />
		</FormGroup>
		<hr />
		<Toggle labelText="Schedule Enabled" bind:toggled={scheduleToEdit.is_enabled} />
	</Modal>
{/if}

{#if scheduleToDelete}
	<Modal
		danger
		bind:open={showDeleteModal}
		modalHeading="Confirm Deletion"
		primaryButtonText="Delete"
		secondaryButtonText="Cancel"
		on:submit={submitDelete}
		on:close={() => (scheduleToDelete = null)}
		on:click:button--secondary={() => (showDeleteModal = false)}
	>
		<p>Are you sure you want to delete this scheduled job?</p>
		<strong>{scheduleToDelete.cron_schedule}</strong>
	</Modal>
{/if}

<Modal
	passiveModal
	bind:open={showRulesInfoModal}
	modalHeading="Available Health Check Rules"
	size="lg"
>
	<table class="rules-table">
		<thead>
			<tr><th scope="col">Rule Name</th><th scope="col">Description</th></tr>
		</thead>
		<tbody>
			{#each allRules as rule (rule.id)}
				<tr><td>{rule.name}</td><td>{rule.description}</td></tr>
			{/each}
		</tbody>
	</table>
</Modal>

<style>
	.subtabs {
		margin-top: 1.5rem;
	}
	.subtab-content {
		padding: 1rem 0;
	}
	.runs-table :global(.cell:nth-child(1)),
	.runs-table :global(.cell:nth-child(2)) {
		align-items: center !important;
		justify-content: center;
	}
	.runs-table :global(.cell:nth-child(3)) {
		align-items: flex-start !important;
		padding: 0 !important;
	}
	.rules-cell {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
		padding: 8px;
		width: 100%;
	}
	.finding-count {
		margin: 0;
		font-size: 12px;
		color: var(--cds-text-secondary, #525252);
	}
	.rule-item {
		padding: 4px;
		border-radius: 4px;
	}
	.rule-header {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		color: inherit;
		cursor: pointer;
		text-align: left;
		width: 100%;
	}
	.rule-header:disabled {
		cursor: default;
	}
	.rule-details {
		margin-top: 0.5rem;
		padding-left: 24px;
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}
	.message-card {
		background-color: var(--cds-layer, #fff);
		border-left: 3px solid var(--cds-support-03, #ff832b);
		padding: 0.5rem 1rem;
		border-radius: 4px;
	}
	.message-card p {
		margin: 0.2rem 0;
		font-size: 13px;
	}
	.row-actions {
		display: flex;
		justify-content: flex-end;
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
		/* Collapses to one column when the modal is too narrow for two. */
		grid-template-columns: repeat(auto-fit, minmax(14rem, 1fr));
		gap: 0.5rem 1rem;
	}
	hr {
		margin: 1.5rem 0;
		border: none;
		border-top: 1px solid var(--cds-border-subtle, #e0e0e0);
	}
	hr.light {
		margin: 0.75rem 0;
	}
	.rules-table {
		width: 100%;
		border-collapse: collapse;
	}
	.rules-table {
		display: block;
		overflow-x: auto;
	}
	.rules-table th,
	.rules-table td {
		border: 1px solid var(--cds-ui-03, #e0e0e0);
		padding: 0.75rem;
		text-align: left;
		vertical-align: top;
	}
	.rules-table th {
		background-color: var(--cds-ui-01, #f4f4f4);
		font-weight: 600;
	}
</style>
