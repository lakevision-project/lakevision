<script>
    import { onMount } from 'svelte';
    import { browser } from '$app/environment';
    import {
        Content,
        Tabs,
        Tab,
        DataTableSkeleton,
        Button,
        ButtonSet,
        Tag,
        Modal,
        FormGroup,
        Checkbox,
        Select,
        SelectItem,
        TextInput,
        ToastNotification,
        Dropdown,
        Toggle,
        ComboBox,
        Accordion,
        AccordionItem,
        Search
    } from 'carbon-components-svelte';
    import { Renew, Run, Calendar, WarningAltFilled, CheckmarkFilled, Information , ChevronRight} from 'carbon-icons-svelte';
    import VirtualTable from '$lib/components/VirtTable3.svelte';
    import '@carbon/charts-svelte/styles.css';

    // --- State for the three tabs ---
    let insightsSubTab = 0;
    let insights_loading = true;
    let insightRuns = [];
    let runningJobsLoading = true;
    let runningJobs = [];
    let schedulesLoading = true;
    let scheduledJobs = [];
    let overviewLoading = true;
    let overviewData = [];
    let rawOverviewData = [];
    let groupedOverviewData = {};
    let overviewSearchTerm = '';
    let expandedOccurrences = {};
    let expandedNamespaces = {};

    const severityOrder = { critical: 3, warning: 2, info: 1 };

    // --- State for rule names and expanding results ---
    let allRules = [];
    let ruleIdToNameMap = new Map();
    let expandedRules = {};

    // --- State for Modals and Forms ---
    let openRunModal = false;
    let openScheduleModal = false;
    let showRulesInfoModal = false;

    let manualRunData = { namespace: '*', rules_requested: [] };
    let manualRunSelectAll = false;
    let manualRunIndeterminate = false;

    let scheduleRunData = {
        namespace: '*',
        rules_requested: [],
        cron_schedule: '0 0 * * 0',
        frequency: 'weekly',
        created_by: 'system'
    };
    let scheduleRunSelectAll = false;
    let scheduleRunIndeterminate = false;

    // --- Toast Notification State ---
    let toastProps = { open: false, kind: 'info', title: '', subtitle: '' };
    let toastTimeout;

    // --- State for table controls ---
    let completedRunsLimit = 100;
    const limitOptions = [
        { id: 10, text: '10' },
        { id: 25, text: '25' },
        { id: 50, text: '50' },
        { id: 100, text: '100' },
        { id: 500, text: '500' },
        { id: 1000, text: '1000' },
        { id: 2500, text: '2500' }
    ];
    let showEmptyResults = false;
    
    // --- State for namespace dropdown ---
    let allNamespaces = [];
    $: dropdownNamespaces = [{ id: '*', text: 'ALL' }, ...allNamespaces];
    
    // Temporary state for modal dropdowns
    let runModalSelectedNsId = '*';
    let scheduleModalSelectedNsId = '*';

    const overviewColumns = {
        Rule: '',
        Namespace: '',
        'Suggested Action': '',
        'Affected Tables': ''
    };
    let overviewColWidths = {
        Rule: 200,
        Namespace: 150,
        'Suggested Action': 400,
        'Affected Tables': 700
    };

    // Reactive logic to translate selected ID to the correct name for the API payload
    $: {
        const selectedRunNs = dropdownNamespaces.find(ns => ns.id === runModalSelectedNsId);
        if (selectedRunNs) {
            manualRunData.namespace = selectedRunNs.id === '*' ? '*' : selectedRunNs.text;
        }
    }
    $: {
        const selectedScheduleNs = dropdownNamespaces.find(ns => ns.id === scheduleModalSelectedNsId);
        if (selectedScheduleNs) {
            scheduleRunData.namespace = selectedScheduleNs.id === '*' ? '*' : selectedScheduleNs.text;
        }
    }


    // --- Helper functions for highlighting ---
    function escapeHtml(text) {
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
    function highlightMatch(text, query) {
        if (!query || !text) return escapeHtml(text);
        const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        return escapeHtml(String(text)).replace(regex, '<mark>$1</mark>');
    }

    function showToast(kind, title, subtitle, timeout = 4000) {
        toastProps = { open: true, kind, title, subtitle };
        if (toastTimeout) clearTimeout(toastTimeout);
        toastTimeout = setTimeout(() => {
            toastProps.open = false;
        }, timeout);
    }

    // --- Define columns for the virtual tables ---
    const completedRunsColumns = {
        'Job Type': '', Namespace: '', 'Table Name': '', Timestamp: '', 'Rules & Results': ''
    };
    let completedRunsColWidths = {
        'Job Type': 120, Namespace: 150, 'Table Name': 200, Timestamp: 220, 'Rules & Results': 700
    };
    const runningJobsColumns = {
        'Job ID': '', Namespace: '', 'Table Name': '', Status: '', 'Started At': ''
    };
    let runningJobsColWidths = {
        'Job ID': 300, Namespace: 150, 'Table Name': 200, Status: 120, 'Started At': 220
    };
    const scheduledJobsColumns = {
        Namespace: '', 'Table Name': '', Rules: '', Schedule: '', Enabled: '', 'Next Run': ''
    };
    let scheduledJobsColWidths = {
        Namespace: 150, 'Table Name': 200, Rules: 350, Schedule: 120, Enabled: 120, 'Next Run': 220
    };

    // --- Data Fetching ---
    onMount(() => {
        fetchOverviewData();
        fetchRunningJobs();
        fetchSchedules();
        if (allRules.length === 0) fetchAllRules();
        if (allNamespaces.length === 0) fetchAllNamespaces();
    });

    $: {
        completedRunsLimit;
        showEmptyResults;
        if (browser) {
            fetchLatestInsights();
        }
    }

    $: {
        let processedData = rawOverviewData;

        if (overviewSearchTerm) {
            const lowerCaseSearch = overviewSearchTerm.toLowerCase();

            processedData = rawOverviewData
                .map(item => {
                    // Check for matches at the group level (rule name and namespace)
                    const ruleName = ruleIdToNameMap.get(item.code) || item.code;
                    const ruleMatch = ruleName.toLowerCase().includes(lowerCaseSearch);
                    const namespaceMatch = item.namespace.toLowerCase().includes(lowerCaseSearch);

                    // Always filter the tables within the group
                    const filteredOccurrences = item.occurrences.filter(occ =>
                        occ.table_name.toLowerCase().includes(lowerCaseSearch)
                    );

                    // Case 1: If the rule or namespace itself matches, show the group with ALL its tables.
                    // This provides context. E.g., searching for a namespace shows all its tables.
                    if (ruleMatch || namespaceMatch) {
                        return item;
                    }

                    // Case 2: If only a table name matched, show the group but with ONLY the filtered tables.
                    if (filteredOccurrences.length > 0) {
                        return { ...item, occurrences: filteredOccurrences };
                    }

                    // Case 3: If nothing matched, exclude this group from the results.
                    return null;
                })
                .filter(Boolean); // This removes any `null` entries from the array.
        }
        
        // This function groups the now correctly filtered data into the hierarchical structure.
        const groupData = (data) => {
            const groups = {};
            

            data.forEach(item => {
                const ruleName = ruleIdToNameMap.get(item.code) || item.code;
                if (!groups[ruleName]) {
                    groups[ruleName] = { namespaces: {}, suggested_action: item.suggested_action };
                }
                if (!groups[ruleName].namespaces[item.namespace]) {
                    groups[ruleName].namespaces[item.namespace] = [];
                }
                groups[ruleName].namespaces[item.namespace].push(...item.occurrences);
            });
            
            for (const rule of Object.values(groups)) {
                let maxSeverity = 'info';
                let maxSeverityValue = 1;
                
                for (const tables of Object.values(rule.namespaces)) {
                    for (const table of tables) {
                        const currentSeverityValue = severityOrder[table.severity.toLowerCase()] || 1;
                        if (currentSeverityValue > maxSeverityValue) {
                            maxSeverityValue = currentSeverityValue;
                            maxSeverity = table.severity.toLowerCase();
                        }
                    }
                }
                rule.highestSeverity = maxSeverity;
            }
            return groups;
        };

        groupedOverviewData = groupData(processedData);
    }

    async function fetchOverviewData() {
        overviewLoading = true;
        try {
            const response = await fetch(`/api/namespaces/*/insights/summary`);
            if (!response.ok) throw new Error('Failed to fetch overview summary');
            
            const rawData = await response.json();

            rawOverviewData = rawData.map(item => ({
                ...item,
                id: `${item.code}-${item.namespace}` 
            }));

            let newExpanded = {};
            rawData.forEach(item => {
                const ruleName = ruleIdToNameMap.get(item.code) || item.code;
                const namespaceKey = `${ruleName}-${item.namespace}`;
                newExpanded[namespaceKey] = true;
            });
            expandedNamespaces = newExpanded;

        } catch (error) {
            console.error('Error fetching overview summary:', error);
            rawOverviewData = [];
        } finally {
            overviewLoading = false;
        }
    }

    async function fetchLatestInsights() {
        insights_loading = true;
        try {
            const response = await fetch(`/api/namespaces/*/insights?&size=${completedRunsLimit}&showEmpty=${showEmptyResults}`);
            if (!response.ok) throw new Error('Failed to fetch latest runs');
            
            const rawData = await response.json();

            rawData.sort((a, b) => {
                const resultSort = (b.results?.length > 0 ? 1 : 0) - (a.results?.length > 0 ? 1 : 0);
                if (resultSort !== 0) {
                    return resultSort;
                }
                return new Date(b.run_timestamp) - new Date(a.run_timestamp);
            });

            insightRuns = rawData;

        } catch (error) { console.error('Error fetching latest runs:', error); }
        finally { insights_loading = false; }
    }

    async function fetchRunningJobs() {
        runningJobsLoading = true;
        try {
            const response = await fetch(`/api/jobs/running?namespace=*`);
            if (!response.ok) throw new Error('Failed to fetch running jobs');
            const data = await response.json();
            runningJobs = data.map((job) => ({ ...job, id: job.run_id }));
        } catch (error) { console.error('Error fetching running jobs:', error); }
        finally { runningJobsLoading = false; }
    }

    async function fetchSchedules() {
        schedulesLoading = true;
        try {
            const response = await fetch(`/api/schedules?namespace=*`);
            if (!response.ok) throw new Error('Failed to fetch schedules');
            scheduledJobs = await response.json();
        } catch (error) { console.error('Error fetching schedules:', error); }
        finally { schedulesLoading = false; }
    }

    async function fetchAllRules() {
        try {
            const response = await fetch('/api/lakehouse/insights/rules');
            if (!response.ok) throw new Error('Failed to fetch rules');
            allRules = await response.json();
            ruleIdToNameMap = new Map(allRules.map((rule) => [rule.id, rule.name]));
        } catch (error) { console.error('Error fetching rules:', error); }
    }
    
    async function fetchAllNamespaces() {
        try {
            const response = await fetch('/api/namespaces');
            if (!response.ok) throw new Error('Failed to fetch namespaces');
            allNamespaces = await response.json();
        } catch (error) { console.error('Error fetching namespaces:', error); }
    }

    $: {
        if (allRules.length > 0) {
            manualRunSelectAll = manualRunData.rules_requested.length === allRules.length;
            manualRunIndeterminate = manualRunData.rules_requested.length > 0 && !manualRunSelectAll;
            scheduleRunSelectAll = scheduleRunData.rules_requested.length === allRules.length;
            scheduleRunIndeterminate = scheduleRunData.rules_requested.length > 0 && !scheduleRunSelectAll;
        }
    }

    function toggleSelectAllManual(event) {
        manualRunData.rules_requested = event.currentTarget.checked ? allRules.map(rule => rule.id) : [];
    }

    function toggleSelectAllSchedule(event) {
        scheduleRunData.rules_requested = event.currentTarget.checked ? allRules.map(rule => rule.id) : [];
    }

    async function handleManualRunSubmit() {
        if (manualRunData.rules_requested.length === 0) {
            showToast('warning', 'Missing Information', 'Please select at least one rule.');
            return;
        }
        try {
            const response = await fetch('/api/start-run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    namespace: manualRunData.namespace,
                    rules_requested: manualRunData.rules_requested
                })
            });
            if (response.status !== 202) throw new Error('Failed to start job');
            const result = await response.json();
            showToast('success', 'Job Started', `Job ID: ${result.run_id}`);
            openRunModal = false;
            setTimeout(fetchRunningJobs, 2000);
        } catch (error) {
            showToast('error', 'Error Starting Job', error.message);
        }
    }

    async function handleScheduleSubmit() {
        if (scheduleRunData.rules_requested.length === 0) {
            showToast('warning', 'Missing Information', 'Please select at least one rule.');
            return;
        }
        try {
            const response = await fetch('/api/schedules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    namespace: scheduleRunData.namespace,
                    rules_requested: scheduleRunData.rules_requested,
                    cron_schedule: scheduleRunData.cron_schedule,
                    created_by: scheduleRunData.created_by
                })
            });
            if (response.status !== 201) throw new Error('Failed to create schedule');
            const result = await response.json();
            showToast('success', 'Schedule Created', `Schedule ID: ${result.id}`);
            openScheduleModal = false;
            setTimeout(fetchSchedules, 2000);
        } catch (error) {
            showToast('error', 'Error Creating Schedule', error.message);
        }
    }

    function resetModalForms() {
        manualRunData.rules_requested = [];
        manualRunData.namespace = '*';
        scheduleRunData.rules_requested = [];
        scheduleRunData.namespace = '*';
        runModalSelectedNsId = '*';
        scheduleModalSelectedNsId = '*';
    }
</script>

<Content>
    {#if toastProps.open}
        <ToastNotification
            kind="{toastProps.kind}"
            title="{toastProps.title}"
            subtitle="{toastProps.subtitle}"
            caption="{new Date().toLocaleString()}"
            on:close="{() => (toastProps.open = false)}"
            style="position: fixed; top: 10%; left: 50%; transform: translate(-50%, -50%); z-index: 9999; min-width: 300px;"
        />
    {/if}

    <div class="header-container">
        <h1>Lakehouse Health</h1>
        <ButtonSet>
            <Button icon="{Run}" on:click="{() => {
                manualRunData.rules_requested = allRules.map(rule => rule.id);
                runModalSelectedNsId = '*'; // Set default ID
                openRunModal = true;
            }}">Run Health Check</Button>
            <Button icon="{Calendar}" kind="secondary" on:click="{() => {
                scheduleRunData.rules_requested = allRules.map(rule => rule.id);
                scheduleModalSelectedNsId = '*'; // Set default ID
                openScheduleModal = true;
            }}">Schedule Health Check</Button>
            <Button kind="ghost" class="cds--btn--icon-only" icon="{Renew}" iconDescription="Refresh All Data" on:click="{() => { fetchOverviewData();fetchLatestInsights(); fetchRunningJobs(); fetchSchedules(); }}"/>
        </ButtonSet>
    </div>

    <Tabs bind:selected="{insightsSubTab}" style="margin-top: 1rem;">
        <Tab label="Overview" />
        <Tab label="Completed Jobs" />
        <Tab label="In-Progress Jobs" />
        <Tab label="Scheduled Jobs" />
    </Tabs>
    <div class="tab-content-container">
        {#if insightsSubTab === 0}
            <div class="overview-controls">
                <Search
                    placeholder="Filter by rule, namespace, or table name..."
                    bind:value="{overviewSearchTerm}"
                />
            </div>

            {#if overviewLoading}
                <DataTableSkeleton rowCount="{5}" columnCount="{1}" />
            {:else if Object.keys(groupedOverviewData).length === 0}
                <p>
                    {#if overviewSearchTerm}
                        No active insights match your filter criteria.
                    {:else}
                        No active insights found across the lakehouse. Great job!
                    {/if}
                </p>
            {:else}
                <Accordion>
                    {#each Object.entries(groupedOverviewData).sort(([, ruleA], [, ruleB]) => {
                        const severityValueA = severityOrder[ruleA.highestSeverity.toLowerCase()] || 0;
                        const severityValueB = severityOrder[ruleB.highestSeverity.toLowerCase()] || 0;
                        return severityValueB - severityValueA; // Sort descending
                    }) as [ruleName, ruleData] (ruleName)}
                        <AccordionItem open>
                            <svelte:fragment slot="title">
                                <div class="accordion-title-container">
                                    <div class="rule-title-with-icon">
                                        {#if ruleData.highestSeverity === 'warning'}
                                            <WarningAltFilled size="{20}" style="color: #ff832b;" />
                                        {:else if ruleData.highestSeverity === 'critical'}
                                            <WarningAltFilled size="{20}" style="color: #da1e28;" />
                                        {:else}
                                            <Information size="{20}" style="color: #0f62fe;" />
                                        {/if}
                                        <span>{ruleName}</span>
                                    </div>
                                    <Tag type="gray">{Object.keys(ruleData.namespaces).length} namespaces</Tag>
                                </div>
                            </svelte:fragment>
                            
                            <div class="accordion-content">
                                <p class="suggested-action">
                                    <strong>Suggested Action:</strong> {ruleData.suggested_action}
                                </p>

                                <div class="custom-accordion-container">
                                    {#each Object.entries(ruleData.namespaces) as [namespace, tables] (namespace)}
                                        {@const namespaceKey = `${ruleName}-${namespace}`}
                                        {@const isOpen = !!expandedNamespaces[namespaceKey]}
                                        <div class="custom-accordion-item">
                                            <button
                                                class="custom-accordion-title"
                                                on:click={() => (expandedNamespaces[namespaceKey] = !isOpen)}
                                                aria-expanded={isOpen}
                                            >
                                                <ChevronRight class="chevron-icon {isOpen ? 'open' : ''}" />
                                                <div class="accordion-title-container">
                                                    <span>{namespace}</span>
                                                    <Tag type="gray">{tables.length} tables</Tag>
                                                </div>
                                            </button>

                                            {#if isOpen}
                                                <div class="custom-accordion-content">
                                                    <div class="table-list">
                                                        {#each tables as table}
                                                            <div class="table-list-item">
                                                                <div class="table-list-header">
                                                                    <a href="/?namespace={namespace}&table={table.table_name}" class="table-link">
                                                                        <strong>{table.table_name}</strong>
                                                                    </a>
                                                                    </div>
                                                                <p class="table-message">{table.message}</p>
                                                                <p class="table-timestamp">
                                                                    Last seen: {new Date(table.timestamp).toLocaleString()}
                                                                </p>
                                                            </div>
                                                        {/each}
                                                    </div>
                                                </div>
                                            {/if}
                                        </div>
                                    {/each}
                                </div>
                            </div>
                        </AccordionItem>
                    {/each}
                </Accordion>
            {/if}
        {:else if insightsSubTab === 1}
            <div class="controls-container">
                <div class="control-item">
                    <Toggle labelText="Show tables with no warnings" bind:toggled="{showEmptyResults}" />
                </div>
                <div class="control-item dropdown-control">
                    <span class="bx--label">Select # of rows to show</span>
                    <Dropdown
                        id="rows-dropdown"
                        bind:selectedId="{completedRunsLimit}"
                        items="{limitOptions}"
                    />
                </div>
            </div>

            {#if insights_loading}
                <DataTableSkeleton rowCount="{5}" columnCount="{5}" />
            {:else if insightRuns.length === 0}
                <p>No completed health check jobs found across the lakehouse.</p>
            {:else}
                <div class="insights-virtual-table-container-lakehouse">
                    <VirtualTable
                        data="{insightRuns}"
                        columns="{completedRunsColumns}"
                        bind:columnWidths="{completedRunsColWidths}"
                        disableVirtualization="{true}"
                        enableSearch="{true}"
                    >
                        <div slot="cell" let:row let:columnKey let:searchQuery>
                            {#if columnKey === 'Job Type'}
                                <Tag type="{row.run_type === 'manual' ? 'cyan' : 'green'}" title="{row.run_type}"
                                    >{row.run_type}</Tag
                                >
                            {:else if columnKey === 'Timestamp'}
                                {@html highlightMatch(new Date(row.run_timestamp).toLocaleString(), searchQuery)}
                            {:else if columnKey === 'Rules & Results'}
                                {@const codesWithResults = new Set(row.results.map((r) => r.code))}
                                <div class="rules-cell-container">
                                    {#each row.rules_requested as ruleId}
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
                                                    <WarningAltFilled size="{16}" style="color: var(--cds-support-03, #ff832b);" />
                                                {:else}
                                                    <CheckmarkFilled size="{16}" style="color: var(--cds-support-02, #24a148);" />
                                                {/if}
                                                <span>{@html highlightMatch(ruleIdToNameMap.get(ruleId) || ruleId, searchQuery)}</span>
                                            </div>
                                            {#if expandedRules[compositeKey]}
                                                <div class="rule-details">
                                                    {#each ruleResults as result}
                                                        <div class="message-card">
                                                            <p><strong>Message:</strong> {@html highlightMatch(result.message, searchQuery)}</p>
                                                        </div>
                                                    {/each}
                                                </div>
                                            {/if}
                                        </div>
                                    {/each}
                                </div>
                            {:else}
                                {@html highlightMatch(row[columnKey.toLowerCase().replace(' ', '_')], searchQuery)}
                            {/if}
                        </div>
                    </VirtualTable>
                </div>
            {/if}
        {:else if insightsSubTab === 2}
            {#if runningJobsLoading}
                <DataTableSkeleton rowCount="{3}" columnCount="{5}" />
            {:else if runningJobs.length === 0}
                <p>No jobs are currently in progress.</p>
            {:else}
                <VirtualTable
                    data="{runningJobs}"
                    columns="{runningJobsColumns}"
                    bind:columnWidths="{runningJobsColWidths}"
                    disableVirtualization="{true}"
                    enableSearch="{true}"
                >
                    <div slot="cell" let:row let:columnKey let:searchQuery>
                        {#if columnKey === 'Status'}
                            <Tag type="blue">{row.status}</Tag>
                        {:else if columnKey === 'Started At'}
                            {@html highlightMatch(row.started_at ? new Date(row.started_at).toLocaleString() : 'N/A', searchQuery)}
                        {:else if columnKey === 'Job ID'}
                            {@html highlightMatch(row.run_id, searchQuery)}
                        {:else if columnKey === 'Table Name'}
                            {@html highlightMatch(row.table_name ? row.table_name: '-', searchQuery)}
                        {:else}
                            {@html highlightMatch(row[columnKey.toLowerCase().replace(/ /g, '_')], searchQuery)}
                        {/if}
                    </div>
                </VirtualTable>
            {/if}
        {:else if insightsSubTab === 3}
            {#if schedulesLoading}
                <DataTableSkeleton rowCount="{3}" columnCount="{6}" />
            {:else if scheduledJobs.length === 0}
                <p>No health checks runs are scheduled.</p>
            {:else}
                <VirtualTable
                    data="{scheduledJobs}"
                    columns="{scheduledJobsColumns}"
                    bind:columnWidths="{scheduledJobsColWidths}"
                    disableVirtualization="{true}"
                    enableSearch="{true}"
                >
                    <div slot="cell" let:row let:columnKey let:searchQuery>
                        {#if columnKey === 'Rules'}
                            {@html highlightMatch(row.rules_requested.map((id) => ruleIdToNameMap.get(id) || id).join(', '), searchQuery)}
                        {:else if columnKey === 'Enabled'}
                            <Tag type="{row.is_enabled ? 'green' : 'gray'}"
                                >{row.is_enabled ? 'Enabled' : 'Disabled'}</Tag
                            >
                        {:else if columnKey === 'Next Job'}
                            {@html highlightMatch(new Date(row.next_run_timestamp).toLocaleString(), searchQuery)}
                        {:else}
                            {@html highlightMatch(row[columnKey.toLowerCase().replace(/ /g, '_')], searchQuery)}
                        {/if}
                    </div>
                </VirtualTable>
            {/if}
        {/if}
    </div>

    <Modal
        bind:open="{openRunModal}"
        modalHeading="Run New Health Check"
        primaryButtonText="Start Job"
        secondaryButtonText="Cancel"
        on:submit="{handleManualRunSubmit}"
        on:close="{resetModalForms}"
        on:click:button--secondary="{() => openRunModal = false}"
    >
        <p>This will run the selected health check on all applicable tables across the selected namespace.</p>
        <FormGroup legendText="Namespace">
            <ComboBox
                items="{dropdownNamespaces}"
                bind:selectedId="{runModalSelectedNsId}"
            />
        </FormGroup>
        <hr class="modal-divider" />
        <div class="bx--form-item">
            <fieldset class="bx--fieldset">
                <legend class="bx--label legend-with-icon">
                    <span>Rules to Check</span>
                    <button class="info-button" on:click="{() => showRulesInfoModal = true}" title="View rule descriptions">
                        <Information size="{16}" />
                    </button>
                </legend>
                <Checkbox labelText="Select All" checked="{manualRunSelectAll}" indeterminate="{manualRunIndeterminate}" on:change="{toggleSelectAllManual}" />
                <hr class="modal-divider-light" />
                <div class="rules-grid">
                    {#each allRules as rule}
                        <Checkbox labelText="{rule.name}" value="{rule.id}" bind:group="{manualRunData.rules_requested}" />
                    {/each}
                </div>
            </fieldset>
        </div>
    </Modal>

    <Modal
        bind:open="{openScheduleModal}"
        modalHeading="Schedule New Health Check Job"
        primaryButtonText="Create Schedule"
        secondaryButtonText="Cancel"
        on:submit="{handleScheduleSubmit}"
        on:close="{resetModalForms}"
        on:click:button--secondary="{() => openScheduleModal = false}"
    >
        <p>This will schedule the selected health check to run on all applicable tables across the selected namespace.</p>
        <FormGroup legendText="Namespace">
            <ComboBox
                items="{dropdownNamespaces}"
                bind:selectedId="{scheduleModalSelectedNsId}"
            />
        </FormGroup>
        <hr class="modal-divider" />
        <div class="bx--form-item">
            <fieldset class="bx--fieldset">
                <legend class="bx--label legend-with-icon">
                    <span>Rules to Check</span>
                    <button class="info-button" on:click="{() => showRulesInfoModal = true}" title="View rule descriptions">
                        <Information size="{16}" />
                    </button>
                </legend>
                <Checkbox labelText="Select All" checked="{scheduleRunSelectAll}" indeterminate="{scheduleRunIndeterminate}" on:change="{toggleSelectAllSchedule}" />
                <hr class="modal-divider-light" />
                <div class="rules-grid">
                    {#each allRules as rule}
                        <Checkbox labelText="{rule.name}" value="{rule.id}" bind:group="{scheduleRunData.rules_requested}" />
                    {/each}
                </div>
            </fieldset>
        </div>
        <hr class="modal-divider" />
        <FormGroup legendText="Frequency">
            <Select bind:selected="{scheduleRunData.frequency}">
                <SelectItem value="weekly" text="Weekly" />
                <SelectItem value="monthly" text="Monthly (1st)" />
            </Select>
        </FormGroup>
        <FormGroup legendText="Cron Schedule (optional)">
            <TextInput bind:value="{scheduleRunData.cron_schedule}" helperText="Overrides frequency selection" />
        </FormGroup>
    </Modal>

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

</Content>

<style>
    .header-container {
        margin-bottom: 1rem;
    }

    h1 {
        margin-bottom: 1.5rem;
    }
    
    .tab-content-container { padding: 1.5rem 0; }
    .rules-cell-container { display: flex; flex-direction: column; gap: 0.5rem; padding: 8px; width: 100%; }
    .rule-item-header { display: flex; align-items: center; gap: 0.5rem; cursor: pointer; }
    .rule-details { margin-top: 0.5rem; padding-left: 24px; display: flex; flex-direction: column; gap: 0.75rem; }
    .message-card p { margin: 0.2rem 0; font-size: 13px; }
    .rules-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.5rem 1rem; }
    .modal-divider { margin: 1.5rem 0; border: none; border-top: 1px solid #e0e0e0; }
    .modal-divider-light { margin: 0.75rem 0; border: none; border-top: 1px solid #f4f4f4; }
    :global(.bx--form-item) { margin-bottom: 1rem; }
    p { margin-bottom: 1rem; }

    .controls-container {
        align-items: flex-end;
        gap: 2rem;
        margin-bottom: 1rem;
    }

    .dropdown-control .bx--label {
        margin-bottom: 0.5rem;
    }

    /* Style all columns EXCEPT the last one */
    .insights-virtual-table-container-lakehouse :global(.cell:not(:last-child)) {
        align-items: center !important; /* Vertically center */
        justify-content: center; /* Horizontally center */
    }

    /* Style ONLY the last column, regardless of whether it's the 4th or 5th */
    .insights-virtual-table-container-lakehouse :global(.cell:last-child) {
        align-items: flex-start !important;
        padding: 0 !important;
    }

    .insights-virtual-table-container-lakehouse :global(.cell:nth-child(3)) {
        white-space: normal; /* Allows the text to wrap */
        word-break: break-word; /* Breaks long words if necessary */
        justify-content: flex-start !important; /* Aligns content to the left */
        padding: 8px !important; /* Adds some breathing room */
    }

    .insights-virtual-table-container-lakehouse :global(.header-cell) {
        justify-content: center !important;
    }

    .overview-controls {
        margin-bottom: 1.5rem;
        max-width: 400px;
    }

    .accordion-title-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        width: 100%;
        padding-right: 1rem;
    }

    .accordion-content {
        padding: 1rem 0;
    }

    .suggested-action {
        font-style: italic;
        color: #525252;
        margin-bottom: 1.5rem;
        padding-left: 1rem;
    }

    .table-list {
        display: flex;
        flex-direction: column;
        gap: 1rem;
    }

    .table-list-item {
        background-color: #f4f4f4;
        border-left: 3px solid #0f62fe; /* Default blue */
        padding: 0.75rem 1rem;
        border-radius: 4px;
    }
    
    .table-list-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-bottom: 0.5rem;
    }
    
    .table-message {
        margin: 0 0 0.5rem 0;
        font-size: 14px;
    }

    .table-timestamp {
        margin: 0;
        font-size: 12px;
        color: #525252;
    }

    .rule-title-with-icon {
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }

    .custom-accordion-container {
        border-top: 1px solid #e0e0e0;
    }

    .custom-accordion-item {
        border-bottom: 1px solid #e0e0e0;
    }

    .custom-accordion-title {
        display: flex;
        align-items: center;
        width: 100%;
        padding: 0.75rem 0;
        background: none;
        border: none;
        cursor: pointer;
        text-align: left;
    }

    .custom-accordion-title:hover {
        background-color: #f4f4f4;
    }

    .custom-accordion-content {
        padding: 0 1rem 1rem 2.5rem; /* Indent the content */
    }

    .chevron-icon {
        transition: transform 150ms ease-in-out;
        margin-right: 0.5rem;
    }

    .chevron-icon.open {
        transform: rotate(90deg);
    }

    .table-link {
        color: inherit;
        text-decoration: none;
    }
    
    .table-link:hover {
        color: #0f62fe; /* Carbon blue */
        text-decoration: underline;
    }
</style>