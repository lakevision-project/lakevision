<script lang="ts">
    import { writable } from 'svelte/store';
    import { goto } from "$app/navigation";
    import VirtualTable from './VirtTable3.svelte';
    import { TextArea, Button, InlineNotification, CodeSnippet, Loading } from 'carbon-components-svelte';

    export let tableName; // passed in from parent route or context
    export let pageSessionId;
  
    const userQuery = writable('');
    const finalQuery = writable('');
    const error = writable('');
    const result = writable(null);
    const loading = writable(false);
  
    // Validate partial query (before injecting FROM clause)
    function validatePartial(query) {
      const q = query.trim().toLowerCase();
      if (!q.startsWith('select')) {
        error.set('Only SELECT queries are allowed.');
        return false;
      }
      if (/join\s/i.test(q)) {
        error.set('JOINs are not allowed.');
        return false;
      }
      if (/from\s/i.test(q)) {
        error.set('Please omit the FROM clause. It will be added automatically.');
        return false;
      }
      error.set('');
      return true;
    }

    function injectFromClause(userInput, tableName) {
        const regex = /^select\s+(.*?)(\s+(where|order\s+by|limit|having|group\s+by)\b.*)?$/i;
        const match = userInput.match(regex);

        if (!match) return null;

        const columns = match[1].trim();
        const clause = match[2] ? match[2].trim() : '';

        return `SELECT ${columns} FROM ${tableName}${clause ? ' ' + clause : ''}`;
    }

    function splitBeforeLastDot(input) {
        const lastDotIndex = input.lastIndexOf('.');
        if (lastDotIndex === -1) return [input, '']; // No dot found
        return [input.slice(0, lastDotIndex), input.slice(lastDotIndex + 1)];
    }

    async function get_data(){
        loading.set(true);
        var feature = "sample";
        var table_id = tableName;        
        if(!table_id || table_id == null || table_id == ".") {
            loading.set(false);
            return;
        }
        try{            
            const $query = $userQuery.trim();
            
            if (!validatePartial($query)) return; 

            const fullQuery = injectFromClause($query, tableName);
            if (!fullQuery) {
                error.set('Invalid SELECT query.');
            return;
}
            finalQuery.set(fullQuery);
            loading.set(true);
            result.set(null);
            const res = await fetch(
                `/api/tables/${table_id}/${feature}?sql=${fullQuery}`,
                {
                    //method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Page-Session-ID': pageSessionId, // Include the session ID in the headers
                    },
                    //queryParams: { table_id: tableId },
                }
            );            
            const statusCode = res.status;
            if (res.ok) {
                const data = await res.json();
                result.set(data);                  
                return data;                
            }
            else if (statusCode == 403){
                console.log("No Access");
                error.set("No Access");
                access_allowed = false;                
            }else if (res.status === 401) {
                const nst = splitBeforeLastDot(tableName);
                goto("/api/login?namespace="+nst[0]+"&table="+nst[1]+"&sample_limit=100");
			}
            else if (statusCode == 418){
                console.error("Failed to fetch data:", res);
                const detail = await res.json();
                error.set(detail.message);
            }
            else{
                console.error("Failed to fetch data:", res);
                error.set(res.statusText);
            }
        } 
        finally {
            loading.set(false);
        }
    }
    $: reset(tableName);
    
    
    function reset(table){
        userQuery.set(null);
        finalQuery.set(null);
        loading.set(false);
        result.set(null);
        error.set(null);
    }

    async function runQuery() {
      const $query = $userQuery.trim();
      if (!validatePartial($query)) return;
  
      const fullQuery = `${$query} FROM ${tableName}`;
      finalQuery.set(fullQuery);
      loading.set(true);
      result.set(null);
  
      try {                
        const res = await fetch(`/api/tables/${tableName}/sample`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: fullQuery })
        });
  
        if (!res.ok) {
          const { detail } = await res.json();
          throw new Error(detail || 'Server error');
        }
  
        const data = await res.json();
        result.set(data);
      } catch (err) {
        error.set(err.message);
      } finally {
        loading.set(false);
      }
    }
  </script>
  
  <style>
    @use '@carbon/layout' as layout;
    .query-container {
      max-width: auto;
      margin-left: 10px;
      
      padding: 1rem;      
    }
    .text-container {
      max-width: 1000px;        
    }
    .generic-container {
        margin-top: 5px;   
        max-width: auto;
    }
  </style>
  <div class="query-container">
    <h5 class="bx--type-expressive-heading-01">Query Table: <code>{tableName}</code></h5>
    
    <div class="text-container">
    <TextArea
      bind:value={$userQuery}
      labelText="SQL Query (omit FROM clause)"
      placeholder="eg: SELECT id, name WHERE age > 30 ORDER BY created_at DESC"
      helperText="Do not include FROM clause in your query"
      rows="1"
    />
    </div>
    {#if $error}
      <InlineNotification
        kind="error"
        title="Query Error"
        subtitle={$error}
        hideCloseButton
      />
    {/if}
   
    <div class="generic-container">
        <Button kind="primary" on:click={get_data} disabled={$loading} size="sm">
        {$loading ? 'Running...' : 'Run Query'}
        </Button>
    </div>
    {#if $finalQuery}    
    <div>
        <br />
        <h5 class="bx--type-heading-compact-01">Effective Query</h5>
        <div class="text-container">
        <TextArea readonly value={ $finalQuery } rows=1></TextArea>
        </div>
    </div>  
    {/if}
      
    {#if $loading}
        <Loading withOverlay={false} small />    
    {/if}
  
    {#if $result}
      <br />
        <h5 class="bx--type-heading-compact-01">Result</h5>
        <VirtualTable data={$result} columns={$result[0]} rowHeight={35} enableSearch=true/>
        <br />
        Total items: {$result.length}
    {/if}
</div>
  