
<script>
    import { Tile, ExpandableTile, Content, Tabs, Tab, TabContent, Grid, Row, Column, CopyButton, ToastNotification, DataTableSkeleton } from "carbon-components-svelte";
    import { selectedNamespce } from '$lib/stores';
    import { selectedTable } from '$lib/stores';
    import { sample_limit } from '$lib/stores';
    import JsonTable from '../lib/components/JsonTable.svelte';
    import { Loading } from 'carbon-components-svelte';
    import { BarChartSimple } from '@carbon/charts-svelte'        
    import '@carbon/charts-svelte/styles.css'
    import options from './options'        	
    import VirtualTable from '../lib/components/VirtTable3.svelte';
    import { goto } from "$app/navigation";
    import QueryRunner from '../lib/components/QueryRunner.svelte';

    let namespace;    
    let table;
    let ns_props;
    let tab_props;
    let error = "";
    let url = "";
    let pageSessionId = Date.now().toString(36) + Math.random().toString(36).substring(2);       

    $: {
        selectedNamespce.subscribe(value => {namespace = value; });
        selectedTable.subscribe(value => {table = value; });
        if(namespace) ns_props = get_namespace_special_properties(namespace);
        if(namespace && table) tab_props = get_table_special_properties( namespace+"."+table);       
    }

    let partitions = [];
    let partitions_loading = false;
    let snapshots = [];
    let snapshots_loading = false;
    let sample_data = [];
    let sample_data_loading = false;
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

    async function get_namespace_special_properties(namespace_name){        
        ns_props = await fetch(
            `/api/namespaces/${namespace_name}/special-properties`,{
                headers: {
                    'Content-Type': 'application/json',
                    'X-Page-Session-ID': pageSessionId,
                },                    
            }
        ).then(res => res.json());
        return ns_props;
    }

    async function get_table_special_properties(table_id){        
        tab_props = await fetch(
            `/api/tables/${table_id}/special-properties`,{
                headers: {
                    'Content-Type': 'application/json',
                    'X-Page-Session-ID': pageSessionId,
                },                    
            }
        ).then(res => res.json());
        return tab_props;
    }

    async function get_data(table_id, feature){
        let loading = true;
        if(!table_id || table_id == null || table_id == "." || !table) {
            loading = false;      
            return;
        }
        try{            
            const res = await fetch(
                `/api/tables/${table_id}/${feature}`,
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
                return JSON.parse(data);                
            }
            else if (statusCode == 403){
                console.log("No Access");
                error = "No Access"
                access_allowed = false;
                return error
            }else if (res.status === 401) {
                goto("/api/login?namespace="+namespace+"&table="+table+"&sample_limit=100");
			}
            else{
                console.error("Failed to fetch data:", res.statusText);
                error = res.statusText;
            }
        } 
        finally {
            loading = false; 
        }  
    }
    let selected = 0; 
    $: reset(table);
    let callOnce = 0;

    $: (async () => {
        if( table === '') return;     
        try {
            if(selected==0){
                if (callOnce > 0 ){ callOnce=0; return;}
                callOnce++;
                summary_loading = true;          
                summary = await get_data(namespace+"."+table, "summary");  
                if(tab_props && 'restricted' in tab_props){
                    summary['Restricted'] = tab_props['restricted'];
                }
                summary_loading = false;   
            }           
        } catch (err) {
            error = err.message; 
            summary_loading = false;  
        }
        try {
            if(selected==0){
                properties_loading = true;          
                properties = await get_data(namespace+"."+table, "properties");  
                properties_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            properties_loading = false;  
        }
        try {
            if(selected==0){
                schema_loading = true;          
                schema = await get_data(namespace+"."+table, "schema");  
                schema_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            schema_loading = false;  
        }        
        try {
            if(selected==0){
                partition_specs_loading = true;          
                partition_specs = await get_data(namespace+"."+table, "partition-specs");  
                partition_specs_loading = false; 
            }
        } catch (err) {
            error = err.message; 
            partition_specs_loading = false;  
        }
        try {
            if(selected==0){
                sort_order_loading = true;          
                sort_order = await get_data(namespace+"."+table, "sort-order");  
                sort_order_loading = false; 
            }
        } catch (err) {
            error = err.message; 
            sort_order_loading = false;  
        }
        try {        
            if(selected==1 && partitions.length == 0 && !partitions_loading){
                partitions_loading = true; 
                partitions = await get_data(namespace+"."+table, "partitions");  
                partitions_loading = false;  
            }
        } catch (err) {
            error = err.message; 
            partitions_loading = false;  
        }
        try {
            if(selected==2 && snapshots.length == 0){
                snapshots_loading = true;        
                snapshots = await get_data(namespace+"."+table, "snapshots");  
                snapshots_loading = false;              
            }
        } catch (err) {
            error = err.message; 
            snapshots_loading = false;  
        }
        try {
            if(selected==3 && sample_data.length == 0 && !sample_data_loading){
                sample_data_loading = true;
                if($sample_limit>0){                     
                    sample_data = await get_data(namespace+"."+table, "sample?sample_limit="+$sample_limit);
                }  
                else{
                    sample_data = await get_data(namespace+"."+table, "sample");
                }
                sample_data_loading = false;
            }
        } catch (err) {
            error = err.message;
            sample_data_loading = false;  
        }        
        try {
            if(selected==5 && data_change.length == 0 && !data_change_loading){
                data_change_loading = true;          
                data_change = await get_data(namespace+"."+table, "data-change");              
                data_change_loading = false;  
            }
            
        } catch (err) {
            error = err.message; 
            data_change_loading = false;  
        }
    })();

    function set_copy_url(){
        url = window.location.origin;
        url = url+"/?namespace="+namespace+"&table="+table+"&sample_limit=100";
    }

    function reset(table){
        partitions = [];
        snapshots = [];
        sample_data = [];
        data_change = [];
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
        //selected = 0;
    }
</script>

<Content>    
    <Tile>
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
              text={url}
              on:click={set_copy_url}
              iconDescription="Copy table link"
              feedback="Table link copied"
            />
          </div>
      </Tile>
    <br />    
    <Tabs bind:selected>
        <Tab label="Summary" />
        <Tab label="Partitions" />
        <Tab label="Snapshots" />
        <Tab label="Sample Data" /> 
        <Tab label="SQL" />
        <Tab label="Insights" />        
        <svelte:fragment slot="content">
            <TabContent><br/>                
                <Grid>
                    <Row>
                      <Column aspectRatio="2x1">                         
                        <h5>Summary</h5>
                        {#if !summary_loading && properties}
                             <JsonTable jsonData={summary} orient = "kv"></JsonTable>
                        {:else}
                            <Loading withOverlay={false} small />
                        {/if}                                                                       
                        </Column>
                        <Column aspectRatio="2x1">
                            <h5>Schema</h5>
                            {#if !schema_loading && schema.length > 0}
                                <VirtualTable data={schema} columns={schema[0]} rowHeight={37} tableHeight={360} defaultColumnWidth={121}/>
                            {/if}
                        </Column>
                    </Row>
                    <Row>
                        <Column aspectRatio="2x1">   
                            <br /><br />                    
                            <h5>Properties</h5>
                        {#if !properties_loading && properties}
                            <JsonTable jsonData={properties} orient = "kv"></JsonTable>                                                     
                        {/if}
                     </Column>
                      <Column aspectRatio="2x1">  
                        <br /><br />
                        <h5>Partition Specs</h5>              
                        {#if !partition_specs_loading && partition_specs}                        
                            <JsonTable jsonData={partition_specs} orient="table" /> 
                        {/if}

                        <br />
                        <h5>Sort Order</h5>
                        {#if !sort_order_loading && sort_order}                        
                            <JsonTable jsonData={sort_order} orient="table" /> 
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

            <TabContent><br/> 
                {#if partitions_loading}
                    <Loading withOverlay={false} small />   
                {:else if !access_allowed}   
                    <ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data"></ToastNotification>
                {:else if partitions.length > 0}
                    <VirtualTable data={partitions} columns={partitions[0]} rowHeight={35} enableSearch=true/>  
                    <br />
                    Total items: {partitions.length}              
                {/if}         
            </TabContent>

            <TabContent><br/>
                {#if snapshots_loading}
                    <Loading withOverlay={false} small />      
                {:else if snapshots.length > 0}
                    <VirtualTable data={snapshots} columns={snapshots[0]} rowHeight={35} enableSearch=true/>  
                    <br />
                    Total items: {snapshots.length}     
                {:else}
                    No data         
                {/if}  
            </TabContent>

            <TabContent><br/>
                {#if sample_data_loading}
                    <Loading withOverlay={false} small />    
                {:else if !access_allowed}   
                    <ToastNotification hideCloseButton title="No Access" subtitle="You don't have access to the table data"></ToastNotification>  
                {:else if sample_data.length > 0}
                    <VirtualTable data={sample_data} columns={sample_data[0]} rowHeight={35} enableSearch=true/>
                    <br />
                    Sample items: {sample_data.length}
                {/if}
            </TabContent>
            <TabContent><br/>
                <QueryRunner tableName={namespace}.{table} pageSessionId=pageSessionId />
            </TabContent>    
            <TabContent><br/>
                {#if data_change_loading}                    
                    <br /> <Loading withOverlay={false} small /> <br />                    
                {:else}                                    
                    <BarChartSimple data={data_change} options={options} style="padding:2rem;" />     
                {/if}           
            </TabContent>
        </svelte:fragment>
      </Tabs>    
  </Content>

  <style>

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
    display: flex;
    justify-content: flex-end;
  }
  </style>