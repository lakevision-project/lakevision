<script>
    // Accept JSON data as a prop
    export let jsonData = {};
    export let orient = "";
  </script>
  
  <!-- Display the key-value pairs in a table -->
  <table>
      {#if orient == 'kv'}
        <thead>
          <tr>
            <th></th>
            <th></th>            
          </tr>
        </thead>
        <tbody>
          {#each Object.entries(jsonData) as [key, value]}
            <tr>
              <td>{key}</td>
              <td>{value}</td>
            </tr>
          {/each}
        </tbody>
      {:else if orient == 'table'}            
      <thead>
        <tr>
          {#if jsonData.length > 0}
            {#each Object.keys(jsonData[0]) as key}
              <th>{key}</th>
            {/each}
          {/if}
        </tr>
      </thead>
      {#each jsonData as row}                
        <tbody>
          <tr>          
            {#each Object.values(row) as value}
              <td>{value}</td>
            {/each}
          </tr>
        </tbody>
      {/each}            
      {/if}    
  </table>
  
  <style>
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 0px;
    }

    /* Long property values (locations, UUID lists) must not push the page wide. */
    td {
      overflow-wrap: anywhere;
    }
  
    th, td {
      border: 1px solid var(--cds-ui-03, #e0e0e0);
      padding: 8px;
      text-align: left;
      color: var(--cds-text-01, #161616);
    }

    th {
      background-color: var(--cds-ui-01, #f4f4f4);
    }
  </style>
  