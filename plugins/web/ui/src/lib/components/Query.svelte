<script lang="ts">
  import Table from './Table.svelte';

  import type TableRow from './Table.svelte';

  let inputVal = '';

  const getQuery = async (expression: string) => {
    try {
      const API_BASE =
        import.meta.env.VITE_VAST_API_ENDPOINT ?? `${import.meta.env.BASE_URL}api/v0`;

      // const url = `${API_BASE}/export?expression=${encodeURIComponent('#type == "zeek.conn" && id.orig_h in 192.168.0.0/16')}`;
      const url = `${API_BASE}/export?expression=${encodeURIComponent(`${expression}`)}`;
      // console.log(url);

      const response = await fetch(url);

      const data = await response.json();

      return { ...data };
    } catch (error) {
      console.error(`Error in getQuery function : ${error}`);
    }
  };
  interface QueryResult {
    version: string,
    num_events: number,
    events: TableRow[], // TODO better type it once the API is stabilized
  }

  let queryResult : QueryResult
  $: console.log(queryResult)

  const handleRun = async () => {
    queryResult = await getQuery(inputVal);
  };
  $: columnNames =
    queryResult?.events?.[0] &&
    Object.keys(queryResult?.events[0]).map((x) => {
      return { header: x, accessor: x };
    });
</script>

<input bind:value={inputVal} />
<button on:click={handleRun} class="flex items-center mr-2"> Run </button>

<div>
  <!-- NOTE: We need to use the key block as the table does not update otherwise (due to use of stores?) -->
  {#key queryResult}
    {#if queryResult?.events?.[0] && columnNames}
      <Table tableRows={queryResult.events} columnDetails={columnNames} />
    {/if}
  {/key}
</div>
