<script lang="ts">
  import Table from './Table.svelte';

  import type TableRow from './Table.svelte';
  import Button from './Button.svelte';

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
    version: string;
    num_events: number;
    events: TableRow[]; // TODO better type it once the API is stabilized
  }

  let queryResult: QueryResult;
  $: console.log(queryResult);

  const handleRun = async () => {
    queryResult = await getQuery(inputVal);
  };
  $: columnNames =
    queryResult?.events?.[0] &&
    Object.keys(queryResult?.events[0]).map((x) => {
      return { header: x, accessor: x };
    });
</script>

<div class="w-1/3 pb-4">
<input
  bind:value={inputVal}
  class="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
/>
</div>

<Button onClick={handleRun} class="flex items-center mr-2">Run</Button>

<div>
  <!-- NOTE: We need to use the key block as the table does not update otherwise (due to use of stores?) -->
  {#key queryResult}
    {#if queryResult?.events?.[0] && columnNames}
      <Table tableRows={queryResult.events} columnDetails={columnNames} />
    {/if}
  {/key}
</div>
