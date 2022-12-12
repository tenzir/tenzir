<script lang="ts">
  import { getQuery } from '$lib/util/api';
  import Button from '$lib/components/Button.svelte';
  import Table from '$lib/components/Table.svelte';
  import type TableRow from '$lib/components/Table.svelte';

  export let parameters = {
    title: 'TODO make me editable',
    query: ''
  };

  interface QueryResult {
    version: string;
    num_events: number;
    events: TableRow[]; // TODO better type it once the API is stabilized
  }

  let queryResult: QueryResult;
  $: console.log(queryResult);

  const handleRun = async () => {
    queryResult = await getQuery(parameters.query);
  };
  $: columnNames =
    queryResult?.events?.[0] &&
    Object.keys(queryResult?.events[0]).map((x) => {
      return { header: x, accessor: x };
    });
</script>

<div class="w-1/3 pb-4">
  <input
    bind:value={parameters.query}
    class="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
  />
</div>

<Button onClick={handleRun}>Run</Button>

<!-- NOTE: We need to use the key block as the table does not update otherwise (due to use of stores?) -->
{#key queryResult}
  {#if queryResult?.events?.[0] && columnNames}
    <div class="py-2 max-w-80% max-h-300px overflow-auto">
      <Table
        tableRows={queryResult.events}
        columnDetails={columnNames}
        showCellBorder
        showHeaderBorder
      />
    </div>
  {/if}
{/key}
