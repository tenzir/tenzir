<script lang="ts">
  import { getQuery } from '$lib/util/api';
  import Button from '$lib/components/Button.svelte';
  import Table from '$lib/components/Table.svelte';
  import type TableRow from '$lib/components/Table.svelte';
  import BlockHeader from './BlockHeader.svelte';
  import { onMount } from 'svelte';

  export let parameters = {
    title: '',
    query: '',
    isEditing: false
  };

  interface QueryResult {
    version: string;
    num_events: number;
    events: TableRow[]; // TODO better type it once the API is stabilized
  }

  let queryResult: QueryResult;

  // If we are mounting with an exisitng query, we need to fetch the results
  onMount(async () => {
    if (parameters.query !== "") {
      await handleRun();
    }
  });

  const handleRun = async () => {
    queryResult = await getQuery(parameters.query);
  };
  $: columnNames =
    queryResult?.events?.[0] &&
    Object.keys(queryResult?.events[0]).map((x) => {
      return { header: x, accessor: x };
    });

  const handleSaveOrEdit = () => {
    parameters.isEditing= !parameters.isEditing;
  };
</script>

{#if !parameters.isEditing}
  <BlockHeader
    bind:title={parameters.title}
    onEdit={() => handleSaveOrEdit()}
    onDelete={() => {
      console.log('delete is not implemented yet');
    }}
  />
{/if}
{#if parameters.isEditing}
  <div class="w-full pb-4 flex">
    <input
      bind:value={parameters.query}
      class="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full h-10px p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
    />

    <div class="pl-10">
      <Button onClick={handleRun}>Run</Button>
    </div>
  </div>
{/if}
{#if !parameters.isEditing}
  <div class="py-2">
    <p class="text-lg p-2 bg-slate-100 rounded">{parameters.query}</p>
  </div>
{/if}

<!-- NOTE: We need to use the key block as the table does not update otherwise (due to use of stores?) -->
{#key queryResult}
  {#if queryResult?.events?.[0] && columnNames}
    <div class="py-2 max-w-100% max-h-400px overflow-auto">
      <Table
        tableRows={queryResult.events}
        columnDetails={columnNames}
        showCellBorder
        showHeaderBorder
      />
    </div>
    {#if parameters.isEditing}
      <div class="pt-2">
        <Button onClick={handleSaveOrEdit}>Save</Button>
      </div>
    {/if}
  {/if}
{/key}
