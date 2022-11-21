<script lang="ts">
  import MultiLine from '$lib/components/LayerCake/MultiLine/index.svelte';
  import Query from '$lib/components/Query.svelte';
  import Menu from '$lib/components/Menu.svelte';
  import Editor from '$lib/components/Editor.svelte';
  import { capitalizeFirstLetter } from '$lib/util/strings';
  import Button from '$lib/components/Button.svelte';
  let possibleBlocks = ['bytes', 'text', 'query'] as const;

  type Block = typeof possibleBlocks[number];
  let visibleBlocks: Block[] = [];
  let addAction = (block: Block) => () => {
    visibleBlocks = [...visibleBlocks, block];
  };
</script>

<svelte:head>
  <title>VAST Reports</title>
  <meta name="description" content="Reporting page for VAST" />
</svelte:head>

<div class="p-2 m-2 text-sm text-left text-gray-600">
  <div class="flex justify-between my-4 mr-4">
    <div class="text-xl font-bold">Reports</div>

    <Button onClick={() => {}}>Export</Button>
  </div>
  {#each visibleBlocks as block}
    {#if block == 'bytes'}
      <div class="text-l font-bold">Inbound vs Outbound Bytes</div>
      <div class="m-10">
        <MultiLine />
      </div>
    {:else if block == 'query'}
      <div class="text-l font-bold">Query</div>
      <div class="my-10">
        <Query />
      </div>
    {:else}
      <div class="text-l font-bold">Text</div>
      <div class="my-10">
        <Editor />
      </div>
    {/if}
  {/each}
  <Menu
    description="Add Block"
    items={possibleBlocks.map((block) => {
      return { text: capitalizeFirstLetter(block), onClick: addAction(block) };
    })}
  />
</div>
