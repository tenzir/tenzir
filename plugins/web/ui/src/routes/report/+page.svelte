<script lang="ts">
  import Editor from '$lib/components/Editor.svelte';
  import MultiLine from '$lib/components/LayerCake/MultiLine/index.svelte';
  let visibleBlocks: ('bytes' | 'text')[] = [];
</script>

<svelte:head>
  <title>VAST Reports</title>
  <meta name="description" content="Reporting page for VAST" />
</svelte:head>

<div class="p-2 m-2 text-sm text-left text-gray-600">
  <div class="flex justify-between my-4 mr-4">
    <div class="text-xl font-bold">Reports</div>

  <button
    class="flex items-center mr-2"
  >
    Export
  </button>
  </div>
  {#each visibleBlocks as block}
    {#if block == 'bytes'}
      <div class="text-l font-bold">Inbound vs Outbound Bytes</div>
      <div class="m-10">
        <MultiLine />
      </div>
    {:else}
      <div class="text-l font-bold">Notes</div>
      <div class="my-10">
        <Editor />
      </div>
    {/if}
  {/each}
  <div class="flex">


  <button
    class="flex items-center mr-2"
    on:click={() => {
      visibleBlocks = [...visibleBlocks, "tex"]
    }}
  >
    <div class="i-ant-design:plus-square-outlined w-16px" />
    Add Text Block
  </button>

  <button
    class="flex items-center mr-2"
    on:click={() => {
      visibleBlocks = [...visibleBlocks, "bytes"]
    }}
  >
    <div class="i-ant-design:plus-square-outlined w-16px" />
    Add Bytes Block
  </button>
  </div>
</div>
