<script>
  import Editor from '$lib/components/Editor.svelte';
  import MultiLine from '$lib/components/LayerCake/MultiLine/index.svelte';
</script>

<svelte:head>
  <title>VAST Reports</title>
  <meta name="description" content="Reporting page for VAST" />
</svelte:head>

<div class="p-2 m-2 text-sm text-left text-gray-600">
  <div class="flex justify-between my-4 mr-4">
    <div class="text-xl font-bold">Reports</div>
  </div>
  <div class="text-l font-bold">Inbound vs Outbound Bytes</div>
  <div class="m-10">
    <MultiLine />
  </div>
  <div class="text-l font-bold">Notes</div>
  <div class="my-10">
    <Editor/>
  </div>
</div>
